from flask import Flask, render_template, request, jsonify, Response, session
import pandas as pd
import mysql.connector
from mysql.connector import pooling
import re
import json
import uuid
import io
import csv
import math

app = Flask(__name__)
app.secret_key = 'change-this-in-production'

# ── Connection pool — handles concurrent users properly ───────────────────────
DB_POOL = pooling.MySQLConnectionPool(
    pool_name="recon_pool",
    pool_size=10,           # up to 10 concurrent DB connections
    host='localhost',
    user='recon_user',
    password='StrongPassword123!',
    database='reconciliation',
    charset='utf8mb4',
    autocommit=False
)

def get_db():
    return DB_POOL.get_connection()


# ── Helpers ───────────────────────────────────────────────────────────────────
def get_session_id():
    if 'sid' not in session:
        session['sid'] = str(uuid.uuid4())
    return session['sid']


def clean_for_json(obj):
    if isinstance(obj, dict):
        return {k: clean_for_json(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [clean_for_json(v) for v in obj]
    elif isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
        return None
    return obj


def extract_pin(text):
    m = re.search(r'\b\d{6}\b', str(text))
    return m.group(0) if m else None


def make_core_key(text):
    return re.sub(r'[^a-z0-9]', '', str(text).lower())


# ── DB helpers ────────────────────────────────────────────────────────────────
def session_exists(sid):
    db = get_db()
    cur = db.cursor()
    try:
        cur.execute("SELECT 1 FROM csv_meta WHERE session_id = %s", (sid,))
        return cur.fetchone() is not None
    finally:
        cur.close()
        db.close()


def save_to_db(sid, columns, rows):
    """Bulk-insert all rows. Uses executemany for speed."""
    db = get_db()
    cur = db.cursor()
    try:
        cur.execute("DELETE FROM csv_rows WHERE session_id = %s", (sid,))
        cur.execute("DELETE FROM csv_meta WHERE session_id = %s", (sid,))

        cur.execute(
            "INSERT INTO csv_meta (session_id, columns_json) VALUES (%s, %s)",
            (sid, json.dumps(columns))
        )

        batch = []
        for i, row in enumerate(rows):
            inst = str(row.get('alloted_institute', '') or '')
            pin = extract_pin(inst)
            ck = make_core_key(inst) if pin else None
            batch.append((sid, i, pin, ck, inst, json.dumps(row)))

        cur.executemany(
            """INSERT INTO csv_rows
               (session_id, row_index, pin, core_key, inst_value, row_data)
               VALUES (%s, %s, %s, %s, %s, %s)""",
            batch
        )
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        cur.close()
        db.close()


def load_columns(sid):
    db = get_db()
    cur = db.cursor()
    try:
        cur.execute("SELECT columns_json FROM csv_meta WHERE session_id = %s", (sid,))
        row = cur.fetchone()
        return json.loads(row[0]) if row else []
    finally:
        cur.close()
        db.close()


def load_rows(sid):
    """Load all rows ordered by index."""
    db = get_db()
    cur = db.cursor()
    try:
        cur.execute(
            "SELECT row_data FROM csv_rows WHERE session_id = %s ORDER BY row_index",
            (sid,)
        )
        return [json.loads(r[0]) for r in cur.fetchall()]
    finally:
        cur.close()
        db.close()


# ── Group computation pushed into MySQL ───────────────────────────────────────
def compute_groups_from_db(sid):
    """
    Compute groups entirely in MySQL — no Python loops over all rows.
    Only rows with inconsistencies need full row_data fetched.
    """
    db = get_db()
    cur = db.cursor(dictionary=True)
    try:
        # Step 1: Get all groups with variant counts — pure SQL aggregation
        cur.execute("""
            SELECT
                pin,
                core_key,
                COUNT(*)                        AS total_rows,
                COUNT(DISTINCT inst_value)      AS variant_count,
                MIN(inst_value)                 AS first_variant
            FROM csv_rows
            WHERE session_id = %s AND pin IS NOT NULL
            GROUP BY pin, core_key
            ORDER BY pin
        """, (sid,))
        group_summaries = cur.fetchall()

        all_groups = []
        inconsistent_groups = []

        for summary in group_summaries:
            pin = summary['pin']
            ck = summary['core_key']
            group_id = f"{pin}-{ck}"
            has_warning = summary['variant_count'] > 1

            # Step 2: fetch full rows only for this group
            cur.execute("""
                SELECT row_index, inst_value, row_data
                FROM csv_rows
                WHERE session_id = %s AND pin = %s AND core_key = %s
                ORDER BY row_index
            """, (sid, pin, ck))
            db_rows = cur.fetchall()

            # Get ordered unique variants
            seen = {}
            for r in db_rows:
                v = r['inst_value']
                if v not in seen:
                    seen[v] = True
            variants = list(seen.keys())

            items = [
                {'index': r['row_index'], 'raw': r['inst_value'],
                 'fullRow': json.loads(r['row_data'])}
                for r in db_rows
            ]

            group_data = {
                'groupId':    group_id,
                'pin':        pin,
                'key':        f"{pin}|{ck}",
                'items':      items,
                'variants':   variants,
                'count':      len(items),
                'hasWarning': has_warning
            }
            all_groups.append(group_data)
            if has_warning:
                inconsistent_groups.append(group_data)

        return {
            'allGroups':          all_groups,
            'inconsistentGroups': inconsistent_groups,
            'totalGroups':        len(all_groups),
            'inconsistentCount':  len(inconsistent_groups)
        }
    finally:
        cur.close()
        db.close()


def fix_rows_in_db(sid, pin, core_key_val, reference):
    """Update only the affected rows directly in MySQL."""
    db = get_db()
    cur = db.cursor()
    try:
        # Fetch only the rows that need changing
        cur.execute("""
            SELECT row_index, row_data FROM csv_rows
            WHERE session_id = %s AND pin = %s AND core_key = %s
              AND inst_value != %s
        """, (sid, pin, core_key_val, reference))
        to_fix = cur.fetchall()

        fixed_count = 0
        for row_index, row_data_json in to_fix:
            row = json.loads(row_data_json)
            row['alloted_institute'] = reference
            cur.execute("""
                UPDATE csv_rows
                SET inst_value = %s, row_data = %s
                WHERE session_id = %s AND row_index = %s
            """, (reference, json.dumps(row), sid, row_index))
            fixed_count += 1

        db.commit()
        return fixed_count
    except Exception:
        db.rollback()
        raise
    finally:
        cur.close()
        db.close()


def fix_all_in_db(sid):
    """Fix all inconsistent groups directly in MySQL."""
    db = get_db()
    cur = db.cursor(dictionary=True)
    try:
        # Find inconsistent groups and their reference value in one query
        cur.execute("""
            SELECT pin, core_key, MIN(inst_value) AS reference
            FROM csv_rows
            WHERE session_id = %s AND pin IS NOT NULL
            GROUP BY pin, core_key
            HAVING COUNT(DISTINCT inst_value) > 1
        """, (sid,))
        inconsistent = cur.fetchall()

        total_fixed = 0
        groups_fixed = 0

        for group in inconsistent:
            pin = group['pin']
            ck = group['core_key']
            reference = group['reference']

            cur.execute("""
                SELECT row_index, row_data FROM csv_rows
                WHERE session_id = %s AND pin = %s AND core_key = %s
                  AND inst_value != %s
            """, (sid, pin, ck, reference))
            to_fix = cur.fetchall()

            for row in to_fix:
                row_data = json.loads(row['row_data'])
                row_data['alloted_institute'] = reference
                cur.execute("""
                    UPDATE csv_rows SET inst_value = %s, row_data = %s
                    WHERE session_id = %s AND row_index = %s
                """, (reference, json.dumps(row_data), sid, row['row_index']))
                total_fixed += 1

            groups_fixed += 1

        db.commit()
        return total_fixed, groups_fixed
    except Exception:
        db.rollback()
        raise
    finally:
        cur.close()
        db.close()


# ── Routes ────────────────────────────────────────────────────────────────────
@app.route('/', methods=['GET', 'POST'])
def index():
    has_data = False
    if request.method == 'POST':
        file = request.files.get('file')
        if file:
            df = pd.read_csv(file)
            columns = df.columns.tolist()
            rows = clean_for_json(df.to_dict(orient='records'))
            sid = get_session_id()
            save_to_db(sid, columns, rows)
            has_data = True
    else:
        sid = get_session_id()
        has_data = session_exists(sid)

    columns = load_columns(get_session_id()) if has_data else []
    return render_template('index.html', data=has_data or None, columns=columns, has_data=has_data)


@app.route('/api/groups')
def get_groups():
    sid = get_session_id()
    if not session_exists(sid):
        return jsonify({'error': 'No data'}), 404

    def generate():
        data = compute_groups_from_db(sid)

        all_groups = data['allGroups']
        inconsistent = data['inconsistentGroups']

        total = len(all_groups)

        # META
        yield json.dumps({
            "type": "meta",
            "totalGroups": total
        }) + "\n"

        # CHUNKS
        CHUNK_SIZE = 20

        for i in range(0, total, CHUNK_SIZE):
            chunk = all_groups[i:i+CHUNK_SIZE]

            progress = int((i + len(chunk)) / total * 100)

            yield json.dumps({
                "type": "chunk",
                "allGroups": chunk,
                "progress": progress
            }) + "\n"

        # FINAL
        yield json.dumps({
            "type": "done",
            "inconsistentGroups": inconsistent
        }) + "\n"

    return Response(generate(), mimetype='text/plain')

@app.route('/api/fix', methods=['POST'])
def fix_group():
    body = request.get_json()
    group_id = body.get('groupId')
    reference = body.get('reference')
    if not group_id or reference is None:
        return jsonify({'error': 'Missing fields'}), 400

    sid = get_session_id()
    if not session_exists(sid):
        return jsonify({'error': 'No data'}), 404

    # group_id is "pin-corekey"
    pin = group_id.split('-')[0]
    ck = group_id[len(pin)+1:]

    fixed_count = fix_rows_in_db(sid, pin, ck, reference)
    updated = clean_for_json(compute_groups_from_db(sid))

    return jsonify({
        'success': True,
        'fixedCount': fixed_count,
        'allGroups': updated['allGroups'],
        'inconsistentGroups': updated['inconsistentGroups'],
        'inconsistentCount': updated['inconsistentCount']
    })


@app.route('/api/fix-all', methods=['POST'])
def fix_all_groups():
    sid = get_session_id()
    if not session_exists(sid):
        return jsonify({'error': 'No data'}), 404

    total_fixed, groups_fixed = fix_all_in_db(sid)
    updated = clean_for_json(compute_groups_from_db(sid))

    return jsonify({
        'success': True,
        'fixedCount': total_fixed,
        'groupsFixed': groups_fixed,
        'message': f'Fixed {total_fixed} row(s) across {groups_fixed} group(s)',
        'allGroups': updated['allGroups'],
        'inconsistentGroups': updated['inconsistentGroups'],
        'inconsistentCount': updated['inconsistentCount']
    })


@app.route('/api/download')
def download_csv():
    sid = get_session_id()
    if not session_exists(sid):
        return jsonify({'error': 'No data'}), 404

    columns = load_columns(sid)
    rows = load_rows(sid)

    output = io.StringIO()
    csv.DictWriter(output, fieldnames=columns).writeheader()
    csv.DictWriter(output, fieldnames=columns).writerows(rows)
    output.seek(0)
    return Response(output.getvalue(), mimetype='text/csv',
                    headers={'Content-Disposition': 'attachment; filename=fixed_data.csv'})


@app.route('/api/best-rank')
def best_rank():
    sid = get_session_id()
    if not session_exists(sid):
        return jsonify({'error': 'No data'}), 404

    columns = load_columns(sid)

    # Done entirely in MySQL with a subquery
    db = get_db()
    cur = db.cursor(dictionary=True)
    try:
        cur.execute("""
            SELECT r.row_data
            FROM csv_rows r
            INNER JOIN (
                SELECT
                    JSON_UNQUOTE(JSON_EXTRACT(row_data, '$.college_id'))        AS college_id,
                    JSON_UNQUOTE(JSON_EXTRACT(row_data, '$.alloted_institute')) AS alloted_institute,
                    JSON_UNQUOTE(JSON_EXTRACT(row_data, '$.branch'))            AS branch,
                    JSON_UNQUOTE(JSON_EXTRACT(row_data, '$.alloted_quota'))     AS alloted_quota,
                    JSON_UNQUOTE(JSON_EXTRACT(row_data, '$.alloted_category'))  AS alloted_category,
                    JSON_UNQUOTE(JSON_EXTRACT(row_data, '$.round'))             AS round,
                    MIN(CAST(JSON_UNQUOTE(JSON_EXTRACT(row_data, '$.alloted_rank')) AS UNSIGNED)) AS best_rank
                FROM csv_rows
                WHERE session_id = %s
                GROUP BY college_id, alloted_institute, branch,
                         alloted_quota, alloted_category, round
            ) best
            ON  session_id = %s
            AND JSON_UNQUOTE(JSON_EXTRACT(r.row_data, '$.college_id'))        = best.college_id
            AND JSON_UNQUOTE(JSON_EXTRACT(r.row_data, '$.alloted_institute')) = best.alloted_institute
            AND JSON_UNQUOTE(JSON_EXTRACT(r.row_data, '$.branch'))            = best.branch
            AND JSON_UNQUOTE(JSON_EXTRACT(r.row_data, '$.alloted_quota'))     = best.alloted_quota
            AND JSON_UNQUOTE(JSON_EXTRACT(r.row_data, '$.alloted_category'))  = best.alloted_category
            AND JSON_UNQUOTE(JSON_EXTRACT(r.row_data, '$.round'))             = best.round
            AND CAST(JSON_UNQUOTE(JSON_EXTRACT(r.row_data, '$.alloted_rank')) AS UNSIGNED) = best.best_rank
            ORDER BY best.alloted_institute
        """, (sid, sid))
        rows = [json.loads(r['row_data']) for r in cur.fetchall()]
    finally:
        cur.close()
        db.close()

    return jsonify({'rows': clean_for_json(rows), 'columns': columns})


@app.route('/api/best-rank/download')
def download_best_rank_csv():
    resp = best_rank()
    data = resp.get_json()
    rows, columns = data['rows'], data['columns']

    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=columns)
    writer.writeheader()
    writer.writerows(rows)
    output.seek(0)
    return Response(output.getvalue(), mimetype='text/csv',
                    headers={'Content-Disposition': 'attachment; filename=best_rank.csv'})


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000)