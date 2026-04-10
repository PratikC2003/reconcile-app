from flask import Flask, render_template, request, jsonify, Response, session
import pandas as pd
import re
import json
import uuid

app = Flask(__name__)
app.secret_key = 'dev-secret-key-change-in-production'

# In-memory storage: session_id -> {columns, rows, groups}
USER_DATA = {}


def get_session_id():
    """Get or create unique session ID for user."""
    if 'sid' not in session:
        session['sid'] = str(uuid.uuid4())
    return session['sid']


def clean_for_json(obj):
    """Convert NaN/Inf to None for valid JSON serialization."""
    import math
    if isinstance(obj, dict):
        return {k: clean_for_json(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [clean_for_json(v) for v in obj]
    elif isinstance(obj, float):
        if math.isnan(obj) or math.isinf(obj):
            return None
        return obj
    return obj


def extract_pin(text):
    match = re.search(r'\b\d{6}\b', text)
    return match.group(0) if match else None


def core_key(text):
    return re.sub(r'[^a-z0-9]', '', text.lower())


def compute_groups(data):
    """Compute groups from data, preserving full row data."""
    groups = {}

    for i, row in enumerate(data):
        raw = row.get('alloted_institute', '')
        pin = extract_pin(raw)
        if not pin:
            continue

        key = f"{pin}|{core_key(raw)}"
        if key not in groups:
            groups[key] = []
        groups[key].append({'index': i, 'raw': raw, 'fullRow': row})

    # Build result
    all_groups = []
    inconsistent = []

    for key, items in groups.items():
        unique_raws = list(dict.fromkeys([item['raw'] for item in items]))
        pin = key.split('|')[0]
        group_id = key.replace('|', '-')

        group_data = {
            'groupId': group_id,
            'pin': pin,
            'key': key,
            'items': items,
            'variants': unique_raws,
            'count': len(items),
            'hasWarning': len(unique_raws) > 1
        }

        all_groups.append(group_data)
        if len(unique_raws) > 1:
            inconsistent.append(group_data)

    all_groups.sort(key=lambda x: x['pin'])
    inconsistent.sort(key=lambda x: x['pin'])

    return {
        'allGroups': all_groups,
        'inconsistentGroups': inconsistent,
        'totalGroups': len(all_groups),
        'inconsistentCount': len(inconsistent)
    }


@app.route('/', methods=['GET', 'POST'])
def index():
    data = None
    columns = None
    has_data = False

    if request.method == 'POST':
        file = request.files.get('file')
        if file:
            df = pd.read_csv(file)
            data = df.to_dict(orient='records')
            columns = df.columns.tolist()

            # Clean NaN values for JSON
            data = clean_for_json(data)

            # Compute groups
            groups = compute_groups(data)
            groups = clean_for_json(groups)

            # Store in session memory
            sid = get_session_id()
            USER_DATA[sid] = {
                'columns': columns,
                'rows': data,
                'groups': groups
            }
            has_data = True

    # Check for existing session data on GET
    sid = get_session_id()
    if sid in USER_DATA:
        has_data = True

    return render_template('index.html', data=data, columns=columns, has_data=has_data)


@app.route('/api/groups')
def get_groups():
    """Get computed groups for current session with chunked loading support."""
    sid = get_session_id()

    if sid not in USER_DATA:
        return jsonify({'error': 'No data'}), 404

    import json
    
    def generate():
        groups = USER_DATA[sid]['groups']
        all_groups = groups.get('allGroups', [])
        inconsistent = groups.get('inconsistentGroups', [])
        total = len(all_groups)
        
        # Send metadata first
        yield json.dumps({
            'type': 'meta',
            'totalGroups': groups.get('totalGroups', 0),
            'inconsistentCount': groups.get('inconsistentCount', 0),
            'chunkCount': (total + 49) // 50  # 50 per chunk
        }) + '\n'
        
        # Send groups in chunks
        chunk_size = 50
        for i in range(0, total, chunk_size):
            chunk = all_groups[i:i + chunk_size]
            yield json.dumps({
                'type': 'chunk',
                'index': i // chunk_size,
                'progress': min(100, int((i + len(chunk)) / total * 100)),
                'allGroups': chunk
            }) + '\n'
        
        # Send inconsistent groups at the end
        yield json.dumps({
            'type': 'done',
            'progress': 100,
            'inconsistentGroups': inconsistent
        }) + '\n'

    return Response(generate(), mimetype='application/x-ndjson')


@app.route('/api/fix', methods=['POST'])
def fix_group():
    """
    Fix inconsistent alloted_institute values in a group.
    Replaces all variants with the reference (first variant) value.
    Only updates the alloted_institute column, not the entire row.
    """
    data = request.get_json()
    group_id = data.get('groupId')
    reference = data.get('reference')  # The consistent reference value

    if not group_id or reference is None:
        return jsonify({'error': 'Missing required fields: groupId, reference'}), 400

    sid = get_session_id()
    if sid not in USER_DATA:
        return jsonify({'error': 'No data'}), 404

    user_data = USER_DATA[sid]
    rows = user_data['rows']
    all_groups = user_data['groups']['allGroups']

    # Find the target group
    target_group = None
    for g in all_groups:
        if g['groupId'] == group_id:
            target_group = g
            break

    if not target_group:
        return jsonify({'error': 'Group not found'}), 404

    # Count how many rows were fixed
    fixed_count = 0

    # Update each inconsistent row in the group
    for item in target_group['items']:
        row_idx = item['index']
        current_value = rows[row_idx].get('alloted_institute', '')

        # Only update if this row has a different value than reference
        if current_value != reference:
            rows[row_idx]['alloted_institute'] = reference
            fixed_count += 1

    # Re-compute groups with the fixed data
    updated_groups = compute_groups(rows)
    updated_groups = clean_for_json(updated_groups)

    # Update in-memory storage
    USER_DATA[sid]['rows'] = rows
    USER_DATA[sid]['groups'] = updated_groups

    return jsonify({
        'success': True,
        'fixedCount': fixed_count,
        'groupId': group_id,
        'reference': reference,
        'allGroups': updated_groups['allGroups'],
        'inconsistentGroups': updated_groups['inconsistentGroups'],
        'inconsistentCount': updated_groups['inconsistentCount']
    })


@app.route('/api/fix-all', methods=['POST'])
def fix_all_groups():
    """
    Fix ALL inconsistent groups for current session.
    Replaces all inconsistent alloted_institute values with their reference values.
    """
    sid = get_session_id()

    if sid not in USER_DATA:
        return jsonify({'error': 'No data'}), 404

    user_data = USER_DATA[sid]
    rows = user_data['rows']
    all_groups = user_data['groups']['allGroups']

    # Find all inconsistent groups
    inconsistent_groups = [g for g in all_groups if g['hasWarning']]

    if not inconsistent_groups:
        return jsonify({
            'success': True,
            'fixedCount': 0,
            'message': 'No inconsistencies found - all groups are already consistent!',
            'allGroups': all_groups,
            'inconsistentGroups': [],
            'inconsistentCount': 0
        })

    # Fix all inconsistent groups
    total_fixed = 0

    for group in inconsistent_groups:
        reference = group['variants'][0]

        for item in group['items']:
            row_idx = item['index']
            current_value = rows[row_idx].get('alloted_institute', '')

            if current_value != reference:
                rows[row_idx]['alloted_institute'] = reference
                total_fixed += 1

    # Re-compute groups with the fixed data
    updated_groups = compute_groups(rows)
    updated_groups = clean_for_json(updated_groups)

    # Update in-memory storage
    USER_DATA[sid]['rows'] = rows
    USER_DATA[sid]['groups'] = updated_groups

    return jsonify({
        'success': True,
        'fixedCount': total_fixed,
        'groupsFixed': len(inconsistent_groups),
        'message': f'Fixed {total_fixed} row(s) across {len(inconsistent_groups)} group(s)',
        'allGroups': updated_groups['allGroups'],
        'inconsistentGroups': updated_groups['inconsistentGroups'],
        'inconsistentCount': updated_groups['inconsistentCount']
    })


@app.route('/api/download')
def download_csv():
    """
    Download the current (possibly fixed) data as a CSV file.
    """
    import io
    import csv

    sid = get_session_id()

    if sid not in USER_DATA:
        return jsonify({'error': 'No data'}), 404

    columns = USER_DATA[sid]['columns']
    rows = USER_DATA[sid]['rows']

    # Generate CSV in memory
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=columns)
    writer.writeheader()
    writer.writerows(rows)

    # Return as downloadable file
    output.seek(0)
    return Response(
        output.getvalue(),
        mimetype='text/csv',
        headers={
            'Content-Disposition': 'attachment; filename=fixed_data.csv',
            'Content-Type': 'text/csv; charset=utf-8'
        }
    )


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000)