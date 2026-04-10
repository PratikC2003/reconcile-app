import argparse
import re
import pandas as pd


# ---------------- CLEANING ---------------- #

def clean_college_name(raw: str) -> str:
    if not isinstance(raw, str):
        return raw
    return re.sub(r'-[^,]+,', ',', raw).strip()


def parse_branch(raw: str) -> tuple[str, str]:
    raw = str(raw).strip()

    match = re.match(r'^([A-Z][A-Z0-9\-]*)\s*-\s*\n?\s*(.+)$', raw, re.DOTALL)
    if match:
        code = match.group(1).strip()
        name = match.group(2).replace('\n', ' ').strip()
        name = re.sub(r'\s{2,}', ' ', name)
        return code, name

    return '', raw


def format_branch(raw: str) -> str:
    code, name = parse_branch(raw)
    return f'{name} ({code})' if code else name


# ---------------- CORE TRANSFORM ---------------- #

def transform_df(df_raw: pd.DataFrame, round_number: int) -> pd.DataFrame:
    header_mask = df_raw[0] == 'College Code'
    header_rows = df_raw.index[header_mask].tolist()

    records = []

    for i, h_row in enumerate(header_rows):
        start = h_row + 1
        end = header_rows[i + 1] if i + 1 < len(header_rows) else len(df_raw)

        header = df_raw.iloc[h_row]

        branch_cols = {
            idx: val
            for idx, val in enumerate(header)
            if idx >= 3 and pd.notna(val) and val != 'College Code'
        }

        if not branch_cols:
            continue

        data_slice = df_raw.iloc[start:end]
        data_slice = data_slice[
            data_slice[0].notna() & (data_slice[0] != 'College Code')
        ]

        for _, row in data_slice.iterrows():
            college_code = str(row[0]).strip()
            college_name = clean_college_name(str(row[1]).strip())
            category = str(row[2]).strip()

            for col_idx, branch_raw in branch_cols.items():
                cell = row[col_idx]

                if pd.isna(cell):
                    continue

                try:
                    rank = int(float(str(cell).strip()))
                except ValueError:
                    continue

                records.append({
                    'College Code': college_code,
                    'College Name': college_name,
                    'Branch': format_branch(branch_raw),
                    'Category': category,
                    'Rank': rank,
                    'Round': round_number,
                })

    return pd.DataFrame(records)


# ---------------- ROUND DETECTION ---------------- #

def extract_round(sheet_name: str) -> int:
    """
    Extract round number from sheet name like:
    'Round 1', 'R2', 'ROUND_3', etc.
    """
    match = re.search(r'(\d+)', sheet_name)
    return int(match.group(1)) if match else 0


# ---------------- EXCEL PROCESSOR ---------------- #

def process_excel(input_path: str) -> pd.DataFrame:
    xls = pd.ExcelFile(input_path)

    all_data = []

    print(f"\nFound sheets: {xls.sheet_names}\n")

    for sheet in xls.sheet_names:
        print(f"Processing → {sheet}")

        df_raw = pd.read_excel(
            xls,
            sheet_name=sheet,
            header=None,
            dtype=str
        )

        round_number = extract_round(sheet)

        if round_number == 0:
            print(f"⚠️ Could not detect round in '{sheet}', defaulting to 0")

        df_processed = transform_df(df_raw, round_number)

        if not df_processed.empty:
            all_data.append(df_processed)

    if not all_data:
        raise ValueError("No valid data extracted from Excel.")

    final_df = pd.concat(all_data, ignore_index=True)

    final_df.sort_values(
        ['College Name', 'Branch', 'Category', 'Round'],
        inplace=True,
        ignore_index=True
    )

    return final_df


# ---------------- CLI ---------------- #

def main():
    parser = argparse.ArgumentParser(
        description='Convert COMEDK Excel cutoff (multi-round) to long format.'
    )

    parser.add_argument(
        '--input', '-i',
        default='comedk 2025 cutoff.xlsx',
        help='Path to Excel file'
    )

    parser.add_argument(
        '--output', '-o',
        default='comedk_2025_all_rounds.csv',
        help='Output CSV file'
    )

    args = parser.parse_args()

    print(f"Reading Excel: {args.input}")

    df = process_excel(args.input)

    df.to_csv(args.output, index=False)

    print("\n✅ DONE")
    print(f"Saved     : {args.output}")
    print(f"Rows      : {len(df):,}")
    print(f"Colleges  : {df['College Name'].nunique()}")
    print(f"Branches  : {df['Branch'].nunique()}")
    print(f"Rounds    : {sorted(df['Round'].unique())}")
    print(f"Categories: {sorted(df['Category'].unique())}")


if __name__ == '__main__':
    main()