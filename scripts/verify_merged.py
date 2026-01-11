"""
Verify that merged database contains all data from source databases.

Usage:
    python -m scripts.erify_merged

Requires config.json with paths.db1, paths.db2, paths.merged defined.
"""

import json
import hashlib
import pandas as pd
from pathlib import Path

def load_config():
    with open("config.json") as f:
        return json.load(f)

def load_csv(path):
    try:
        return pd.read_csv(path, dtype=str).fillna("")
    except:
        return None

def row_hash(row):
    s = "||".join(str(x) for x in row.values)
    return hashlib.md5(s.encode()).hexdigest()

def align_columns(df1, df2):
    all_cols = sorted(set(df1.columns) | set(df2.columns))
    for col in all_cols:
        if col not in df1.columns:
            df1[col] = ""
        if col not in df2.columns:
            df2[col] = ""
    return df1[all_cols], df2[all_cols]

def find_missing(source_df, target_df):
    cols = list(set(source_df.columns) & set(target_df.columns))
    src_rows = set(source_df[cols].apply(lambda r: tuple(r.values), axis=1))
    tgt_rows = set(target_df[cols].apply(lambda r: tuple(r.values), axis=1))
    return len(src_rows - tgt_rows)

def verify_db(db_path, db_label, merged_path):
    print(f"\n{'=' * 60}")
    print(f"VERIFYING {db_label.upper()}")
    print("=" * 60)

    tables = sorted(db_path.glob("*.csv"))
    if not tables:
        print(f"[WARN] No tables in {db_path}")
        return

    total_missing = 0
    issues = 0

    for table_file in tables:
        merged_file = merged_path / table_file.name
        
        if not merged_file.exists():
            print(f"[FAIL] {table_file.name}: not in merged")
            issues += 1
            continue

        src_df = load_csv(table_file)
        mrg_df = load_csv(merged_file)
        
        if src_df is None or mrg_df is None:
            print(f"[FAIL] {table_file.name}: load error")
            issues += 1
            continue

        src_df, mrg_df = align_columns(src_df, mrg_df)
        missing = find_missing(src_df, mrg_df)

        if missing > 0:
            print(f"[FAIL] {table_file.name}: {missing} missing rows (src={len(src_df)}, merged={len(mrg_df)})")
            total_missing += missing
            issues += 1
        else:
            print(f"[PASS] {table_file.name}: {len(src_df)} -> {len(mrg_df)}")

    print(f"\nSummary for {db_label}:")
    print(f"  Tables: {len(tables)}")
    print(f"  Issues: {issues}")
    print(f"  Missing rows: {total_missing}")
    
    return total_missing == 0

def main():
    cfg = load_config()
    db1 = Path(cfg["paths"]["db1"])
    db2 = Path(cfg["paths"]["db2"])
    merged = Path(cfg["paths"]["merged"])

    print("=" * 60)
    print("MERGE VERIFICATION")
    print("=" * 60)

    ok1 = verify_db(db1, "db1", merged)
    ok2 = verify_db(db2, "db2", merged)

    if ok1 and ok2:
        print(f"\n[PASS] All data preserved in merge")
    else:
        print(f"\n[FAIL] Some data missing from merge")

if __name__ == "__main__":
    main()