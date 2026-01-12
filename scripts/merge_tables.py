"""
Merge tables from db1 and db2 into a unified merged database

Usage:
    python -m scripts.merge_tables

Requires config.json with paths.db1, paths.db2, paths.merged defined.
"""

import json
import pandas as pd
from pathlib import Path


def load_config():
    with open("config.json") as f:
        return json.load(f)


def load_csv(path):
    try:
        return pd.read_csv(path, dtype=str)
    except Exception as e:
        print(f"[FAIL] Cannot read {path}: {e}")
        return None


def align_columns(dfs):
    all_cols = sorted({col for df in dfs for col in df.columns})
    aligned = []
    for df in dfs:
        for col in all_cols:
            if col not in df.columns:
                df[col] = ""
        aligned.append(df[all_cols])
    return aligned


def merge_table(table_name, db1_path, db2_path, output_path):
    dfs = []

    file1 = db1_path / table_name
    file2 = db2_path / table_name

    if file1.exists():
        df1 = load_csv(file1)
        if df1 is not None:
            dfs.append(df1)

    if file2.exists():
        df2 = load_csv(file2)
        if df2 is not None:
            dfs.append(df2)

    if not dfs:
        print(f"[WARN] {table_name}: no data in either database")
        return False

    if len(dfs) == 1:
        merged = dfs[0]
        src = "db1" if file1.exists() else "db2"
        print(f"[PASS] {table_name}: {len(merged)} rows (from {src} only)")
    else:
        aligned = align_columns(dfs)
        merged = pd.concat(aligned, ignore_index=True).drop_duplicates()
        print(f"[PASS] {table_name}: {len(merged)} rows (merged)")

    try:
        merged.to_csv(output_path / table_name, index=False)
        return True
    except Exception as e:
        print(f"[FAIL] {table_name}: save failed - {e}")
        return False


def main():
    cfg = load_config()
    db1 = Path(cfg["paths"]["db1"])
    db2 = Path(cfg["paths"]["db2"])
    output = Path(cfg["paths"]["merged"])
    output.mkdir(exist_ok=True)

    print("=" * 60)
    print("MERGING TABLES")
    print("=" * 60)
    print(f"DB1: {db1}")
    print(f"DB2: {db2}")
    print(f"Output: {output}\n")

    tables_db1 = {p.name for p in db1.glob("*.csv")}
    tables_db2 = {p.name for p in db2.glob("*.csv")}
    all_tables = sorted(tables_db1 | tables_db2)

    print(f"Tables: {len(all_tables)} total")
    print(f"  In db1: {len(tables_db1)}")
    print(f"  In db2: {len(tables_db2)}")
    print(f"  In both: {len(tables_db1 & tables_db2)}\n")

    success = sum(1 for t in all_tables if merge_table(t, db1, db2, output))

    print(
        f"\n[{'PASS' if success == len(all_tables) else 'WARN'}] Merged {success}/{len(all_tables)} tables"
    )


if __name__ == "__main__":
    main()
