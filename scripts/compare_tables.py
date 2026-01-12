"""
Universal table comparison tool for database analysis.

Usage:
    # Compare Books across db1, db2, merged (default)
    python -m scripts.compare_tables Books

    # Compare Books across specific databases
    python -m scripts.compare_tables Books --databases db1 db2

    # Compare Books vs Books1 within merged database
    python -m scripts.compare_tables Books Books1 --database merged

    # Find duplicates between db1 and db2
    python -m scripts.compare_tables --find-duplicates

    # Verify merged database integrity
    python -m scripts.compare_tables --verify-merge

Requires config.json with paths.db1, paths.db2, paths.merged defined.
"""

import sys
import json
import argparse
import hashlib
import pandas as pd
from pathlib import Path


def load_config():
    with open("config.json") as f:
        return json.load(f)


def get_path(key):
    return Path(load_config()["paths"][key])


def load_csv(path):
    try:
        return pd.read_csv(path, dtype=str).fillna("")
    except:
        return None


def align_two(df1, df2):
    cols = sorted(set(df1.columns) | set(df2.columns))
    for col in cols:
        if col not in df1.columns:
            df1[col] = ""
        if col not in df2.columns:
            df2[col] = ""
    return df1[cols], df2[cols]


def row_hash(row):
    return hashlib.md5("||".join(str(x) for x in row.values).encode()).hexdigest()


def find_missing(src, tgt):
    cols = list(set(src.columns) & set(tgt.columns))
    src_set = set(src[cols].apply(lambda r: tuple(r.values), axis=1))
    tgt_set = set(tgt[cols].apply(lambda r: tuple(r.values), axis=1))
    return len(src_set - tgt_set)


def find_duplicates():
    print("=" * 60)
    print("FINDING DUPLICATES")
    print("=" * 60)

    db1 = get_path("db1")
    db2 = get_path("db2")
    cfg = load_config()
    out_dir = Path(cfg["paths"].get("duplicates", "duplicates"))
    out_dir.mkdir(exist_ok=True)

    total_dups = 0
    tables_with_dups = 0

    for f1 in sorted(db1.glob("*.csv")):
        f2 = db2 / f1.name
        if not f2.exists():
            continue

        df1, df2 = load_csv(f1), load_csv(f2)
        if df1 is None or df2 is None:
            continue

        dups = df1.merge(df2, how="inner")
        if len(dups) > 0:
            out_file = out_dir / f"duplicates_{f1.name}"
            dups.to_csv(out_file, index=False)
            print(f"[PASS] {f1.name}: {len(dups)} duplicates")
            total_dups += len(dups)
            tables_with_dups += 1
        else:
            print(f"[PASS] {f1.name}: no duplicates")

    print(f"\nTables with duplicates: {tables_with_dups}")
    print(f"Total duplicate rows: {total_dups}")


def verify_merge():
    print("=" * 60)
    print("VERIFYING MERGE")
    print("=" * 60)

    db1, db2 = get_path("db1"), get_path("db2")
    merged = get_path("merged")

    for db_path, label in [(db1, "db1"), (db2, "db2")]:
        print(f"\n--- {label.upper()} ---")
        total_missing = 0

        for tf in sorted(db_path.glob("*.csv")):
            mf = merged / tf.name
            if not mf.exists():
                print(f"[FAIL] {tf.name}: not in merged")
                continue

            src, mrg = load_csv(tf), load_csv(mf)
            if src is None or mrg is None:
                continue

            src, mrg = align_two(src, mrg)
            missing = find_missing(src, mrg)

            if missing > 0:
                print(
                    f"[FAIL] {tf.name}: {missing} missing (src={len(src)}, merged={len(mrg)})"
                )
                total_missing += missing
            else:
                print(f"[PASS] {tf.name}: OK")

        if total_missing == 0:
            print(f"[PASS] All {label} data preserved")
        else:
            print(f"[FAIL] {total_missing} rows missing from {label}")


def compare_across(table_name, databases):
    print("=" * 60)
    print(f"COMPARING: {table_name}")
    print("=" * 60)

    csv_name = f"{table_name}.csv"
    dfs = {}

    for db_key in databases:
        db_path = get_path(db_key)
        df = load_csv(db_path / csv_name)
        if df is not None:
            dfs[db_key] = df
            print(f"  {db_key}: {len(df)} rows")
        else:
            print(f"  [WARN] {db_key}: not found")

    if len(dfs) < 2:
        print("[FAIL] Need at least 2 databases")
        return

    keys = list(dfs.keys())
    for i in range(len(keys)):
        for j in range(i + 1, len(keys)):
            k1, k2 = keys[i], keys[j]
            df1, df2 = align_two(dfs[k1].copy(), dfs[k2].copy())
            dups = df1.merge(df2, how="inner")
            print(f"\n{k1} vs {k2}:")
            print(f"  Shared rows: {len(dups)}")
            print(f"  Only in {k1}: {len(df1) - len(dups)}")
            print(f"  Only in {k2}: {len(df2) - len(dups)}")


def compare_within(table1, table2, database):
    print("=" * 60)
    print(f"COMPARING: {table1} vs {table2} in {database}")
    print("=" * 60)

    db_path = get_path(database)
    df1 = load_csv(db_path / f"{table1}.csv")
    df2 = load_csv(db_path / f"{table2}.csv")

    if df1 is None:
        print(f"[FAIL] {table1} not found")
        return
    if df2 is None:
        print(f"[FAIL] {table2} not found")
        return

    df1, df2 = align_two(df1, df2)
    dups = df1.merge(df2, how="inner")

    print(f"{table1}: {len(df1)} rows")
    print(f"{table2}: {len(df2)} rows")
    print(f"Shared rows: {len(dups)}")
    print(f"Only in {table1}: {len(df1) - len(dups)}")
    print(f"Only in {table2}: {len(df2) - len(dups)}")


def main():
    parser = argparse.ArgumentParser(description="Table comparison tool")
    parser.add_argument("tables", nargs="*", help="Table name(s)")
    parser.add_argument("--database", help="Single database for within-db comparison")
    parser.add_argument(
        "--databases", nargs="+", help="Multiple databases for cross-db comparison"
    )
    parser.add_argument("--find-duplicates", action="store_true")
    parser.add_argument("--verify-merge", action="store_true")
    args = parser.parse_args()

    if args.find_duplicates:
        find_duplicates()
        return

    if args.verify_merge:
        verify_merge()
        return

    if not args.tables:
        parser.print_help()
        sys.exit(1)

    tables = [t.removesuffix(".csv") for t in args.tables]

    if len(tables) == 2 and args.database:
        compare_within(tables[0], tables[1], args.database)
    elif len(tables) == 1 and args.databases:
        compare_across(tables[0], args.databases)
    elif len(tables) == 1:
        compare_across(tables[0], ["db1", "db2", "merged"])
    else:
        print("[FAIL] Invalid arguments")
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
