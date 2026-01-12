"""
Inspect CSV files for embedded newline characters

Usage:
    python -m scripts.test_newlines                     # Default: merged_csv/Authors.csv
    python -m scripts.test_newlines path/to/file.csv    # Inspect specific file
"""

import sys
import pandas as pd
from pathlib import Path


def inspect(file_path: Path):
    if not file_path.exists():
        print(f"[FAIL] File not found: {file_path}")
        return

    print("=" * 60)
    print(f"NEWLINE INSPECTION: {file_path.name}")
    print("=" * 60)

    try:
        df = pd.read_csv(file_path, dtype=str)
        print(f"DataFrame rows: {len(df)}")
    except Exception as e:
        print(f"[FAIL] Cannot load CSV: {e}")
        return

    try:
        with file_path.open("r", encoding="utf-8", errors="ignore") as f:
            physical_lines = sum(1 for _ in f)
        print(f"Physical lines: {physical_lines}")
    except Exception as e:
        print(f"[FAIL] Cannot read file: {e}")
        return

    has_newlines = df.apply(
        lambda col: col.str.contains(r"\r|\n", na=False), axis=0
    ).any(axis=1)

    bad_rows = df[has_newlines]
    count = len(bad_rows)

    if count == 0:
        print("\n[PASS] No embedded newlines found")
        return

    print(f"\n[WARN] Found {count} rows with embedded newlines")
    print(f"Problematic indices: {bad_rows.index.tolist()}")

    print("\n" + "-" * 60)
    for idx in bad_rows.index:
        print(f"\n--- Row {idx} ---")
        for col, val in df.loc[idx].items():
            if pd.notna(val) and ("\n" in str(val) or "\r" in str(val)):
                print(f"  {col}: {repr(val)}")


def main():
    if len(sys.argv) > 1:
        file_path = Path(sys.argv[1])
    else:
        file_path = Path("merged_csv/Authors.csv")
        print(f"No file specified, using: {file_path}")

    inspect(file_path)


if __name__ == "__main__":
    main()
