"""
Extract column schemas from database directories to JSON.

Usage:
    python -m scripts.get_columns db1              # Extract schema from db1
    python -m scripts.get_columns db1 db2          # Extract from db1 and db2
    python -m scripts.get_columns db1 db2 merged   # Extract from all three

Requires config.json with paths defined for the specified database keys.
"""

import json
import sys
import pandas as pd
from pathlib import Path

def load_config():
    with open("config.json") as f:
        return json.load(f)

def get_columns(csv_file):
    try:
        df = pd.read_csv(csv_file, nrows=0)
        return list(df.columns)
    except Exception as e:
        return [f"ERROR: {e}"]

def extract_schema(db_path):
    if not db_path.exists() or not db_path.is_dir():
        raise ValueError(f"Invalid path: {db_path}")
    
    schema = {}
    for csv_file in sorted(db_path.glob("*.csv")):
        schema[csv_file.stem] = get_columns(csv_file)
    return schema

def main():
    if len(sys.argv) < 2:
        print("Usage: python -m scripts.get_columns <db_key1> [db_key2] ...")
        print("\nExamples:")
        print("  python -m scripts.get_columns db1")
        print("  python -m scripts.get_columns db1 db2 merged")
        sys.exit(1)

    db_keys = sys.argv[1:]

    print("=" * 60)
    print("SCHEMA EXTRACTION")
    print("=" * 60)

    cfg = load_config()
    paths = cfg.get("paths", {})

    missing = [k for k in db_keys if k not in paths]
    if missing:
        print(f"[FAIL] Unknown keys: {missing}")
        print(f"Available: {list(paths.keys())}")
        sys.exit(1)

    result = {}
    for key in db_keys:
        db_path = Path(paths[key])
        print(f"\nExtracting: {key} ({db_path})")
        
        try:
            schema = extract_schema(db_path)
            result[key] = schema
            print(f"[PASS] Found {len(schema)} tables")
        except Exception as e:
            print(f"[FAIL] {e}")
            sys.exit(1)

    output_file = Path(f"schema_{'_'.join(db_keys)}.json")
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, ensure_ascii=False)

    print(f"\n[PASS] Saved to: {output_file}")

if __name__ == "__main__":
    main()