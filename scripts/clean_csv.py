"""
Clean CSV files by removing metadata lines from beginning and end

Usage:
    python -m scripts.clean_csv

Requires config.json with:
  - paths.db1, paths.db2
  - clean_csv.remove_first_lines, clean_csv.remove_last_lines
"""

import json
from pathlib import Path

def load_config():
    with open("config.json") as f:
        return json.load(f)

def clean_file(file: Path, first: int, last: int) -> bool:
    try:
        lines = file.read_text(encoding="utf-8", errors="ignore").splitlines()
        
        if len(lines) <= first + last:
            print(f"[WARN] {file.name}: too few lines ({len(lines)}), skipping")
            return False

        cleaned = lines[first:len(lines)-last]
        file.write_text("\n".join(cleaned), encoding="utf-8")
        print(f"[PASS] {file.name}")
        return True
    except Exception as e:
        print(f"[FAIL] {file.name}: {e}")
        return False

def clean_folder(folder: Path, first: int, last: int):
    csv_files = list(folder.glob("*.csv"))
    
    if not csv_files:
        print(f"[WARN] No CSV files in {folder}")
        return
    
    success = sum(1 for f in csv_files if clean_file(f, first, last))
    print(f"\nCleaned {success}/{len(csv_files)} files")

def main():
    cfg = load_config()
    
    folder1 = Path(cfg["paths"]["db1"])
    folder2 = Path(cfg["paths"]["db2"])
    first = cfg.get("clean_csv", {}).get("remove_first_lines", 0)
    last = cfg.get("clean_csv", {}).get("remove_last_lines", 0)

    print("=" * 60)
    print("CLEANING CSV FILES")
    print("=" * 60)
    print(f"Config: remove first {first} lines, last {last} lines\n")

    print(f"Cleaning: {folder1}")
    clean_folder(folder1, first, last)

    print(f"\nCleaning: {folder2}")
    clean_folder(folder2, first, last)

    print("\n[PASS] Done")

if __name__ == "__main__":
    main()