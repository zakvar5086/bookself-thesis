"""
ISBN Analyzer - Categorizes books by ISBN availability with OpenLibrary API lookups

Usage:
    python -m scripts.isbn_analyze

Requires config.json with paths.db1 and paths.db2 defined.
Outputs JSON files to isbn_analysis_results/ directory:
  - case1_with_isbn.json      (books that already have ISBN)
  - case2a_no_isbn_found.json (API returned no ISBN)
  - case2b_multiple_isbns.json (API returned multiple ISBNs)
  - case2c_single_isbn.json   (API returned single ISBN pair)
  - stats.json
"""

import json
import time
import requests
import pandas as pd
from pathlib import Path

TABLES = ["Books", "Books1", "MissingBooks", "NB"]
API_URL = "https://openlibrary.org/search.json"
API_DELAY = 0.5


def load_config():
    with open("config.json") as f:
        return json.load(f)


def normalize_isbn(isbn):
    if not isbn or pd.isna(isbn):
        return ""
    return str(isbn).replace("-", "").replace(" ", "").strip().upper()


def is_valid_isbn(isbn):
    n = normalize_isbn(isbn)
    if not n or n.lower() in ["nan", "none", "null"]:
        return False
    if not all(c in "0123456789X" for c in n):
        return False
    return len(n) in [10, 13]


def normalize_text(v):
    if pd.isna(v):
        return ""
    return str(v).replace("\r", " ").replace("\n", " ").strip().lower()


def fetch_isbn(title, retries=3):
    if not title or not title.strip():
        return []
    for attempt in range(retries):
        try:
            resp = requests.get(
                API_URL,
                params={"title": title, "fields": "isbn", "limit": 1},
                timeout=10,
            )
            resp.raise_for_status()
            data = resp.json()
            if data.get("numFound", 0) == 0 or not data.get("docs"):
                return []
            return data["docs"][0].get("isbn", [])
        except Exception as e:
            print(f"  [WARN] API error (attempt {attempt + 1}): {e}")
            if attempt < retries - 1:
                time.sleep(1)
    return None


def classify_isbns(isbn_list):
    isbn10s, isbn13s = [], []
    for isbn in isbn_list:
        n = normalize_isbn(isbn)
        if len(n) == 10:
            isbn10s.append(n)
        elif len(n) == 13:
            isbn13s.append(n)
    return isbn10s, isbn13s


def main():
    print("=" * 60)
    print("ISBN ANALYZER")
    print("=" * 60)

    cfg = load_config()
    db1_path = Path(cfg["paths"]["db1"])
    db2_path = Path(cfg["paths"]["db2"])
    output_dir = Path("isbn_analysis_results")
    output_dir.mkdir(exist_ok=True)

    # Results containers
    case1_with_isbn = {}  # {ISBN: [tables]}
    case2a_no_isbn = {}  # {title: [tables]}
    case2b_multiple = {}  # {title: {tables: [], isbns: []}}
    case2c_single = {}  # {"isbn10|isbn13": {title: [tables]}}

    stats = {
        "total": 0,
        "with_isbn": 0,
        "without_isbn": 0,
        "api_calls": 0,
        "api_errors": 0,
    }
    api_cache = {}

    for db_name, db_path in [("db1", db1_path), ("db2", db2_path)]:
        print(f"\n[INFO] Processing {db_name}: {db_path}")

        for table in TABLES:
            csv_path = db_path / f"{table}.csv"
            if not csv_path.exists():
                continue

            try:
                df = pd.read_csv(csv_path, dtype=str)
            except Exception as e:
                print(f"  [FAIL] Cannot read {csv_path}: {e}")
                continue

            # Find columns (case-insensitive)
            cols = {c.lower(): c for c in df.columns}
            isbn_col = cols.get("isbn")
            title_col = cols.get("title")

            if not title_col:
                print(f"  [WARN] No Title column in {table}")
                continue

            table_id = f"{db_name}/{table}"
            count = 0

            for _, row in df.iterrows():
                stats["total"] += 1
                isbn = str(row.get(isbn_col, "")) if isbn_col else ""
                title = str(row.get(title_col, ""))
                norm_isbn = normalize_isbn(isbn)
                norm_title = normalize_text(title)

                # Case 1: Has valid ISBN
                if is_valid_isbn(norm_isbn):
                    stats["with_isbn"] += 1
                    case1_with_isbn.setdefault(norm_isbn, [])
                    if table_id not in case1_with_isbn[norm_isbn]:
                        case1_with_isbn[norm_isbn].append(table_id)
                    count += 1
                    continue

                stats["without_isbn"] += 1
                if not norm_title:
                    continue

                # Check API cache or make request
                if norm_title in api_cache:
                    api_result = api_cache[norm_title]
                else:
                    print(
                        f"  [API] {title[:50]}..."
                        if len(title) > 50
                        else f"  [API] {title}"
                    )
                    time.sleep(API_DELAY)
                    api_result = fetch_isbn(title)
                    stats["api_calls"] += 1
                    if api_result is None:
                        stats["api_errors"] += 1
                        api_result = []
                    api_cache[norm_title] = api_result

                # Case 2A: No ISBN found
                if not api_result:
                    case2a_no_isbn.setdefault(norm_title, [])
                    if table_id not in case2a_no_isbn[norm_title]:
                        case2a_no_isbn[norm_title].append(table_id)
                    count += 1
                    continue

                isbn10s, isbn13s = classify_isbns(api_result)
                is_single = len(isbn10s) <= 1 and len(isbn13s) <= 1

                if is_single and (isbn10s or isbn13s):
                    # Case 2C: Single ISBN pair
                    isbn10 = isbn10s[0] if isbn10s else ""
                    isbn13 = isbn13s[0] if isbn13s else ""
                    key = f"{isbn10}|{isbn13}"
                    case2c_single.setdefault(key, {})
                    case2c_single[key].setdefault(norm_title, [])
                    if table_id not in case2c_single[key][norm_title]:
                        case2c_single[key][norm_title].append(table_id)
                else:
                    # Case 2B: Multiple ISBNs
                    all_isbns = [normalize_isbn(i) for i in api_result]
                    if norm_title not in case2b_multiple:
                        case2b_multiple[norm_title] = {"tables": [], "isbns": all_isbns}
                    if table_id not in case2b_multiple[norm_title]["tables"]:
                        case2b_multiple[norm_title]["tables"].append(table_id)
                count += 1

            print(f"  [PASS] {table}: {count} entries")

    # Sort case1 by table count
    case1_with_isbn = dict(
        sorted(case1_with_isbn.items(), key=lambda x: len(x[1]), reverse=True)
    )

    # Convert case2c for JSON
    case2c_json = []
    for k, titles in case2c_single.items():
        i10, i13 = k.split("|")
        isbns = [x for x in [i10, i13] if x]
        case2c_json.append({"isbns": isbns, "titles": titles})

    # Save results
    with open(output_dir / "case1_with_isbn.json", "w") as f:
        json.dump(case1_with_isbn, f, indent=2)
    with open(output_dir / "case2a_no_isbn_found.json", "w") as f:
        json.dump(case2a_no_isbn, f, indent=2)
    with open(output_dir / "case2b_multiple_isbns.json", "w") as f:
        json.dump(case2b_multiple, f, indent=2)
    with open(output_dir / "case2c_single_isbn.json", "w") as f:
        json.dump(case2c_json, f, indent=2)
    with open(output_dir / "stats.json", "w") as f:
        json.dump(stats, f, indent=2)

    # Summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Total entries: {stats['total']}")
    print(f"With ISBN: {stats['with_isbn']}")
    print(f"Without ISBN: {stats['without_isbn']}")
    print(f"API calls: {stats['api_calls']} (errors: {stats['api_errors']})")
    print(f"\nCase 1 (Has ISBN): {len(case1_with_isbn)} unique ISBNs")
    print(f"Case 2A (No ISBN via API): {len(case2a_no_isbn)} titles")
    print(f"Case 2B (Multiple ISBNs): {len(case2b_multiple)} titles")
    print(f"Case 2C (Single ISBN pair): {len(case2c_single)} pairs")
    print(f"\n[PASS] Results saved to {output_dir}/")


if __name__ == "__main__":
    main()
