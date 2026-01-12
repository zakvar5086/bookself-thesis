"""
Analyze books without ISBN that have a title - shows field availability stats

Usage:
    python -m scripts.no_isbn_analyze
"""

import pandas as pd
from pathlib import Path
from collections import defaultdict

DBS = ["export_books_2004", "export_booksCollection"]
TABLES = ["Books", "Books1", "MissingBooks", "NB"]


def is_empty(val):
    if pd.isna(val):
        return True
    return str(val).strip() == ""


def has_field(row, names):
    for col in row.index:
        if col.lower() in [n.lower() for n in names]:
            if not is_empty(row[col]):
                return True
    return False


def get_field(row, names):
    for col in row.index:
        if col.lower() in [n.lower() for n in names]:
            if not is_empty(row[col]):
                return row[col]
    return None


def main():
    print("=" * 70)
    print("ANALYZING BOOKS WITHOUT ISBN (but with Title)")
    print("=" * 70)

    all_no_isbn = []

    for db in DBS:
        for table in TABLES:
            path = Path(db) / f"{table}.csv"
            if not path.exists():
                continue

            try:
                df = pd.read_csv(path, dtype=str)
            except Exception as e:
                print(f"[FAIL] Cannot read {path}: {e}")
                continue

            for _, row in df.iterrows():
                if not has_field(row, ["ISBN"]) and has_field(row, ["Title"]):
                    all_no_isbn.append(
                        {
                            "db": db,
                            "table": table,
                            "title": get_field(row, ["Title"]),
                            "publisher": get_field(row, ["Publisher"]),
                            "shelf": get_field(row, ["Shelf"]),
                            "field_count": sum(1 for v in row if not is_empty(v)),
                        }
                    )

    total = len(all_no_isbn)
    print(f"\nTotal books without ISBN (with title): {total}")

    if total == 0:
        print("[WARN] No books found without ISBN")
        return

    # Field count stats
    fc = [b["field_count"] for b in all_no_isbn]
    print(
        f"Fields per entry: min={min(fc)}, max={max(fc)}, avg={sum(fc) / len(fc):.1f}"
    )

    # Breakdown by source
    print("\nBreakdown by source:")
    by_source = defaultdict(int)
    for b in all_no_isbn:
        by_source[f"{b['db']}/{b['table']}"] += 1
    for src, cnt in sorted(by_source.items()):
        print(f"  {src}: {cnt}")

    # Filter categories
    print("\n" + "=" * 70)
    print("FILTERING BY AVAILABLE FIELDS")
    print("=" * 70)

    title_pub = [b for b in all_no_isbn if b["publisher"]]
    title_shelf = [b for b in all_no_isbn if b["shelf"]]
    title_pub_shelf = [b for b in all_no_isbn if b["publisher"] and b["shelf"]]
    title_only = [b for b in all_no_isbn if not b["publisher"] and not b["shelf"]]

    def show_cat(name, items):
        print(f"\n[{name}]: {len(items)} books")
        if items:
            fc = [b["field_count"] for b in items]
            print(
                f"  Fields per entry: min={min(fc)}, max={max(fc)}, avg={sum(fc) / len(fc):.1f}"
            )

    show_cat("Title + Publisher", title_pub)
    show_cat("Title + Shelf", title_shelf)
    show_cat("Title + Publisher + Shelf", title_pub_shelf)

    # Summary table
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"{'Category':<30} {'Count':>10} {'% of Total':>12}")
    print("-" * 52)
    print(f"{'All (Title only)':<30} {total:>10} {'100.0%':>12}")
    print(
        f"{'Title + Publisher':<30} {len(title_pub):>10} {len(title_pub) / total * 100:>11.1f}%"
    )
    print(
        f"{'Title + Shelf':<30} {len(title_shelf):>10} {len(title_shelf) / total * 100:>11.1f}%"
    )
    print(
        f"{'Title + Publisher + Shelf':<30} {len(title_pub_shelf):>10} {len(title_pub_shelf) / total * 100:>11.1f}%"
    )
    print(
        f"{'Title ONLY (no pub/shelf)':<30} {len(title_only):>10} {len(title_only) / total * 100:>11.1f}%"
    )

    print("\n[PASS] Analysis complete")


if __name__ == "__main__":
    main()
