"""
Analyze books without ISBN that have a title.
Shows counts and field availability for different field combinations.
"""

import pandas as pd
from pathlib import Path
from collections import defaultdict

DBS = ['export_books_2004', 'export_booksCollection']
TABLES = ['Books', 'Books1', 'MissingBooks', 'NB']


def is_empty(val):
    """Check if a value is empty/null."""
    if pd.isna(val):
        return True
    return str(val).strip() == ''


def has_field(row, field_names):
    """Check if row has a non-empty value for any of the field names (case-insensitive)."""
    for col in row.index:
        if col.lower() in [f.lower() for f in field_names]:
            if not is_empty(row[col]):
                return True
    return False


def get_field_value(row, field_names):
    """Get value for a field (case-insensitive search)."""
    for col in row.index:
        if col.lower() in [f.lower() for f in field_names]:
            if not is_empty(row[col]):
                return row[col]
    return None


def count_non_empty_fields(row):
    """Count how many fields have non-empty values."""
    return sum(1 for val in row if not is_empty(val))


def analyze():
    # Collect all books without ISBN but with title
    all_no_isbn = []
    
    print("=" * 70)
    print("ANALYZING BOOKS WITHOUT ISBN (but with Title)")
    print("=" * 70)
    
    for db in DBS:
        for table in TABLES:
            path = Path(db) / f'{table}.csv'
            if not path.exists():
                continue
            
            df = pd.read_csv(path, dtype=str)
            
            for idx, row in df.iterrows():
                has_isbn = has_field(row, ['ISBN'])
                has_title = has_field(row, ['Title'])
                
                if not has_isbn and has_title:
                    all_no_isbn.append({
                        'db': db,
                        'table': table,
                        'row': row,
                        'title': get_field_value(row, ['Title']),
                        'publisher': get_field_value(row, ['Publisher']),
                        'shelf': get_field_value(row, ['Shelf']),
                        'field_count': count_non_empty_fields(row)
                    })
    
    print(f"\nTotal books without ISBN (with title): {len(all_no_isbn)}")
    
    if not all_no_isbn:
        print("No books found without ISBN.")
        return
    
    # Field count statistics
    field_counts = [b['field_count'] for b in all_no_isbn]
    print(f"Fields per entry: min={min(field_counts)}, max={max(field_counts)}, avg={sum(field_counts)/len(field_counts):.1f}")
    
    # Breakdown by db/table
    print("\nBreakdown by source:")
    by_source = defaultdict(int)
    for b in all_no_isbn:
        by_source[f"{b['db']}/{b['table']}"] += 1
    for source, count in sorted(by_source.items()):
        print(f"  {source}: {count}")
    
    # Filter categories
    print("\n" + "=" * 70)
    print("FILTERING BY AVAILABLE FIELDS")
    print("=" * 70)
    
    # Title + Publisher
    title_publisher = [b for b in all_no_isbn if b['publisher'] is not None]
    print(f"\n[Title + Publisher]: {len(title_publisher)} books")
    if title_publisher:
        fc = [b['field_count'] for b in title_publisher]
        print(f"  Fields per entry: min={min(fc)}, max={max(fc)}, avg={sum(fc)/len(fc):.1f}")
    
    # Title + Shelf
    title_shelf = [b for b in all_no_isbn if b['shelf'] is not None]
    print(f"\n[Title + Shelf]: {len(title_shelf)} books")
    if title_shelf:
        fc = [b['field_count'] for b in title_shelf]
        print(f"  Fields per entry: min={min(fc)}, max={max(fc)}, avg={sum(fc)/len(fc):.1f}")
    
    # Title + Publisher + Shelf
    title_pub_shelf = [b for b in all_no_isbn if b['publisher'] is not None and b['shelf'] is not None]
    print(f"\n[Title + Publisher + Shelf]: {len(title_pub_shelf)} books")
    if title_pub_shelf:
        fc = [b['field_count'] for b in title_pub_shelf]
        print(f"  Fields per entry: min={min(fc)}, max={max(fc)}, avg={sum(fc)/len(fc):.1f}")
    
    # Summary table
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"{'Category':<30} {'Count':>10} {'% of Total':>12}")
    print("-" * 52)
    print(f"{'All (Title only)':<30} {len(all_no_isbn):>10} {'100.0%':>12}")
    print(f"{'Title + Publisher':<30} {len(title_publisher):>10} {len(title_publisher)/len(all_no_isbn)*100:>11.1f}%")
    print(f"{'Title + Shelf':<30} {len(title_shelf):>10} {len(title_shelf)/len(all_no_isbn)*100:>11.1f}%")
    print(f"{'Title + Publisher + Shelf':<30} {len(title_pub_shelf):>10} {len(title_pub_shelf)/len(all_no_isbn)*100:>11.1f}%")
    
    # Books with ONLY title (no publisher, no shelf)
    title_only = [b for b in all_no_isbn if b['publisher'] is None and b['shelf'] is None]
    print(f"{'Title ONLY (no pub/shelf)':<30} {len(title_only):>10} {len(title_only)/len(all_no_isbn)*100:>11.1f}%")


if __name__ == "__main__":
    analyze()