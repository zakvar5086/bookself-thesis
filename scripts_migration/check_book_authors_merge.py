import sys
import pandas as pd
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from database_utils.config import get_path

def check(condition, msg):
    if condition:
        print(f"[PASS] {msg}")
    else:
        print(f"[FAIL] {msg}")
        return False
    return True

def warn(msg):
    print(f"[WARN] {msg}")

def get_norm_keys(df):
    """Re-creates the normalization logic for verification."""
    f = df.get('FirstName', pd.Series(dtype=str)).fillna('').astype(str).str.strip()
    m = df.get('MiddleName', pd.Series(dtype=str)).fillna('').astype(str).str.strip()
    l = df.get('LastName', pd.Series(dtype=str)).fillna('').astype(str).str.strip()
    
    combined = (f + ' ' + m).str.strip().str.replace(r'\s+', ' ', regex=True)
    keys = (combined.str.lower() + "|" + l.str.lower())
    return set(keys[keys != "|"])

def main():
    print("--- Validating Book Authors Migration ---")
    out_dir = Path("migration_output")
    
    try:
        new_authors = pd.read_csv(out_dir / "BOOK_AUTHORS.csv", dtype=str)
        mapping = pd.read_csv(out_dir / "author_id_mapping.csv", dtype=str)
        enriched_db1 = pd.read_csv(out_dir / "book_authors_enriched_db1.csv", dtype=str)
        enriched_db2 = pd.read_csv(out_dir / "book_authors_enriched_db2.csv", dtype=str)
        meta = pd.read_csv(out_dir / "book_authors_migration_metadata.csv").iloc[0]
        
        src1 = pd.read_csv(get_path("db1") / "Authors.csv", dtype=str)
        src2 = pd.read_csv(get_path("db2") / "Authors.csv", dtype=str)
    except FileNotFoundError as e:
        print(f"Critical Error: Missing file {e}")
        sys.exit(1)

    passed = 0
    failed = 0

    # 1. Check Data Preservation
    print("\n[Check 1: Author Preservation]")
    s1_keys = get_norm_keys(src1)
    s2_keys = get_norm_keys(src2)
    all_source_keys = s1_keys.union(s2_keys)
    
    nf = new_authors['first_name'].fillna('').str.strip()
    nl = new_authors['last_name'].fillna('').str.strip()
    new_keys = set((nf.str.lower() + "|" + nl.str.lower()))
    new_keys.discard("|")
    
    missing = all_source_keys - new_keys
    if check(len(missing) == 0, f"All unique authors preserved. (Source: {len(all_source_keys)}, New: {len(new_keys)})"):
        passed += 1
    else:
        failed += 1
        print(f"   Missing keys sample: {list(missing)[:5]}")
    
    extra = new_keys - all_source_keys
    if extra:
        warn(f"{len(extra)} unexpected authors in output")

    # 2. Check ID Mapping
    print("\n[Check 2: Mapping Completeness]")
    mapped_ids_1 = set(mapping[mapping['source_db'] == 'db1']['old_author_id'].astype(str))
    mapped_ids_2 = set(mapping[mapping['source_db'] == 'db2']['old_author_id'].astype(str))
    
    # Only expect AuthorIDs that have non-empty names (matching merge logic)
    # Filter out authors with empty keys (key == "|")
    def get_keys_series(df):
        f = df.get('FirstName', pd.Series(dtype=str)).fillna('').astype(str).str.strip()
        m = df.get('MiddleName', pd.Series(dtype=str)).fillna('').astype(str).str.strip()
        l = df.get('LastName', pd.Series(dtype=str)).fillna('').astype(str).str.strip()
        combined = (f + ' ' + m).str.strip().str.replace(r'\s+', ' ', regex=True)
        return (combined.str.lower() + "|" + l.str.lower())
    
    src1_keys = get_keys_series(src1)
    src2_keys = get_keys_series(src2)
    src1_ids = set(src1[src1_keys != '|']['AuthorID'].dropna().astype(str))
    src2_ids = set(src2[src2_keys != '|']['AuthorID'].dropna().astype(str))
    
    if check(src1_ids.issubset(mapped_ids_1), f"All DB1 AuthorIDs mapped ({len(src1_ids)} IDs)"):
        passed += 1
    else:
        failed += 1
    
    if check(src2_ids.issubset(mapped_ids_2), f"All DB2 AuthorIDs mapped ({len(src2_ids)} IDs)"):
        passed += 1
    else:
        failed += 1

    # 3. Check UUID Integrity
    print("\n[Check 3: UUID Validity]")
    valid_uuids = set(new_authors['book_author_id'])
    mapped_uuids = set(mapping['new_book_author_id'].dropna())
    mapped_uuids.discard('')
    mapped_uuids.discard('nan')
    
    invalid = mapped_uuids - valid_uuids
    if check(len(invalid) == 0, f"All {len(mapped_uuids)} mapped UUIDs exist in BOOK_AUTHORS table"):
        passed += 1
    else:
        failed += 1
        print(f"   Invalid UUIDs sample: {list(invalid)[:5]}")

    # 4. Duplicate Checks
    print("\n[Check 4: No Duplicates]")
    dupe_uuid = new_authors['book_author_id'].duplicated().any()
    if check(not dupe_uuid, "No duplicate UUIDs in new table"):
        passed += 1
    else:
        failed += 1
    
    new_keys_series = (nf.str.lower() + "|" + nl.str.lower())
    dupe_names = new_keys_series.duplicated().any()
    if check(not dupe_names, "No duplicate author names in new table"):
        passed += 1
    else:
        failed += 1

    # 5. Enriched Files Have Author Names
    print("\n[Check 5: Enriched Files Have Author Names]")
    db1_missing = enriched_db1['new_book_author_id'].isna() | (enriched_db1['new_book_author_id'] == '')
    db2_missing = enriched_db2['new_book_author_id'].isna() | (enriched_db2['new_book_author_id'] == '')
    
    if check(db1_missing.sum() == 0, f"All enriched_db1 rows have new_book_author_id ({len(enriched_db1)} rows)"):
        passed += 1
    else:
        failed += 1
        print(f"   {db1_missing.sum()} rows missing new_book_author_id")
    
    if check(db2_missing.sum() == 0, f"All enriched_db2 rows have new_book_author_id ({len(enriched_db2)} rows)"):
        passed += 1
    else:
        failed += 1
        print(f"   {db2_missing.sum()} rows missing new_book_author_id")

    # 6. Row Count Verification
    print("\n[Check 6: Row Count Sanity]")
    skipped_db1 = int(meta['skipped_incomplete_db1'])
    skipped_db2 = int(meta['skipped_incomplete_db2'])
    orphaned_db1 = int(meta.get('orphaned_db1', 0))
    orphaned_db2 = int(meta.get('orphaned_db2', 0))
    src_db1_count = int(meta['source_db1_book_authors'])
    src_db2_count = int(meta['source_db2_book_authors'])
    
    # Expected: source - skipped - orphaned = enriched
    expected_db1 = src_db1_count - skipped_db1 - orphaned_db1
    expected_db2 = src_db2_count - skipped_db2 - orphaned_db2
    
    if check(len(enriched_db1) == expected_db1, f"DB1 row count: {len(enriched_db1)} enriched + {skipped_db1} skipped + {orphaned_db1} orphaned = {src_db1_count} source"):
        passed += 1
    else:
        failed += 1
        print(f"   Got {len(enriched_db1)}, expected {expected_db1}")
    
    if check(len(enriched_db2) == expected_db2, f"DB2 row count: {len(enriched_db2)} enriched + {skipped_db2} skipped + {orphaned_db2} orphaned = {src_db2_count} source"):
        passed += 1
    else:
        failed += 1
        print(f"   Got {len(enriched_db2)}, expected {expected_db2}")
    
    if check(len(new_authors) == len(all_source_keys), f"BOOK_AUTHORS count matches unique authors ({len(new_authors)})"):
        passed += 1
    else:
        failed += 1

    # 7. Skipped/Orphaned Entries
    print("\n[Check 7: Skipped & Orphaned Entry Summary]")
    total_skipped = skipped_db1 + skipped_db2
    total_orphaned = orphaned_db1 + orphaned_db2
    
    if total_skipped > 0:
        warn(f"{total_skipped} incomplete entries skipped (DB1: {skipped_db1}, DB2: {skipped_db2})")
    else:
        print(f"[INFO] No incomplete entries were skipped")
    
    if total_orphaned > 0:
        warn(f"{total_orphaned} orphaned entries filtered (DB1: {orphaned_db1}, DB2: {orphaned_db2}) - AuthorIDs not in Authors table")
    else:
        print(f"[INFO] No orphaned entries found")

    # Summary
    print("\n--- Validation Summary ---")
    print(f"Passed: {passed}")
    print(f"Failed: {failed}")
    
    if failed == 0:
        print("\nALL CHECKS PASSED - No data loss detected!")
        return True
    else:
        print("\nVALIDATION FAILED - Review errors above")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)