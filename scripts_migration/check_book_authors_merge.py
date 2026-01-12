"""
Validate the book authors migration output for data integrity.

Usage:
    python -m scripts_migration.check_book_authors_merge

Requires:
  - config.json with paths.db1 and paths.db2
  - migration_output/ directory with merge results
"""

import sys
import json
import pandas as pd
from pathlib import Path


def load_config():
    with open("config.json") as f:
        return json.load(f)


def get_path(key):
    return Path(load_config()["paths"][key])


def get_norm_keys(df):
    f = df.get("FirstName", pd.Series(dtype=str)).fillna("").astype(str).str.strip()
    m = df.get("MiddleName", pd.Series(dtype=str)).fillna("").astype(str).str.strip()
    l = df.get("LastName", pd.Series(dtype=str)).fillna("").astype(str).str.strip()

    combined = (f + " " + m).str.strip().str.replace(r"\s+", " ", regex=True)
    keys = combined.str.lower() + "|" + l.str.lower()
    return set(keys[keys != "|"])


def get_keys_series(df):
    f = df.get("FirstName", pd.Series(dtype=str)).fillna("").astype(str).str.strip()
    m = df.get("MiddleName", pd.Series(dtype=str)).fillna("").astype(str).str.strip()
    l = df.get("LastName", pd.Series(dtype=str)).fillna("").astype(str).str.strip()
    combined = (f + " " + m).str.strip().str.replace(r"\s+", " ", regex=True)
    return combined.str.lower() + "|" + l.str.lower()


def main():
    print("=" * 60)
    print("VALIDATING BOOK AUTHORS MIGRATION")
    print("=" * 60)

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
        print(f"[FAIL] Missing file: {e}")
        sys.exit(1)

    passed, failed = 0, 0

    # Check 1: Author Preservation
    print("\n[Check 1: Author Preservation]")
    s1_keys = get_norm_keys(src1)
    s2_keys = get_norm_keys(src2)
    all_source_keys = s1_keys.union(s2_keys)

    nf = new_authors["first_name"].fillna("").str.strip()
    nl = new_authors["last_name"].fillna("").str.strip()
    new_keys = set((nf.str.lower() + "|" + nl.str.lower()))
    new_keys.discard("|")

    missing = all_source_keys - new_keys
    if len(missing) == 0:
        print(
            f"[PASS] All unique authors preserved (Source: {len(all_source_keys)}, New: {len(new_keys)})"
        )
        passed += 1
    else:
        print(f"[FAIL] Missing {len(missing)} authors: {list(missing)[:5]}")
        failed += 1

    extra = new_keys - all_source_keys
    if extra:
        print(f"[WARN] {len(extra)} unexpected authors in output")

    # Check 2: Mapping Completeness
    print("\n[Check 2: Mapping Completeness]")
    mapped_ids_1 = set(
        mapping[mapping["source_db"] == "db1"]["old_author_id"].astype(str)
    )
    mapped_ids_2 = set(
        mapping[mapping["source_db"] == "db2"]["old_author_id"].astype(str)
    )

    src1_keys = get_keys_series(src1)
    src2_keys = get_keys_series(src2)
    src1_ids = set(src1[src1_keys != "|"]["AuthorID"].dropna().astype(str))
    src2_ids = set(src2[src2_keys != "|"]["AuthorID"].dropna().astype(str))

    if src1_ids.issubset(mapped_ids_1):
        print(f"[PASS] All DB1 AuthorIDs mapped ({len(src1_ids)} IDs)")
        passed += 1
    else:
        print("[FAIL] Missing DB1 AuthorIDs")
        failed += 1

    if src2_ids.issubset(mapped_ids_2):
        print(f"[PASS] All DB2 AuthorIDs mapped ({len(src2_ids)} IDs)")
        passed += 1
    else:
        print("[FAIL] Missing DB2 AuthorIDs")
        failed += 1

    # Check 3: UUID Validity
    print("\n[Check 3: UUID Validity]")
    valid_uuids = set(new_authors["book_author_id"])
    mapped_uuids = set(mapping["new_book_author_id"].dropna())
    mapped_uuids.discard("")
    mapped_uuids.discard("nan")

    invalid = mapped_uuids - valid_uuids
    if len(invalid) == 0:
        print(f"[PASS] All {len(mapped_uuids)} mapped UUIDs exist in BOOK_AUTHORS")
        passed += 1
    else:
        print(f"[FAIL] {len(invalid)} invalid UUIDs: {list(invalid)[:5]}")
        failed += 1

    # Check 4: No Duplicates
    print("\n[Check 4: No Duplicates]")
    if not new_authors["book_author_id"].duplicated().any():
        print("[PASS] No duplicate UUIDs")
        passed += 1
    else:
        print("[FAIL] Duplicate UUIDs found")
        failed += 1

    new_keys_series = nf.str.lower() + "|" + nl.str.lower()
    if not new_keys_series.duplicated().any():
        print("[PASS] No duplicate author names")
        passed += 1
    else:
        print("[FAIL] Duplicate author names found")
        failed += 1

    # Check 5: Enriched Files
    print("\n[Check 5: Enriched Files Have Author IDs]")
    db1_missing = enriched_db1["new_book_author_id"].isna() | (
        enriched_db1["new_book_author_id"] == ""
    )
    db2_missing = enriched_db2["new_book_author_id"].isna() | (
        enriched_db2["new_book_author_id"] == ""
    )

    if db1_missing.sum() == 0:
        print(
            f"[PASS] All enriched_db1 rows have new_book_author_id ({len(enriched_db1)} rows)"
        )
        passed += 1
    else:
        print(f"[FAIL] {db1_missing.sum()} rows missing new_book_author_id")
        failed += 1

    if db2_missing.sum() == 0:
        print(
            f"[PASS] All enriched_db2 rows have new_book_author_id ({len(enriched_db2)} rows)"
        )
        passed += 1
    else:
        print(f"[FAIL] {db2_missing.sum()} rows missing new_book_author_id")
        failed += 1

    # Check 6: Row Count Verification
    print("\n[Check 6: Row Count Sanity]")
    skipped_db1 = int(meta["skipped_incomplete_db1"])
    skipped_db2 = int(meta["skipped_incomplete_db2"])
    orphaned_db1 = int(meta.get("orphaned_db1", 0))
    orphaned_db2 = int(meta.get("orphaned_db2", 0))
    src_db1_count = int(meta["source_db1_book_authors"])
    src_db2_count = int(meta["source_db2_book_authors"])

    expected_db1 = src_db1_count - skipped_db1 - orphaned_db1
    expected_db2 = src_db2_count - skipped_db2 - orphaned_db2

    if len(enriched_db1) == expected_db1:
        print(
            f"[PASS] DB1: {len(enriched_db1)} enriched + {skipped_db1} skipped + {orphaned_db1} orphaned = {src_db1_count}"
        )
        passed += 1
    else:
        print(f"[FAIL] DB1: got {len(enriched_db1)}, expected {expected_db1}")
        failed += 1

    if len(enriched_db2) == expected_db2:
        print(
            f"[PASS] DB2: {len(enriched_db2)} enriched + {skipped_db2} skipped + {orphaned_db2} orphaned = {src_db2_count}"
        )
        passed += 1
    else:
        print(f"[FAIL] DB2: got {len(enriched_db2)}, expected {expected_db2}")
        failed += 1

    if len(new_authors) == len(all_source_keys):
        print(f"[PASS] BOOK_AUTHORS count matches unique authors ({len(new_authors)})")
        passed += 1
    else:
        print("[FAIL] BOOK_AUTHORS count mismatch")
        failed += 1

    # Check 7: Summary
    print("\n[Check 7: Skipped & Orphaned Summary]")
    total_skipped = skipped_db1 + skipped_db2
    total_orphaned = orphaned_db1 + orphaned_db2

    if total_skipped > 0:
        print(
            f"[WARN] {total_skipped} incomplete entries skipped (DB1: {skipped_db1}, DB2: {skipped_db2})"
        )
    else:
        print("[INFO] No incomplete entries skipped")

    if total_orphaned > 0:
        print(
            f"[WARN] {total_orphaned} orphaned entries filtered (DB1: {orphaned_db1}, DB2: {orphaned_db2})"
        )
    else:
        print("[INFO] No orphaned entries found")

    # Final Summary
    print("\n" + "=" * 60)
    print(f"SUMMARY: {passed} passed, {failed} failed")
    print("=" * 60)

    if failed == 0:
        print("[PASS] ALL CHECKS PASSED")
        return True
    else:
        print("[FAIL] VALIDATION FAILED")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
