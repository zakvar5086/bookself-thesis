"""
Validate the papers migration output for data integrity.

Usage:
    python -m scripts_migration.check_papers_merge

Requires:
  - config.json with paths.db1 and paths.db2
  - final_tables/ and metadata_new_tables/PAPERS/ with merge results
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


def normalize_title(series):
    return series.fillna("").astype(str).str.strip().str.lower()


def get_dedup_keys(df):
    titles = normalize_title(df["Title"])
    years = df["Year"].fillna("").astype(str).str.strip()
    keys = titles + "|" + years
    valid = titles != ""
    return set(keys[valid])


def parse_authors_array(val):
    if pd.isna(val) or val == "{}":
        return []
    s = str(val).strip()
    if s.startswith("{") and s.endswith("}"):
        s = s[1:-1]
    if not s:
        return []
    return [x.strip() for x in s.split(",") if x.strip()]


def main():
    print("=" * 60)
    print("VALIDATING PAPERS MIGRATION")
    print("=" * 60)

    final_dir = get_path("final_tables")
    meta_dir = get_path("metadata") / "PAPERS"

    try:
        new_papers = pd.read_csv(final_dir / "PAPERS.csv", dtype=str)
        mapping = pd.read_csv(meta_dir / "paper_id_mapping.csv", dtype=str)
        meta = pd.read_csv(meta_dir / "papers_migration_metadata.csv").iloc[0]
        pauthors = pd.read_csv(final_dir / "PAUTHORS.csv", dtype=str)

        src1 = pd.read_csv(get_path("db1") / "Papers.csv", dtype=str)
        src2 = pd.read_csv(get_path("db2") / "Papers.csv", dtype=str)
    except FileNotFoundError as e:
        print(f"[FAIL] Missing file: {e}")
        sys.exit(1)

    passed, failed = 0, 0

    # Check 1: Paper Preservation
    print("\n[Check 1: Paper Preservation]")
    s1_keys = get_dedup_keys(src1)
    s2_keys = get_dedup_keys(src2)
    all_source_keys = s1_keys.union(s2_keys)

    new_titles = normalize_title(new_papers["title"])
    new_years = new_papers["year"].fillna("").astype(str).str.strip()
    new_keys = set(new_titles + "|" + new_years)
    new_keys.discard("|")

    missing = all_source_keys - new_keys
    if len(missing) == 0:
        print(
            f"[PASS] All unique papers preserved (Source: {len(all_source_keys)}, New: {len(new_keys)})"
        )
        passed += 1
    else:
        print(f"[FAIL] Missing {len(missing)} papers: {list(missing)[:5]}")
        failed += 1

    extra = new_keys - all_source_keys
    if extra:
        print(f"[WARN] {len(extra)} unexpected papers in output")

    # Check 2: Mapping Completeness
    print("\n[Check 2: Mapping Completeness]")
    mapped_ids_1 = set(
        mapping[mapping["source_db"] == "db1"]["old_paper_id"].astype(str)
    )
    mapped_ids_2 = set(
        mapping[mapping["source_db"] == "db2"]["old_paper_id"].astype(str)
    )

    # Filter source IDs with valid titles
    src1_valid = src1[normalize_title(src1["Title"]) != ""]
    src2_valid = src2[normalize_title(src2["Title"]) != ""]
    src1_ids = set(src1_valid["PaperID"].dropna().astype(str))
    src2_ids = set(src2_valid["PaperID"].dropna().astype(str))

    if src1_ids.issubset(mapped_ids_1):
        print(f"[PASS] All valid DB1 PaperIDs mapped ({len(src1_ids)} IDs)")
        passed += 1
    else:
        missing_ids = src1_ids - mapped_ids_1
        print(f"[FAIL] Missing {len(missing_ids)} DB1 PaperIDs")
        failed += 1

    if src2_ids.issubset(mapped_ids_2):
        print(f"[PASS] All valid DB2 PaperIDs mapped ({len(src2_ids)} IDs)")
        passed += 1
    else:
        missing_ids = src2_ids - mapped_ids_2
        print(f"[FAIL] Missing {len(missing_ids)} DB2 PaperIDs")
        failed += 1

    # Check 3: UUID Validity
    print("\n[Check 3: UUID Validity]")
    valid_uuids = set(new_papers["paper_id"])
    mapped_uuids = set(mapping["new_paper_id"].dropna())
    mapped_uuids.discard("")
    mapped_uuids.discard("nan")

    invalid = mapped_uuids - valid_uuids
    if len(invalid) == 0:
        print(f"[PASS] All {len(mapped_uuids)} mapped UUIDs exist in PAPERS")
        passed += 1
    else:
        print(f"[FAIL] {len(invalid)} invalid UUIDs: {list(invalid)[:5]}")
        failed += 1

    # Check 4: No Duplicates
    print("\n[Check 4: No Duplicates]")
    if not new_papers["paper_id"].duplicated().any():
        print("[PASS] No duplicate UUIDs")
        passed += 1
    else:
        print("[FAIL] Duplicate UUIDs found")
        failed += 1

    title_year_keys = new_titles + "|" + new_years
    if not title_year_keys.duplicated().any():
        print("[PASS] No duplicate title+year combinations")
        passed += 1
    else:
        print("[FAIL] Duplicate title+year combinations found")
        failed += 1

    # Check 5: Authors IDs Validity
    print("\n[Check 5: Authors IDs Reference Valid PAuthors]")
    valid_author_uuids = set(pauthors["author_id"])

    all_referenced_authors = set()
    invalid_author_refs = []
    for idx, row in new_papers.iterrows():
        authors = parse_authors_array(row["authors_ids"])
        for author_id in authors:
            all_referenced_authors.add(author_id)
            if author_id not in valid_author_uuids:
                invalid_author_refs.append((row["paper_id"], author_id))

    if len(invalid_author_refs) == 0:
        print(
            f"[PASS] All {len(all_referenced_authors)} referenced author IDs exist in PAUTHORS"
        )
        passed += 1
    else:
        print(
            f"[FAIL] {len(invalid_author_refs)} invalid author references: {invalid_author_refs[:5]}"
        )
        failed += 1

    # Check 6: Boolean Fields
    print("\n[Check 6: Boolean Fields Valid]")
    bool_cols = [
        "soft_copy",
        "hard_copy",
        "full_paper",
        "accepted",
        "cond_accepted",
        "submitted",
        "und_submission",
        "in_press",
    ]

    bool_valid = True
    for col in bool_cols:
        if col in new_papers.columns:
            valid_values = new_papers[col].isin(
                ["True", "False", "true", "false", True, False, "0", "1"]
            )
            if not valid_values.all():
                print(f"[FAIL] Invalid values in {col}")
                bool_valid = False

    if bool_valid:
        print("[PASS] All boolean fields contain valid values")
        passed += 1
    else:
        failed += 1

    # Check 7: Row Count Sanity
    print("\n[Check 7: Row Count Sanity]")
    skipped_id_db1 = int(meta.get("skipped_invalid_id_db1", 0))
    skipped_id_db2 = int(meta.get("skipped_invalid_id_db2", 0))
    skipped_title_db1 = int(meta.get("skipped_empty_title_db1", 0))
    skipped_title_db2 = int(meta.get("skipped_empty_title_db2", 0))
    duplicates = int(meta.get("duplicates_merged", 0))
    src_db1_count = int(meta["source_db1_papers"])
    src_db2_count = int(meta["source_db2_papers"])

    valid_db1 = src_db1_count - skipped_id_db1 - skipped_title_db1
    valid_db2 = src_db2_count - skipped_id_db2 - skipped_title_db2
    expected_unique = valid_db1 + valid_db2 - duplicates

    if len(new_papers) == expected_unique:
        print(
            f"[PASS] PAPERS count: {valid_db1} + {valid_db2} - {duplicates} duplicates = {len(new_papers)}"
        )
        passed += 1
    else:
        print(f"[FAIL] Expected {expected_unique}, got {len(new_papers)}")
        failed += 1

    # Check 8: Required Fields
    print("\n[Check 8: Required Fields Present]")
    required_cols = ["paper_id", "title"]
    missing_cols = [col for col in required_cols if col not in new_papers.columns]

    if not missing_cols:
        print("[PASS] All required columns present")
        passed += 1
    else:
        print(f"[FAIL] Missing columns: {missing_cols}")
        failed += 1

    # Check for empty paper_ids
    empty_ids = new_papers["paper_id"].isna() | (new_papers["paper_id"] == "")
    if empty_ids.sum() == 0:
        print("[PASS] No empty paper_ids")
        passed += 1
    else:
        print(f"[FAIL] {empty_ids.sum()} papers with empty paper_id")
        failed += 1

    # Check 9: Skipped Summary
    print("\n[Check 9: Skipped Entry Summary]")
    total_skipped_id = skipped_id_db1 + skipped_id_db2
    total_skipped_title = skipped_title_db1 + skipped_title_db2

    if total_skipped_id > 0:
        print(
            f"[WARN] {total_skipped_id} entries skipped (invalid ID): DB1={skipped_id_db1}, DB2={skipped_id_db2}"
        )
    else:
        print("[INFO] No entries skipped for invalid ID")

    if total_skipped_title > 0:
        print(
            f"[WARN] {total_skipped_title} entries skipped (empty title): DB1={skipped_title_db1}, DB2={skipped_title_db2}"
        )
    else:
        print("[INFO] No entries skipped for empty title")

    if duplicates > 0:
        print(f"[INFO] {duplicates} duplicate papers merged")
    else:
        print("[INFO] No duplicates found")

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
