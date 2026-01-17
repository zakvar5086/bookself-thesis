"""
Validate the book topic migration output for data integrity.

Usage:
    python -m scripts_migration.check_book_topic_merge

Requires:
  - config.json with paths.db1 and paths.db2
  - final_tables/ and metadata_new_tables/BOOK_TOPIC/ with merge results
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


def main():
    print("=" * 60)
    print("VALIDATING BOOK TOPIC MIGRATION")
    print("=" * 60)

    final_dir = get_path("final_tables")
    meta_dir = get_path("metadata") / "BOOK_TOPIC"

    try:
        new_topics = pd.read_csv(final_dir / "BOOK_TOPIC.csv", dtype=str)
        mapping = pd.read_csv(meta_dir / "topic_id_mapping.csv", dtype=str)
        enriched_db1 = pd.read_csv(meta_dir / "book_topic_enriched_db1.csv", dtype=str)
        enriched_db2 = pd.read_csv(meta_dir / "book_topic_enriched_db2.csv", dtype=str)
        meta = pd.read_csv(meta_dir / "book_topic_migration_metadata.csv").iloc[0]

        src1 = pd.read_csv(get_path("db1") / "Topic.csv", dtype=str)
        src2 = pd.read_csv(get_path("db2") / "Topic.csv", dtype=str)
    except FileNotFoundError as e:
        print(f"[FAIL] Missing file: {e}")
        sys.exit(1)

    passed, failed = 0, 0

    # Check 1: Topic Preservation
    print("\n[Check 1: Topic Preservation]")
    s1_topics = set(src1["Topic"].dropna().str.strip().str.lower())
    s2_topics = set(src2["Topic"].dropna().str.strip().str.lower())
    all_source_topics = s1_topics.union(s2_topics)
    new_topics_set = set(new_topics["topic_name"].dropna().str.strip().str.lower())

    missing = all_source_topics - new_topics_set
    if len(missing) == 0:
        print(
            f"[PASS] All unique topics preserved (Source: {len(all_source_topics)}, New: {len(new_topics_set)})"
        )
        passed += 1
    else:
        print(f"[FAIL] Missing {len(missing)} topics: {list(missing)[:5]}")
        failed += 1

    extra = new_topics_set - all_source_topics
    if extra:
        print(f"[WARN] {len(extra)} unexpected topics in output")

    # Check 2: Mapping Completeness
    print("\n[Check 2: Mapping Completeness]")
    mapped_ids_1 = set(
        mapping[mapping["source_db"] == "db1"]["old_topic_id"].astype(str)
    )
    mapped_ids_2 = set(
        mapping[mapping["source_db"] == "db2"]["old_topic_id"].astype(str)
    )

    src1_ids = set(src1["TopicID"].astype(str))
    src2_ids = set(src2["TopicID"].astype(str))

    if src1_ids.issubset(mapped_ids_1):
        print(f"[PASS] All DB1 TopicIDs mapped ({len(src1_ids)} IDs)")
        passed += 1
    else:
        print("[FAIL] Missing DB1 TopicIDs")
        failed += 1

    if src2_ids.issubset(mapped_ids_2):
        print(f"[PASS] All DB2 TopicIDs mapped ({len(src2_ids)} IDs)")
        passed += 1
    else:
        print("[FAIL] Missing DB2 TopicIDs")
        failed += 1

    # Check 3: UUID Validity
    print("\n[Check 3: UUID Validity]")
    valid_uuids = set(new_topics["book_topic_id"])
    mapped_uuids = set(mapping["new_book_topic_id"].dropna())
    mapped_uuids.discard("")
    mapped_uuids.discard("nan")

    invalid = mapped_uuids - valid_uuids
    if len(invalid) == 0:
        print(f"[PASS] All {len(mapped_uuids)} mapped UUIDs exist in BOOK_TOPIC")
        passed += 1
    else:
        print(f"[FAIL] {len(invalid)} invalid UUIDs: {list(invalid)[:5]}")
        failed += 1

    # Check 4: No Duplicates
    print("\n[Check 4: No Duplicates]")
    if not new_topics["book_topic_id"].duplicated().any():
        print("[PASS] No duplicate UUIDs")
        passed += 1
    else:
        print("[FAIL] Duplicate UUIDs found")
        failed += 1

    if not new_topics["topic_name"].str.strip().str.lower().duplicated().any():
        print("[PASS] No duplicate topic names")
        passed += 1
    else:
        print("[FAIL] Duplicate topic names found")
        failed += 1

    # Check 5: Enriched Files
    print("\n[Check 5: Enriched Files Have TopicNames]")
    db1_missing = enriched_db1["TopicName"].isna() | (enriched_db1["TopicName"] == "")
    db2_missing = enriched_db2["TopicName"].isna() | (enriched_db2["TopicName"] == "")

    if db1_missing.sum() == 0:
        print(
            f"[PASS] All enriched_db1 rows have TopicNames ({len(enriched_db1)} rows)"
        )
        passed += 1
    else:
        print(f"[FAIL] {db1_missing.sum()} rows missing TopicName")
        failed += 1

    if db2_missing.sum() == 0:
        print(
            f"[PASS] All enriched_db2 rows have TopicNames ({len(enriched_db2)} rows)"
        )
        passed += 1
    else:
        print(f"[FAIL] {db2_missing.sum()} rows missing TopicName")
        failed += 1

    # Check 6: Row Count Verification
    print("\n[Check 6: Row Count Sanity]")
    skipped_db1 = int(meta["skipped_incomplete_db1"])
    skipped_db2 = int(meta["skipped_incomplete_db2"])
    src_db1_count = int(meta["source_db1_book_topics"])
    src_db2_count = int(meta["source_db2_book_topics"])

    expected_db1 = src_db1_count - skipped_db1
    expected_db2 = src_db2_count - skipped_db2

    if len(enriched_db1) == expected_db1:
        print(
            f"[PASS] DB1: {len(enriched_db1)} enriched + {skipped_db1} skipped = {src_db1_count}"
        )
        passed += 1
    else:
        print(f"[FAIL] DB1: got {len(enriched_db1)}, expected {expected_db1}")
        failed += 1

    if len(enriched_db2) == expected_db2:
        print(
            f"[PASS] DB2: {len(enriched_db2)} enriched + {skipped_db2} skipped = {src_db2_count}"
        )
        passed += 1
    else:
        print(f"[FAIL] DB2: got {len(enriched_db2)}, expected {expected_db2}")
        failed += 1

    if len(new_topics) == len(all_source_topics):
        print(f"[PASS] BOOK_TOPIC count matches unique topics ({len(new_topics)})")
        passed += 1
    else:
        print("[FAIL] BOOK_TOPIC count mismatch")
        failed += 1

    # Check 7: Skipped Summary
    print("\n[Check 7: Skipped Entry Summary]")
    total_skipped = skipped_db1 + skipped_db2
    if total_skipped > 0:
        print(
            f"[WARN] {total_skipped} incomplete entries skipped (DB1: {skipped_db1}, DB2: {skipped_db2})"
        )
    else:
        print("[INFO] No incomplete entries skipped")

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
