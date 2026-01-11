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

def main():
    print("--- Validating Book Topic Migration ---")
    out_dir = Path("migration_output")
    
    # Load Artifacts
    try:
        new_topics = pd.read_csv(out_dir / "BOOK_TOPIC.csv", dtype=str)
        mapping = pd.read_csv(out_dir / "topic_id_mapping.csv", dtype=str)
        enriched_db1 = pd.read_csv(out_dir / "book_topic_enriched_db1.csv", dtype=str)
        enriched_db2 = pd.read_csv(out_dir / "book_topic_enriched_db2.csv", dtype=str)
        meta = pd.read_csv(out_dir / "book_topic_migration_metadata.csv").iloc[0]
        
        # Load Raw Sources for verification
        src1 = pd.read_csv(get_path("db1") / "Topic.csv", dtype=str)
        src2 = pd.read_csv(get_path("db2") / "Topic.csv", dtype=str)
    except FileNotFoundError as e:
        print(f"Critical Error: Missing file {e}")
        sys.exit(1)

    passed = 0
    failed = 0

    # 1. Check Topic Preservation
    print("\n[Check 1: Topic Preservation]")
    s1_topics = set(src1['Topic'].dropna().str.strip().str.lower())
    s2_topics = set(src2['Topic'].dropna().str.strip().str.lower())
    all_source_topics = s1_topics.union(s2_topics)
    new_topics_set = set(new_topics['topic_name'].dropna().str.strip().str.lower())
    
    missing = all_source_topics - new_topics_set
    if check(len(missing) == 0, f"All unique source topics preserved. (Source: {len(all_source_topics)}, New: {len(new_topics_set)})"):
        passed += 1
    else:
        failed += 1
        print(f"   Missing: {list(missing)[:5]}...")
    
    extra = new_topics_set - all_source_topics
    if extra:
        warn(f"{len(extra)} unexpected topics in output")

    # 2. Check Mapping Completeness
    print("\n[Check 2: Mapping Completeness]")
    mapped_ids_1 = set(mapping[mapping['source_db'] == 'db1']['old_topic_id'].astype(str))
    mapped_ids_2 = set(mapping[mapping['source_db'] == 'db2']['old_topic_id'].astype(str))
    
    src1_ids = set(src1['TopicID'].astype(str))
    src2_ids = set(src2['TopicID'].astype(str))
    
    if check(src1_ids.issubset(mapped_ids_1), f"All DB1 TopicIDs mapped ({len(src1_ids)} IDs)"):
        passed += 1
    else:
        failed += 1
    
    if check(src2_ids.issubset(mapped_ids_2), f"All DB2 TopicIDs mapped ({len(src2_ids)} IDs)"):
        passed += 1
    else:
        failed += 1

    # 3. Check UUID Validity
    print("\n[Check 3: UUID Validity]")
    valid_uuids = set(new_topics['book_topic_id'])
    mapped_uuids = set(mapping['new_book_topic_id'].dropna())
    mapped_uuids.discard('')
    mapped_uuids.discard('nan')
    
    invalid = mapped_uuids - valid_uuids
    if check(len(invalid) == 0, f"All {len(mapped_uuids)} mapped UUIDs exist in BOOK_TOPIC table"):
        passed += 1
    else:
        failed += 1
        print(f"   Invalid: {list(invalid)[:5]}...")

    # 4. Duplicate Checks
    print("\n[Check 4: No Duplicates]")
    dupe_uuid = new_topics['book_topic_id'].duplicated().any()
    if check(not dupe_uuid, "No duplicate UUIDs in new table"):
        passed += 1
    else:
        failed += 1
    
    dupe_names = new_topics['topic_name'].str.strip().str.lower().duplicated().any()
    if check(not dupe_names, "No duplicate topic names in new table"):
        passed += 1
    else:
        failed += 1

    # 5. Enriched Files Completeness
    print("\n[Check 5: Enriched Files Have TopicNames]")
    db1_missing = enriched_db1['TopicName'].isna() | (enriched_db1['TopicName'] == '')
    db2_missing = enriched_db2['TopicName'].isna() | (enriched_db2['TopicName'] == '')
    
    if check(db1_missing.sum() == 0, f"All enriched_db1 rows have TopicNames ({len(enriched_db1)} rows)"):
        passed += 1
    else:
        failed += 1
        print(f"   {db1_missing.sum()} rows missing TopicName")
    
    if check(db2_missing.sum() == 0, f"All enriched_db2 rows have TopicNames ({len(enriched_db2)} rows)"):
        passed += 1
    else:
        failed += 1
        print(f"   {db2_missing.sum()} rows missing TopicName")

    # 6. Row Count Verification
    print("\n[Check 6: Row Count Sanity]")
    skipped_db1 = int(meta['skipped_incomplete_db1'])
    skipped_db2 = int(meta['skipped_incomplete_db2'])
    src_db1_count = int(meta['source_db1_book_topics'])
    src_db2_count = int(meta['source_db2_book_topics'])
    
    # Expected: enriched rows + skipped = source total
    expected_db1 = src_db1_count - skipped_db1
    expected_db2 = src_db2_count - skipped_db2
    
    if check(len(enriched_db1) == expected_db1, f"DB1 row count: {len(enriched_db1)} enriched + {skipped_db1} skipped = {src_db1_count} source"):
        passed += 1
    else:
        failed += 1
        print(f"   Got {len(enriched_db1)}, expected {expected_db1}")
    
    if check(len(enriched_db2) == expected_db2, f"DB2 row count: {len(enriched_db2)} enriched + {skipped_db2} skipped = {src_db2_count} source"):
        passed += 1
    else:
        failed += 1
        print(f"   Got {len(enriched_db2)}, expected {expected_db2}")
    
    # Topic table count check
    if check(len(new_topics) == len(all_source_topics), f"BOOK_TOPIC count matches unique topics ({len(new_topics)})"):
        passed += 1
    else:
        failed += 1

    # 7. Skipped Entries
    print("\n[Check 7: Skipped Entry Summary]")
    total_skipped = skipped_db1 + skipped_db2
    if total_skipped > 0:
        warn(f"{total_skipped} incomplete entries skipped (DB1: {skipped_db1}, DB2: {skipped_db2})")
    else:
        print(f"[INFO] No incomplete entries were skipped")

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