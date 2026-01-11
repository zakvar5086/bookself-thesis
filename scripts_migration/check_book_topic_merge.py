"""
Validation Script for Book Topic Migration

This script verifies that no data was lost during the topic migration.

Checks performed:
1. All unique topics from both source DBs exist in new BOOK_TOPIC table
2. All old TopicIDs have mappings to new UUIDs
3. All mappings point to valid UUIDs in BOOK_TOPIC
4. Enriched BookTopic files have no unmapped topics
5. Row count sanity checks
"""

import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent))

from database_utils.config import get_path
from database_utils.reporting import (
    print_section,
    print_subsection,
    print_success,
    print_warning,
    print_error,
)


class ValidationResult:
    """Tracks validation results."""
    
    def __init__(self):
        self.checks_passed = 0
        self.checks_failed = 0
        self.warnings = 0
        self.errors = []
    
    def passed(self, message: str):
        self.checks_passed += 1
        print_success(message)
    
    def failed(self, message: str, details: list = None):
        self.checks_failed += 1
        print_error(message)
        self.errors.append(message)
        if details:
            for d in details[:10]:  # Show first 10
                print(f"      - {d}")
            if len(details) > 10:
                print(f"      ... and {len(details) - 10} more")
    
    def warn(self, message: str):
        self.warnings += 1
        print_warning(message)
    
    def summary(self) -> bool:
        """Print summary and return True if all checks passed."""
        print_section("VALIDATION SUMMARY")
        print(f"   Checks passed: {self.checks_passed}")
        print(f"   Checks failed: {self.checks_failed}")
        print(f"   Warnings: {self.warnings}")
        
        if self.checks_failed == 0:
            print("\n   ALL VALIDATION CHECKS PASSED - No data loss detected!")
            return True
        else:
            print("\n   VALIDATION FAILED - Data loss detected!")
            print("   Failed checks:")
            for err in self.errors:
                print(f"      - {err}")
            return False


def normalize_topic(name: str) -> str:
    """Normalize topic name for comparison."""
    if pd.isna(name) or name is None:
        return ""
    return str(name).strip().lower()


def load_source_topics(db_path: Path) -> set:
    """Load and return set of normalized topic names from source."""
    topic_file = db_path / "Topic.csv"
    if not topic_file.exists():
        return set()
    
    df = pd.read_csv(topic_file, dtype=str)
    return {normalize_topic(t) for t in df["Topic"] if pd.notna(t) and str(t).strip()}


def load_source_topic_ids(db_path: Path) -> set:
    """Load and return set of TopicIDs from source."""
    topic_file = db_path / "Topic.csv"
    if not topic_file.exists():
        return set()
    
    df = pd.read_csv(topic_file, dtype=str)
    return {str(tid) for tid in df["TopicID"] if pd.notna(tid)}


def main():
    print_section("BOOK TOPIC MIGRATION VALIDATION")
    
    result = ValidationResult()
    
    # Paths
    db1_path = get_path("db1")
    db2_path = get_path("db2")
    output_path = Path("migration_output")
    
    print(f"\nSource DB1: {db1_path}")
    print(f"Source DB2: {db2_path}")
    print(f"Output: {output_path}")
    
    # Check output files exist
    print_subsection("Check 1: Output Files Exist")
    
    required_files = [
        "BOOK_TOPIC.csv",
        "topic_id_mapping.csv",
        "book_topic_enriched_db1.csv",
        "book_topic_enriched_db2.csv",
        "book_topic_migration_metadata.csv",
    ]
    
    for fname in required_files:
        fpath = output_path / fname
        if fpath.exists():
            result.passed(f"{fname} exists")
        else:
            result.failed(f"{fname} is missing!")
    
    if result.checks_failed > 0:
        print("\n   Cannot continue validation - required files missing.")
        return result.summary()
    
    # Load files
    book_topic_new = pd.read_csv(output_path / "BOOK_TOPIC.csv", dtype=str)
    id_mapping = pd.read_csv(output_path / "topic_id_mapping.csv", dtype=str)
    enriched_db1 = pd.read_csv(output_path / "book_topic_enriched_db1.csv", dtype=str)
    enriched_db2 = pd.read_csv(output_path / "book_topic_enriched_db2.csv", dtype=str)
    metadata = pd.read_csv(output_path / "book_topic_migration_metadata.csv").iloc[0]
    
    # Show metadata summary
    print_subsection("Migration Metadata")
    print(f"   Source DB1 BookTopic entries: {int(metadata['source_db1_book_topics'])}")
    print(f"   Source DB2 BookTopic entries: {int(metadata['source_db2_book_topics'])}")
    print(f"   Skipped incomplete (DB1): {int(metadata['skipped_incomplete_db1'])}")
    print(f"   Skipped incomplete (DB2): {int(metadata['skipped_incomplete_db2'])}")
    
    # Load source data
    source_topics_db1 = load_source_topics(db1_path)
    source_topics_db2 = load_source_topics(db2_path)
    source_ids_db1 = load_source_topic_ids(db1_path)
    source_ids_db2 = load_source_topic_ids(db2_path)
    
    # Check 2: All unique topics preserved
    print_subsection("Check 2: All Unique Topics Preserved")
    
    all_source_topics = source_topics_db1 | source_topics_db2
    new_topics = {normalize_topic(t) for t in book_topic_new["topic_name"]}
    
    missing_topics = all_source_topics - new_topics
    if missing_topics:
        result.failed(
            f"{len(missing_topics)} topics missing from BOOK_TOPIC.csv",
            list(missing_topics)
        )
    else:
        result.passed(f"All {len(all_source_topics)} unique topics preserved")
    
    # Check for extra topics (shouldn't happen, but good to verify)
    extra_topics = new_topics - all_source_topics
    if extra_topics:
        result.warn(f"{len(extra_topics)} unexpected topics in output: {extra_topics}")
    
    # Check 3: All TopicIDs mapped
    print_subsection("Check 3: All TopicIDs Have Mappings")
    
    # DB1 mappings
    mapped_db1_ids = set(
        id_mapping[id_mapping["source_db"] == "db1"]["old_topic_id"].astype(str)
    )
    missing_db1_ids = source_ids_db1 - mapped_db1_ids
    if missing_db1_ids:
        result.failed(
            f"{len(missing_db1_ids)} DB1 TopicIDs missing from mapping",
            list(missing_db1_ids)
        )
    else:
        result.passed(f"All {len(source_ids_db1)} DB1 TopicIDs mapped")
    
    # DB2 mappings
    mapped_db2_ids = set(
        id_mapping[id_mapping["source_db"] == "db2"]["old_topic_id"].astype(str)
    )
    missing_db2_ids = source_ids_db2 - mapped_db2_ids
    if missing_db2_ids:
        result.failed(
            f"{len(missing_db2_ids)} DB2 TopicIDs missing from mapping",
            list(missing_db2_ids)
        )
    else:
        result.passed(f"All {len(source_ids_db2)} DB2 TopicIDs mapped")
    
    # Check 4: All mappings point to valid UUIDs
    print_subsection("Check 4: Mapping UUIDs Valid")
    
    valid_uuids = set(book_topic_new["book_topic_id"])
    # Filter out NaN/empty mappings (these are expected for incomplete entries)
    mapped_uuids = set(
        id_mapping[
            id_mapping["new_book_topic_id"].notna() & 
            (id_mapping["new_book_topic_id"] != "") &
            (id_mapping["new_book_topic_id"] != "nan")
        ]["new_book_topic_id"]
    )
    
    invalid_uuids = mapped_uuids - valid_uuids
    if invalid_uuids:
        result.failed(
            f"{len(invalid_uuids)} mappings point to non-existent UUIDs",
            list(invalid_uuids)
        )
    else:
        result.passed(f"All {len(mapped_uuids)} mapping UUIDs exist in BOOK_TOPIC.csv")
    
    # Check 5: Enriched files have topic names and correct row counts
    print_subsection("Check 5: Enriched BookTopic Files Complete")
    
    # Verify row counts match expected (original - skipped)
    expected_db1_rows = int(metadata['source_db1_book_topics']) - int(metadata['skipped_incomplete_db1'])
    expected_db2_rows = int(metadata['source_db2_book_topics']) - int(metadata['skipped_incomplete_db2'])
    
    if len(enriched_db1) == expected_db1_rows:
        result.passed(f"enriched_db1 has expected {expected_db1_rows} rows (after skipping incomplete)")
    else:
        result.failed(f"enriched_db1 has {len(enriched_db1)} rows, expected {expected_db1_rows}")
    
    if len(enriched_db2) == expected_db2_rows:
        result.passed(f"enriched_db2 has expected {expected_db2_rows} rows (after skipping incomplete)")
    else:
        result.failed(f"enriched_db2 has {len(enriched_db2)} rows, expected {expected_db2_rows}")
    
    # Check all rows have TopicNames
    db1_missing_names = enriched_db1[enriched_db1["TopicName"].isna() | (enriched_db1["TopicName"] == "")]
    if len(db1_missing_names) > 0:
        result.failed(
            f"{len(db1_missing_names)} rows in enriched_db1 have no TopicName",
            db1_missing_names["TopicID"].tolist()
        )
    else:
        result.passed(f"All enriched_db1 rows have TopicNames")
    
    # Check db2 enriched
    db2_missing_names = enriched_db2[enriched_db2["TopicName"].isna() | (enriched_db2["TopicName"] == "")]
    if len(db2_missing_names) > 0:
        result.failed(
            f"{len(db2_missing_names)} rows in enriched_db2 have no TopicName",
            db2_missing_names["TopicID"].tolist()
        )
    else:
        result.passed(f"All enriched_db2 rows have TopicNames")
    
    # Check 6: Row count sanity
    print_subsection("Check 6: Row Count Sanity")
    
    print(f"   Source DB1 topics: {len(source_ids_db1)}")
    print(f"   Source DB2 topics: {len(source_ids_db2)}")
    print(f"   Combined unique topics: {len(all_source_topics)}")
    print(f"   New BOOK_TOPIC rows: {len(book_topic_new)}")
    print(f"   ID mapping entries: {len(id_mapping)}")
    
    # New table should have same count as unique topics
    if len(book_topic_new) == len(all_source_topics):
        result.passed("BOOK_TOPIC row count matches unique topic count")
    elif len(book_topic_new) < len(all_source_topics):
        result.failed(
            f"BOOK_TOPIC has fewer rows ({len(book_topic_new)}) than unique topics ({len(all_source_topics)})"
        )
    else:
        result.warn(
            f"BOOK_TOPIC has more rows ({len(book_topic_new)}) than unique topics ({len(all_source_topics)})"
        )
    
    # Mapping should have entry for each source topic ID
    expected_mappings = len(source_ids_db1) + len(source_ids_db2)
    if len(id_mapping) == expected_mappings:
        result.passed(f"ID mapping has expected {expected_mappings} entries")
    else:
        result.warn(
            f"ID mapping has {len(id_mapping)} entries, expected {expected_mappings}"
        )
    
    # Check 7: No Duplicates in New Table
    print_subsection("Check 7: No Duplicates in New Table")
    
    # Check for duplicate UUIDs
    duplicate_uuids = book_topic_new[book_topic_new["book_topic_id"].duplicated()]
    if len(duplicate_uuids) > 0:
        result.failed(
            f"{len(duplicate_uuids)} duplicate UUIDs in BOOK_TOPIC.csv",
            duplicate_uuids["book_topic_id"].tolist()
        )
    else:
        result.passed("No duplicate UUIDs in BOOK_TOPIC.csv")
    
    # Check for duplicate topic names (case-insensitive)
    book_topic_new["_norm"] = book_topic_new["topic_name"].apply(normalize_topic)
    duplicate_names = book_topic_new[book_topic_new["_norm"].duplicated()]
    if len(duplicate_names) > 0:
        result.failed(
            f"{len(duplicate_names)} duplicate topic names (case-insensitive)",
            duplicate_names["topic_name"].tolist()
        )
    else:
        result.passed("No duplicate topic names in BOOK_TOPIC.csv")
    
    # Check 8: Verify skipped entries are accounted for
    print_subsection("Check 8: Skipped Entries Verification")
    
    skipped_db1 = int(metadata['skipped_incomplete_db1'])
    skipped_db2 = int(metadata['skipped_incomplete_db2'])
    total_skipped = skipped_db1 + skipped_db2
    
    if total_skipped > 0:
        result.warn(f"{total_skipped} incomplete entries were skipped (DB1: {skipped_db1}, DB2: {skipped_db2})")
    else:
        result.passed("No incomplete entries were skipped")
    
    # Verify the math adds up
    total_source_db1 = int(metadata['source_db1_book_topics'])
    total_source_db2 = int(metadata['source_db2_book_topics'])
    
    if len(enriched_db1) + skipped_db1 == total_source_db1:
        result.passed(f"DB1 row count verified: {len(enriched_db1)} enriched + {skipped_db1} skipped = {total_source_db1} source")
    else:
        result.failed(f"DB1 row count mismatch: {len(enriched_db1)} + {skipped_db1} != {total_source_db1}")
    
    if len(enriched_db2) + skipped_db2 == total_source_db2:
        result.passed(f"DB2 row count verified: {len(enriched_db2)} enriched + {skipped_db2} skipped = {total_source_db2} source")
    else:
        result.failed(f"DB2 row count mismatch: {len(enriched_db2)} + {skipped_db2} != {total_source_db2}")
    
    # Final summary
    return result.summary()


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)