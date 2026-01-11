"""
Book Topic Migration Script

This script merges BookTopic data from two old databases:
1. Loads Topic.csv (TopicID, Topic) from both databases
2. Loads BookTopic.csv (BookTopicID, TopicID, BookID) from both databases  
3. Enriches BookTopic with actual topic names
4. Deduplicates topic names across both databases
5. Generates new UUIDs for the unified BOOK_TOPIC table
6. Creates mapping files for future reference

Output:
- BOOK_TOPIC.csv: New unified topic table with UUIDs
- topic_id_mapping.csv: Maps old TopicIDs to new UUIDs (for updating references)
- book_topic_enriched_db1.csv: BookTopic from db1 with topic names added
- book_topic_enriched_db2.csv: BookTopic from db2 with topic names added
"""

import sys
import uuid
from pathlib import Path

import pandas as pd

# Add parent directory to path for database_utils imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from database_utils import load_csv, normalize_value
from database_utils.config import get_path
from database_utils.reporting import (
    print_section,
    print_subsection,
    print_success,
    print_warning,
    print_error,
)


def load_topic_table(db_path: Path) -> pd.DataFrame:
    """Load Topic.csv from a database directory."""
    topic_file = db_path / "Topic.csv"
    if not topic_file.exists():
        print_error(f"Topic.csv not found in {db_path}")
        return pd.DataFrame(columns=["TopicID", "Topic"])
    
    df = load_csv(topic_file, normalize=False)
    if df is None:
        return pd.DataFrame(columns=["TopicID", "Topic"])
    
    print(f"   Loaded {len(df)} topics from {db_path.name}")
    return df


def load_book_topic_table(db_path: Path) -> pd.DataFrame:
    """Load BookTopic.csv from a database directory."""
    book_topic_file = db_path / "BookTopic.csv"
    if not book_topic_file.exists():
        print_error(f"BookTopic.csv not found in {db_path}")
        return pd.DataFrame(columns=["BookTopicID", "TopicID", "BookID"])
    
    df = load_csv(book_topic_file, normalize=False)
    if df is None:
        return pd.DataFrame(columns=["BookTopicID", "TopicID", "BookID"])
    
    print(f"   Loaded {len(df)} book-topic links from {db_path.name}")
    return df


def create_topic_id_map(topic_df: pd.DataFrame) -> dict:
    """Create a mapping from TopicID to Topic name."""
    return dict(zip(topic_df["TopicID"].astype(str), topic_df["Topic"]))


def enrich_book_topics(
    book_topic_df: pd.DataFrame, 
    topic_map: dict,
    db_name: str
) -> tuple[pd.DataFrame, int]:
    """
    Add topic names to BookTopic dataframe.
    Skips incomplete entries (missing TopicID).
    
    Returns:
        Tuple of (enriched_df, skipped_count)
    """
    df = book_topic_df.copy()
    
    # Identify incomplete entries (missing TopicID)
    incomplete_mask = df["TopicID"].isna() | (df["TopicID"].astype(str).str.strip() == "") | (df["TopicID"].astype(str) == "nan")
    incomplete_count = incomplete_mask.sum()
    
    if incomplete_count > 0:
        print_warning(f"{incomplete_count} incomplete entries (missing TopicID) skipped in {db_name}")
    
    # Filter out incomplete entries
    df = df[~incomplete_mask].copy()
    
    # Map topic names
    df["TopicID_str"] = df["TopicID"].astype(str)
    df["TopicName"] = df["TopicID_str"].map(topic_map)
    
    # Check for unmapped topics (TopicID exists but not in Topic table - this is a data issue)
    unmapped = df[df["TopicName"].isna()]
    if len(unmapped) > 0:
        print_warning(f"{len(unmapped)} entries have TopicIDs not found in Topic table in {db_name}")
        print(f"   Unknown TopicIDs: {unmapped['TopicID'].unique().tolist()[:10]}...")
    
    df = df.drop(columns=["TopicID_str"])
    return df, incomplete_count


def normalize_topic_name(name: str) -> str:
    """Normalize topic name for deduplication comparison."""
    if pd.isna(name) or name is None:
        return ""
    return str(name).strip().lower()


def merge_and_deduplicate_topics(
    topics_db1: pd.DataFrame,
    topics_db2: pd.DataFrame
) -> pd.DataFrame:
    """
    Merge topics from both databases and deduplicate by normalized name.
    Keeps the first occurrence's original casing.
    """
    all_topics = []
    seen_normalized = set()
    
    # Process db1 topics first
    for _, row in topics_db1.iterrows():
        topic = row["Topic"]
        normalized = normalize_topic_name(topic)
        if normalized and normalized not in seen_normalized:
            seen_normalized.add(normalized)
            all_topics.append({
                "original_topic": topic,
                "normalized_topic": normalized,
                "source": "db1",
                "original_topic_id": row["TopicID"]
            })
    
    # Process db2 topics
    for _, row in topics_db2.iterrows():
        topic = row["Topic"]
        normalized = normalize_topic_name(topic)
        if normalized and normalized not in seen_normalized:
            seen_normalized.add(normalized)
            all_topics.append({
                "original_topic": topic,
                "normalized_topic": normalized,
                "source": "db2",
                "original_topic_id": row["TopicID"]
            })
    
    return pd.DataFrame(all_topics)


def generate_new_book_topic_table(merged_topics: pd.DataFrame) -> pd.DataFrame:
    """
    Generate the new BOOK_TOPIC table with UUIDs.
    
    Schema:
        book_topic_id: uuid
        topic_name: text
    """
    new_table = pd.DataFrame({
        "book_topic_id": [str(uuid.uuid4()) for _ in range(len(merged_topics))],
        "topic_name": merged_topics["original_topic"].values
    })
    return new_table


def create_topic_id_mapping(
    topics_db1: pd.DataFrame,
    topics_db2: pd.DataFrame,
    new_book_topic: pd.DataFrame,
    merged_topics: pd.DataFrame
) -> pd.DataFrame:
    """
    Create a mapping from old TopicIDs to new book_topic_ids.
    This helps update references in other tables during migration.
    """
    # Create normalized -> new_uuid mapping
    normalized_to_uuid = {}
    for idx, row in merged_topics.iterrows():
        normalized_to_uuid[row["normalized_topic"]] = new_book_topic.loc[idx, "book_topic_id"]
    
    mappings = []
    
    # Map db1 TopicIDs
    for _, row in topics_db1.iterrows():
        normalized = normalize_topic_name(row["Topic"])
        new_uuid = normalized_to_uuid.get(normalized, "")
        mappings.append({
            "source_db": "db1",
            "old_topic_id": row["TopicID"],
            "old_topic_name": row["Topic"],
            "new_book_topic_id": new_uuid
        })
    
    # Map db2 TopicIDs
    for _, row in topics_db2.iterrows():
        normalized = normalize_topic_name(row["Topic"])
        new_uuid = normalized_to_uuid.get(normalized, "")
        mappings.append({
            "source_db": "db2",
            "old_topic_id": row["TopicID"],
            "old_topic_name": row["Topic"],
            "new_book_topic_id": new_uuid
        })
    
    return pd.DataFrame(mappings)


def main():
    print_section("BOOK TOPIC MIGRATION")
    
    # Get paths from config
    db1_path = get_path("db1")
    db2_path = get_path("db2")
    output_path = Path("migration_output")
    output_path.mkdir(exist_ok=True)
    
    print(f"\nSource DB1: {db1_path}")
    print(f"Source DB2: {db2_path}")
    print(f"Output: {output_path}")
    
    # Step 1: Load Topic tables
    print_subsection("Step 1: Loading Topic Tables")
    topics_db1 = load_topic_table(db1_path)
    topics_db2 = load_topic_table(db2_path)
    
    if topics_db1.empty and topics_db2.empty:
        print_error("No topic data found in either database!")
        return
    
    # Step 2: Load BookTopic tables
    print_subsection("Step 2: Loading BookTopic Tables")
    book_topics_db1 = load_book_topic_table(db1_path)
    book_topics_db2 = load_book_topic_table(db2_path)
    
    # Step 3: Create topic ID -> name mappings
    print_subsection("Step 3: Creating Topic Mappings")
    topic_map_db1 = create_topic_id_map(topics_db1)
    topic_map_db2 = create_topic_id_map(topics_db2)
    print(f"   DB1 has {len(topic_map_db1)} unique topic mappings")
    print(f"   DB2 has {len(topic_map_db2)} unique topic mappings")
    
    # Step 4: Enrich BookTopic tables with topic names
    print_subsection("Step 4: Enriching BookTopic Tables")
    enriched_db1, skipped_db1 = enrich_book_topics(book_topics_db1, topic_map_db1, "db1")
    enriched_db2, skipped_db2 = enrich_book_topics(book_topics_db2, topic_map_db2, "db2")
    
    # Save enriched BookTopic tables
    enriched_db1_path = output_path / "book_topic_enriched_db1.csv"
    enriched_db2_path = output_path / "book_topic_enriched_db2.csv"
    enriched_db1.to_csv(enriched_db1_path, index=False)
    enriched_db2.to_csv(enriched_db2_path, index=False)
    print_success(f"Saved enriched db1 BookTopic to {enriched_db1_path} ({len(enriched_db1)} rows)")
    print_success(f"Saved enriched db2 BookTopic to {enriched_db2_path} ({len(enriched_db2)} rows)")
    
    # Step 5: Merge and deduplicate topics
    print_subsection("Step 5: Merging and Deduplicating Topics")
    merged_topics = merge_and_deduplicate_topics(topics_db1, topics_db2)
    print(f"   Total topics from db1: {len(topics_db1)}")
    print(f"   Total topics from db2: {len(topics_db2)}")
    print(f"   Unique topics after deduplication: {len(merged_topics)}")
    
    # Show some examples of deduplication
    db1_only = merged_topics[merged_topics["source"] == "db1"]
    db2_only = merged_topics[merged_topics["source"] == "db2"]
    print(f"   Topics unique to db1: {len(db1_only)}")
    print(f"   Topics unique to db2 (not in db1): {len(db2_only)}")
    
    # Step 6: Generate new BOOK_TOPIC table with UUIDs
    print_subsection("Step 6: Generating New BOOK_TOPIC Table")
    new_book_topic = generate_new_book_topic_table(merged_topics)
    
    book_topic_path = output_path / "BOOK_TOPIC.csv"
    new_book_topic.to_csv(book_topic_path, index=False)
    print_success(f"Saved new BOOK_TOPIC table to {book_topic_path}")
    print(f"   New table has {len(new_book_topic)} topics")
    
    # Step 7: Create ID mapping for reference
    print_subsection("Step 7: Creating ID Mapping")
    id_mapping = create_topic_id_mapping(
        topics_db1, topics_db2, new_book_topic, merged_topics
    )
    
    mapping_path = output_path / "topic_id_mapping.csv"
    id_mapping.to_csv(mapping_path, index=False)
    print_success(f"Saved ID mapping to {mapping_path}")
    print(f"   Mapping has {len(id_mapping)} entries")
    
    # Step 8: Save migration metadata for validation
    print_subsection("Step 8: Saving Migration Metadata")
    metadata = {
        "source_db1_topics": len(topics_db1),
        "source_db2_topics": len(topics_db2),
        "source_db1_book_topics": len(book_topics_db1),
        "source_db2_book_topics": len(book_topics_db2),
        "skipped_incomplete_db1": skipped_db1,
        "skipped_incomplete_db2": skipped_db2,
        "enriched_db1_rows": len(enriched_db1),
        "enriched_db2_rows": len(enriched_db2),
        "unique_topics": len(new_book_topic),
        "id_mappings": len(id_mapping),
    }
    metadata_df = pd.DataFrame([metadata])
    metadata_path = output_path / "book_topic_migration_metadata.csv"
    metadata_df.to_csv(metadata_path, index=False)
    print_success(f"Saved migration metadata to {metadata_path}")
    
    # Summary
    print_section("MIGRATION SUMMARY")
    
    total_skipped = skipped_db1 + skipped_db2
    if total_skipped > 0:
        print(f"""
Incomplete Entries Skipped:
  - DB1: {skipped_db1} entries with missing TopicID
  - DB2: {skipped_db2} entries with missing TopicID
  - Total skipped: {total_skipped}
""")
    
    print(f"""
Output Files:
  1. BOOK_TOPIC.csv - New unified topic table ({len(new_book_topic)} topics)
     Columns: book_topic_id (UUID), topic_name

  2. topic_id_mapping.csv - Old ID to new UUID mapping ({len(id_mapping)} entries)
     Use this to update BookTopic.TopicID references to new UUIDs

  3. book_topic_enriched_db1.csv - DB1 BookTopic with topic names ({len(enriched_db1)} rows)
  4. book_topic_enriched_db2.csv - DB2 BookTopic with topic names ({len(enriched_db2)} rows)
     These show which books are linked to which topics

Next Steps:
  - Use topic_id_mapping.csv to update book references during Books migration
  - The enriched files can help verify the migration is correct
""")

if __name__ == "__main__":
    main()