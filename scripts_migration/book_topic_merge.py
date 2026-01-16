"""
Merge Topic tables from db1 and db2, deduplicate by name, and generate UUID mappings.

Usage:
    python -m scripts_migration.book_topic_merge
"""

import json
import uuid
import pandas as pd
from pathlib import Path

NS_BOOK_TOPICS = uuid.uuid5(uuid.NAMESPACE_DNS, "bookshelf.thesis.topics")


def load_config():
    with open("config.json") as f:
        return json.load(f)


def get_path(key):
    return Path(load_config()["paths"][key])


def load_db_data(db_path):
    topics = (
        pd.read_csv(db_path / "Topic.csv", dtype=str)
        if (db_path / "Topic.csv").exists()
        else pd.DataFrame(columns=["TopicID", "Topic"])
    )
    links = (
        pd.read_csv(db_path / "BookTopic.csv", dtype=str)
        if (db_path / "BookTopic.csv").exists()
        else pd.DataFrame(columns=["BookTopicID", "TopicID", "BookID"])
    )
    return topics, links


def normalize(series):
    return series.astype(str).str.strip().str.lower()


def is_valid_id(val):
    if pd.isna(val):
        return False
    s = str(val).strip()
    return s != "" and s.lower() != "nan"


def generate_topic_uuid(row):
    topic = str(row.get("Topic", "") or "").strip()

    if topic:
        key = f"topic:{topic.lower()}"
    else:
        # Fallback for empty topics - use original ID and source
        key = f"fallback:{row.get('src', 'unknown')}:{row.get('TopicID', 'unknown')}"

    return str(uuid.uuid5(NS_BOOK_TOPICS, key))


def main():
    out_dir = Path("migration_output")
    out_dir.mkdir(exist_ok=True)

    print("=" * 60)
    print("BOOK TOPIC MERGE")
    print("=" * 60)

    print("\n[INFO] Loading data...")
    t1, bt1 = load_db_data(get_path("db1"))
    t2, bt2 = load_db_data(get_path("db2"))

    orig_bt1_count = len(bt1)
    orig_bt2_count = len(bt2)

    # Filter incomplete BookTopic entries
    valid_mask_1 = bt1["TopicID"].apply(is_valid_id)
    valid_mask_2 = bt2["TopicID"].apply(is_valid_id)

    skipped_db1 = (~valid_mask_1).sum()
    skipped_db2 = (~valid_mask_2).sum()

    if skipped_db1 > 0:
        print(
            f"[WARN] Skipping {skipped_db1} incomplete entries in DB1 (missing TopicID)"
        )
    if skipped_db2 > 0:
        print(
            f"[WARN] Skipping {skipped_db2} incomplete entries in DB2 (missing TopicID)"
        )

    bt1 = bt1[valid_mask_1].copy()
    bt2 = bt2[valid_mask_2].copy()

    # Merge and deduplicate topics
    t1["src"], t2["src"] = "db1", "db2"
    all_topics = pd.concat([t1, t2], ignore_index=True)

    # Filter empty topics
    all_topics = all_topics[
        all_topics["Topic"].notna() & (all_topics["Topic"].str.strip() != "")
    ].copy()

    # Deduplicate by normalized name (db1 takes precedence)
    all_topics["norm_name"] = normalize(all_topics["Topic"])
    unique_topics = all_topics.drop_duplicates(subset="norm_name").copy()
    unique_topics["new_id"] = unique_topics.apply(generate_topic_uuid, axis=1)

    print(f"[INFO] Merged {len(all_topics)} topics -> {len(unique_topics)} unique")

    # Create ID mapping
    norm_to_uuid = dict(zip(unique_topics["norm_name"], unique_topics["new_id"]))

    mapping_rows = []
    for df, db_name in [(t1, "db1"), (t2, "db2")]:
        df = df.copy()
        df["norm"] = normalize(df["Topic"])
        df["new_id"] = df["norm"].map(norm_to_uuid)

        subset = df[["TopicID", "Topic", "new_id"]].copy()
        subset["source_db"] = db_name
        subset.rename(
            columns={
                "TopicID": "old_topic_id",
                "Topic": "old_topic_name",
                "new_id": "new_book_topic_id",
            },
            inplace=True,
        )
        mapping_rows.append(subset)

    id_mapping = pd.concat(mapping_rows, ignore_index=True)

    # Enrich BookTopic
    map_db1 = dict(zip(t1["TopicID"], t1["Topic"]))
    map_db2 = dict(zip(t2["TopicID"], t2["Topic"]))

    bt1["TopicName"] = bt1["TopicID"].map(map_db1)
    bt2["TopicName"] = bt2["TopicID"].map(map_db2)

    # Warn about unmapped TopicIDs
    unmapped_1 = bt1["TopicName"].isna().sum()
    unmapped_2 = bt2["TopicName"].isna().sum()
    if unmapped_1 > 0:
        print(f"[WARN] {unmapped_1} DB1 entries have TopicIDs not in Topic table")
    if unmapped_2 > 0:
        print(f"[WARN] {unmapped_2} DB2 entries have TopicIDs not in Topic table")

    # Save outputs
    print("\n[INFO] Saving files...")

    final_topics = unique_topics[["new_id", "Topic"]].rename(
        columns={"new_id": "book_topic_id", "Topic": "topic_name"}
    )
    final_topics.to_csv(out_dir / "BOOK_TOPIC.csv", index=False)
    id_mapping.to_csv(out_dir / "topic_id_mapping.csv", index=False)
    bt1.to_csv(out_dir / "book_topic_enriched_db1.csv", index=False)
    bt2.to_csv(out_dir / "book_topic_enriched_db2.csv", index=False)

    # Metadata
    meta = {
        "source_db1_topics": len(t1),
        "source_db2_topics": len(t2),
        "source_db1_book_topics": orig_bt1_count,
        "source_db2_book_topics": orig_bt2_count,
        "skipped_incomplete_db1": skipped_db1,
        "skipped_incomplete_db2": skipped_db2,
        "enriched_db1_rows": len(bt1),
        "enriched_db2_rows": len(bt2),
        "unique_topics": len(unique_topics),
        "id_mappings": len(id_mapping),
    }
    pd.DataFrame([meta]).to_csv(
        out_dir / "book_topic_migration_metadata.csv", index=False
    )

    print("\n[PASS] Migration complete")
    print(f"  - {len(unique_topics)} unique topics")
    print(f"  - {len(id_mapping)} ID mappings")
    print(f"  - {len(bt1)} enriched DB1 links")
    print(f"  - {len(bt2)} enriched DB2 links")


if __name__ == "__main__":
    main()
