"""
Merge PAuthors tables from db1 and db2, deduplicate by name, and generate UUID mappings.

Usage:
    python -m scripts_migration.pauthors_merge
"""

import json
import uuid
import pandas as pd
from pathlib import Path

NS_PAUTHORS = uuid.uuid5(uuid.NAMESPACE_DNS, "bookshelf.thesis.pauthors")


def load_config():
    with open("config.json") as f:
        return json.load(f)


def get_path(key):
    return Path(load_config()["paths"][key])


def load_db_data(db_path):
    pa_path = db_path / "PAuthors.csv"
    pauthors = (
        pd.read_csv(pa_path, dtype=str)
        if pa_path.exists()
        else pd.DataFrame(columns=["AuthorID", "FirstName", "LastName"])
    )

    link_path = db_path / "PapersAuthors.csv"
    links = (
        pd.read_csv(link_path, dtype=str)
        if link_path.exists()
        else pd.DataFrame(columns=["PapersAuthorsID", "PaperID", "AuthorID"])
    )

    return pauthors, links


def clean_str(series):
    return series.fillna("").astype(str).str.strip()


def process_names(df):
    f = clean_str(df.get("FirstName", pd.Series(dtype=str)))
    l = clean_str(df.get("LastName", pd.Series(dtype=str)))

    df["final_first_name"] = f
    df["final_last_name"] = l
    df["key"] = f.str.lower() + "|" + l.str.lower()
    return df


def is_valid_id(val):
    if pd.isna(val):
        return False
    s = str(val).strip()
    return s != "" and s.lower() != "nan"


def generate_pauthor_uuid(row):
    first = str(row.get("final_first_name", "") or "").strip()
    last = str(row.get("final_last_name", "") or "").strip()

    if last or first:
        key = f"lastname:{last}:firstname:{first}"
    else:
        key = f"fallback:{row.get('source_db', 'unknown')}:{row.get('AuthorID', 'unknown')}"

    return str(uuid.uuid5(NS_PAUTHORS, key))


def main():
    final_dir = get_path("final_tables")
    meta_dir = get_path("metadata") / "PAUTHORS"
    final_dir.mkdir(exist_ok=True)
    meta_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("PAUTHORS MERGE")
    print("=" * 60)

    print("\n[INFO] Loading data...")
    pa1, links1 = load_db_data(get_path("db1"))
    pa2, links2 = load_db_data(get_path("db2"))

    orig_links1_count = len(links1)
    orig_links2_count = len(links2)

    # Filter incomplete PapersAuthors entries
    valid_mask_1 = links1["AuthorID"].apply(is_valid_id)
    valid_mask_2 = links2["AuthorID"].apply(is_valid_id)

    skipped_db1 = (~valid_mask_1).sum()
    skipped_db2 = (~valid_mask_2).sum()

    if skipped_db1 > 0:
        print(
            f"[WARN] Skipping {skipped_db1} incomplete entries in DB1 (missing AuthorID)"
        )
    if skipped_db2 > 0:
        print(
            f"[WARN] Skipping {skipped_db2} incomplete entries in DB2 (missing AuthorID)"
        )

    links1 = links1[valid_mask_1].copy()
    links2 = links2[valid_mask_2].copy()

    # Pre-process PAuthors
    pa1["source_db"] = "db1"
    pa2["source_db"] = "db2"

    pa1 = process_names(pa1)
    pa2 = process_names(pa2)

    # Filter empty entries
    pa1 = pa1[pa1["key"] != "|"]
    pa2 = pa2[pa2["key"] != "|"]

    # Merge and deduplicate
    all_pauthors = pd.concat([pa1, pa2], ignore_index=True)
    unique_pauthors = all_pauthors.drop_duplicates(subset="key").copy()
    unique_pauthors["author_id"] = unique_pauthors.apply(generate_pauthor_uuid, axis=1)

    print(
        f"[INFO] Merged {len(all_pauthors)} pauthors -> {len(unique_pauthors)} unique"
    )

    # Create ID mapping
    key_to_uuid = unique_pauthors[["key", "author_id"]].set_index("key")

    mapping = all_pauthors[
        ["source_db", "AuthorID", "FirstName", "LastName", "key"]
    ].copy()
    mapping = mapping.merge(key_to_uuid, on="key", how="left")

    mapping.rename(
        columns={
            "AuthorID": "old_author_id",
            "FirstName": "old_first_name",
            "LastName": "old_last_name",
            "author_id": "new_author_id",
        },
        inplace=True,
    )

    # Enrich PapersAuthors (Link Tables)
    map_db1 = mapping[mapping["source_db"] == "db1"].set_index("old_author_id")
    links1_enriched = links1.merge(
        map_db1[["old_first_name", "old_last_name", "new_author_id"]],
        left_on="AuthorID",
        right_index=True,
        how="left",
    )

    map_db2 = mapping[mapping["source_db"] == "db2"].set_index("old_author_id")
    links2_enriched = links2.merge(
        map_db2[["old_first_name", "old_last_name", "new_author_id"]],
        left_on="AuthorID",
        right_index=True,
        how="left",
    )

    # Filter orphaned entries
    orphaned_db1 = links1_enriched["new_author_id"].isna().sum()
    orphaned_db2 = links2_enriched["new_author_id"].isna().sum()
    if orphaned_db1 > 0:
        print(
            f"[WARN] Filtering {orphaned_db1} DB1 entries with AuthorIDs not in PAuthors table"
        )
        links1_enriched = links1_enriched[
            links1_enriched["new_author_id"].notna()
        ].copy()
    if orphaned_db2 > 0:
        print(
            f"[WARN] Filtering {orphaned_db2} DB2 entries with AuthorIDs not in PAuthors table"
        )
        links2_enriched = links2_enriched[
            links2_enriched["new_author_id"].notna()
        ].copy()

    # Save outputs
    print("\n[INFO] Saving files...")

    final_table = unique_pauthors[
        ["author_id", "final_first_name", "final_last_name"]
    ].rename(columns={"final_first_name": "first_name", "final_last_name": "last_name"})
    final_table.to_csv(final_dir / "PAUTHORS.csv", index=False)
    mapping.drop(columns=["key"]).to_csv(
        meta_dir / "pauthor_id_mapping.csv", index=False
    )
    links1_enriched.to_csv(meta_dir / "papers_authors_enriched_db1.csv", index=False)
    links2_enriched.to_csv(meta_dir / "papers_authors_enriched_db2.csv", index=False)

    # Metadata
    meta = {
        "source_db1_pauthors": len(pa1),
        "source_db2_pauthors": len(pa2),
        "source_db1_papers_authors": orig_links1_count,
        "source_db2_papers_authors": orig_links2_count,
        "skipped_incomplete_db1": skipped_db1,
        "skipped_incomplete_db2": skipped_db2,
        "orphaned_db1": orphaned_db1,
        "orphaned_db2": orphaned_db2,
        "enriched_db1_rows": len(links1_enriched),
        "enriched_db2_rows": len(links2_enriched),
        "unique_pauthors": len(unique_pauthors),
        "id_mappings": len(mapping),
    }
    pd.DataFrame([meta]).to_csv(
        meta_dir / "pauthors_migration_metadata.csv", index=False
    )

    print("\n[PASS] Migration complete")
    print(f"  - {len(unique_pauthors)} unique pauthors")
    print(f"  - {len(mapping)} ID mappings")
    print(f"  - {len(links1_enriched)} enriched DB1 links")
    print(f"  - {len(links2_enriched)} enriched DB2 links")


if __name__ == "__main__":
    main()
