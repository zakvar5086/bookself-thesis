"""
Merge Papers tables from db1 and db2, deduplicate by title, generate UUID mappings,
and build authors_ids arrays from PapersAuthors links.

Usage:
    python -m scripts_migration.papers_merge

Requires:
  - pauthors_merge to have been run first (needs enriched PapersAuthors files)
"""

import json
import uuid
import pandas as pd
from pathlib import Path

NS_PAPERS = uuid.uuid5(uuid.NAMESPACE_DNS, "bookshelf.thesis.papers")


def load_config():
    with open("config.json") as f:
        return json.load(f)


def get_path(key):
    return Path(load_config()["paths"][key])


def load_papers(db_path):
    p_path = db_path / "Papers.csv"
    return (
        pd.read_csv(p_path, dtype=str)
        if p_path.exists()
        else pd.DataFrame(
            columns=[
                "PaperID",
                "CnfJ",
                "Project",
                "Topic",
                "Description",
                "Title",
                "Year",
                "SoftCopy",
                "HardCopy",
                "Link",
                "Accepted",
                "CondAccepted",
                "Submitted",
                "UndSubmission",
                "InPress",
                "FullPaper",
                "Abstract",
            ]
        )
    )


def clean_str(val):
    if pd.isna(val):
        return ""
    return str(val).strip()


def normalize_title(series):
    return series.fillna("").astype(str).str.strip().str.lower()


def is_valid_id(val):
    if pd.isna(val):
        return False
    s = str(val).strip()
    return s != "" and s.lower() != "nan"


def parse_bool(val):
    if pd.isna(val):
        return False
    s = str(val).strip().lower()
    return s in ("1", "true", "yes", "-1")


def generate_paper_uuid(row):
    title = str(row.get("Title", "") or "").strip()
    year = str(row.get("Year", "") or "").strip()

    if title:
        key = f"title:{title.lower()}:year:{year}"
    else:
        key = f"fallback:{row.get('source_db', 'unknown')}:{row.get('PaperID', 'unknown')}"

    return str(uuid.uuid5(NS_PAPERS, key))


def main():
    final_dir = get_path("final_tables")
    meta_dir = get_path("metadata") / "PAPERS"
    pauthors_meta_dir = get_path("metadata") / "PAUTHORS"
    final_dir.mkdir(exist_ok=True)
    meta_dir.mkdir(parents=True, exist_ok=True)

    print("=" * 60)
    print("PAPERS MERGE")
    print("=" * 60)

    enriched_db1_path = pauthors_meta_dir / "papers_authors_enriched_db1.csv"
    enriched_db2_path = pauthors_meta_dir / "papers_authors_enriched_db2.csv"

    if not enriched_db1_path.exists() or not enriched_db2_path.exists():
        print(
            "[FAIL] Run pauthors_merge first to generate enriched PapersAuthors files"
        )
        return

    print("\n[INFO] Loading data...")
    p1 = load_papers(get_path("db1"))
    p2 = load_papers(get_path("db2"))

    pa_enriched_db1 = pd.read_csv(enriched_db1_path, dtype=str)
    pa_enriched_db2 = pd.read_csv(enriched_db2_path, dtype=str)

    orig_p1_count = len(p1)
    orig_p2_count = len(p2)

    # Filter papers with invalid PaperID
    valid_mask_1 = p1["PaperID"].apply(is_valid_id)
    valid_mask_2 = p2["PaperID"].apply(is_valid_id)

    skipped_db1 = (~valid_mask_1).sum()
    skipped_db2 = (~valid_mask_2).sum()

    if skipped_db1 > 0:
        print(
            f"[WARN] Skipping {skipped_db1} incomplete entries in DB1 (missing PaperID)"
        )
    if skipped_db2 > 0:
        print(
            f"[WARN] Skipping {skipped_db2} incomplete entries in DB2 (missing PaperID)"
        )

    p1 = p1[valid_mask_1].copy()
    p2 = p2[valid_mask_2].copy()

    # Filter papers with empty titles
    p1_empty_title = p1["Title"].isna() | (p1["Title"].str.strip() == "")
    p2_empty_title = p2["Title"].isna() | (p2["Title"].str.strip() == "")

    skipped_empty_db1 = p1_empty_title.sum()
    skipped_empty_db2 = p2_empty_title.sum()

    if skipped_empty_db1 > 0:
        print(f"[WARN] Skipping {skipped_empty_db1} DB1 entries with empty titles")
    if skipped_empty_db2 > 0:
        print(f"[WARN] Skipping {skipped_empty_db2} DB2 entries with empty titles")

    p1 = p1[~p1_empty_title].copy()
    p2 = p2[~p2_empty_title].copy()

    p1["source_db"] = "db1"
    p2["source_db"] = "db2"

    # Build author lookup from enriched PapersAuthors
    def build_author_lookup(enriched_df, source_db):
        lookup = {}
        for _, row in enriched_df.iterrows():
            paper_id = str(row.get("PaperID", "")).strip()
            author_id = str(row.get("new_author_id", "")).strip()
            if paper_id and author_id and author_id != "nan":
                if paper_id not in lookup:
                    lookup[paper_id] = []
                if author_id not in lookup[paper_id]:
                    lookup[paper_id].append(author_id)
        return lookup

    author_lookup_db1 = build_author_lookup(pa_enriched_db1, "db1")
    author_lookup_db2 = build_author_lookup(pa_enriched_db2, "db2")

    p1["authors_ids"] = p1["PaperID"].apply(
        lambda x: author_lookup_db1.get(str(x).strip(), [])
    )
    p2["authors_ids"] = p2["PaperID"].apply(
        lambda x: author_lookup_db2.get(str(x).strip(), [])
    )

    # Merge and deduplicate by normalized title + year
    p1["norm_title"] = normalize_title(p1["Title"])
    p2["norm_title"] = normalize_title(p2["Title"])
    p1["norm_year"] = p1["Year"].fillna("").astype(str).str.strip()
    p2["norm_year"] = p2["Year"].fillna("").astype(str).str.strip()
    p1["dedup_key"] = p1["norm_title"] + "|" + p1["norm_year"]
    p2["dedup_key"] = p2["norm_title"] + "|" + p2["norm_year"]

    all_papers = pd.concat([p1, p2], ignore_index=True)

    # For duplicates, merge authors_ids from both sources
    def merge_authors_for_duplicates(group):
        if len(group) == 1:
            return group.iloc[0]

        # Take first row as base (db1 takes precedence)
        base = group.iloc[0].copy()

        # Merge all authors_ids
        all_authors = set()
        for _, row in group.iterrows():
            all_authors.update(row["authors_ids"])
        base["authors_ids"] = list(all_authors)

        return base

    unique_papers = (
        all_papers.groupby("dedup_key", as_index=False)
        .apply(merge_authors_for_duplicates)
        .reset_index(drop=True)
    )

    unique_papers["paper_id"] = unique_papers.apply(generate_paper_uuid, axis=1)

    duplicates_found = len(all_papers) - len(unique_papers)
    print(
        f"[INFO] Merged {len(all_papers)} papers -> {len(unique_papers)} unique ({duplicates_found} duplicates)"
    )

    # ID mapping
    mapping_rows = []
    for _, row in all_papers.iterrows():
        dedup_key = row["dedup_key"]
        new_uuid = unique_papers[unique_papers["dedup_key"] == dedup_key][
            "paper_id"
        ].iloc[0]
        mapping_rows.append(
            {
                "source_db": row["source_db"],
                "old_paper_id": row["PaperID"],
                "old_title": row["Title"],
                "new_paper_id": new_uuid,
            }
        )

    id_mapping = pd.DataFrame(mapping_rows)

    # Prepare final output table
    bool_cols = [
        "SoftCopy",
        "HardCopy",
        "FullPaper",
        "Accepted",
        "CondAccepted",
        "Submitted",
        "UndSubmission",
        "InPress",
    ]

    final_papers = unique_papers[
        [
            "paper_id",
            "Title",
            "Abstract",
            "Description",
            "Topic",
            "Project",
            "Year",
            "CnfJ",
            "Link",
            "SoftCopy",
            "HardCopy",
            "FullPaper",
            "Accepted",
            "CondAccepted",
            "Submitted",
            "UndSubmission",
            "InPress",
            "authors_ids",
        ]
    ].copy()

    for col in bool_cols:
        final_papers[col] = final_papers[col].apply(parse_bool)

    # Rename columns to match new schema
    final_papers.rename(
        columns={
            "Title": "title",
            "Abstract": "abstract",
            "Description": "description",
            "Topic": "topic",
            "Project": "project",
            "Year": "year",
            "CnfJ": "cnfj",
            "Link": "link",
            "SoftCopy": "soft_copy",
            "HardCopy": "hard_copy",
            "FullPaper": "full_paper",
            "Accepted": "accepted",
            "CondAccepted": "cond_accepted",
            "Submitted": "submitted",
            "UndSubmission": "und_submission",
            "InPress": "in_press",
        },
        inplace=True,
    )

    # Convert authors_ids list to string representation for CSV
    final_papers["authors_ids"] = final_papers["authors_ids"].apply(
        lambda x: "{" + ",".join(x) + "}" if x else "{}"
    )

    print("\n[INFO] Saving files...")

    final_papers.to_csv(final_dir / "PAPERS.csv", index=False)
    id_mapping.to_csv(meta_dir / "paper_id_mapping.csv", index=False)

    meta = {
        "source_db1_papers": orig_p1_count,
        "source_db2_papers": orig_p2_count,
        "skipped_invalid_id_db1": skipped_db1,
        "skipped_invalid_id_db2": skipped_db2,
        "skipped_empty_title_db1": skipped_empty_db1,
        "skipped_empty_title_db2": skipped_empty_db2,
        "valid_db1_papers": len(p1),
        "valid_db2_papers": len(p2),
        "duplicates_merged": duplicates_found,
        "unique_papers": len(unique_papers),
        "id_mappings": len(id_mapping),
    }
    pd.DataFrame([meta]).to_csv(meta_dir / "papers_migration_metadata.csv", index=False)

    print("\n[PASS] Migration complete")
    print(f"  - {len(unique_papers)} unique papers")
    print(f"  - {len(id_mapping)} ID mappings")
    print(f"  - {duplicates_found} duplicates merged")


if __name__ == "__main__":
    main()
