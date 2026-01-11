import sys
import uuid
import pandas as pd
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from database_utils.config import get_path

def load_db_data(db_path):
    """Loads Authors and BookAuthors, handling missing files gracefully."""
    a_path = db_path / "Authors.csv"
    authors = pd.read_csv(a_path, dtype=str) if a_path.exists() else pd.DataFrame(columns=["AuthorID", "FirstName", "MiddleName", "LastName"])
    
    ba_path = db_path / "BookAuthors.csv"
    links = pd.read_csv(ba_path, dtype=str) if ba_path.exists() else pd.DataFrame(columns=["BookAuthorID", "BookID", "AuthorID"])
    
    return authors, links

def clean_str(series):
    """Standardizes string columns to prevent NaN issues."""
    return series.fillna('').astype(str).str.strip()

def process_names(df):
    """Adds combined first_name and normalized key columns vectorized."""
    f = clean_str(df.get('FirstName', pd.Series(dtype=str)))
    m = clean_str(df.get('MiddleName', pd.Series(dtype=str)))
    l = clean_str(df.get('LastName', pd.Series(dtype=str)))
    
    combined_first = (f + ' ' + m).str.strip().str.replace(r'\s+', ' ', regex=True)
    
    df['final_first_name'] = combined_first
    df['final_last_name'] = l
    df['key'] = (combined_first.str.lower() + "|" + l.str.lower())
    return df

def is_valid_id(val):
    """Check if a value is a valid, non-empty ID."""
    if pd.isna(val):
        return False
    s = str(val).strip()
    return s != '' and s.lower() != 'nan'

def main():
    out_dir = Path("migration_output")
    out_dir.mkdir(exist_ok=True)
    
    print("Loading data...")
    a1, ba1 = load_db_data(get_path("db1"))
    a2, ba2 = load_db_data(get_path("db2"))
    
    # Track original counts before filtering
    orig_ba1_count = len(ba1)
    orig_ba2_count = len(ba2)
    
    # Filter incomplete BookAuthors entries (missing AuthorID)
    valid_mask_1 = ba1['AuthorID'].apply(is_valid_id)
    valid_mask_2 = ba2['AuthorID'].apply(is_valid_id)
    
    skipped_db1 = (~valid_mask_1).sum()
    skipped_db2 = (~valid_mask_2).sum()
    
    if skipped_db1 > 0:
        print(f"  Skipping {skipped_db1} incomplete entries in DB1 (missing AuthorID)")
    if skipped_db2 > 0:
        print(f"  Skipping {skipped_db2} incomplete entries in DB2 (missing AuthorID)")
    
    ba1 = ba1[valid_mask_1].copy()
    ba2 = ba2[valid_mask_2].copy()
    
    # 1. Pre-process Authors (Combine Names & Create Keys)
    a1['source_db'] = 'db1'
    a2['source_db'] = 'db2'
    
    a1 = process_names(a1)
    a2 = process_names(a2)
    
    # Filter out empty entries (key == "|")
    a1 = a1[a1['key'] != '|']
    a2 = a2[a2['key'] != '|']
    
    # 2. Merge and Deduplicate
    all_authors = pd.concat([a1, a2], ignore_index=True)
    unique_authors = all_authors.drop_duplicates(subset='key').copy()
    unique_authors['book_author_id'] = [str(uuid.uuid4()) for _ in range(len(unique_authors))]
    
    print(f"Merged {len(all_authors)} total source authors into {len(unique_authors)} unique authors.")

    # 3. Create ID Mapping
    key_to_uuid = unique_authors[['key', 'book_author_id']].set_index('key')
    
    mapping = all_authors[['source_db', 'AuthorID', 'FirstName', 'MiddleName', 'LastName', 'key']].copy()
    mapping = mapping.merge(key_to_uuid, on='key', how='left')
    
    mapping.rename(columns={
        'AuthorID': 'old_author_id',
        'FirstName': 'old_first_name', 
        'MiddleName': 'old_middle_name',
        'LastName': 'old_last_name',
        'book_author_id': 'new_book_author_id'
    }, inplace=True)

    # 4. Enrich BookAuthors (Link Tables)
    map_db1 = mapping[mapping['source_db'] == 'db1'].set_index('old_author_id')
    ba1_enriched = ba1.merge(map_db1[['old_first_name', 'old_last_name', 'new_book_author_id']], 
                             left_on='AuthorID', right_index=True, how='left')
    
    map_db2 = mapping[mapping['source_db'] == 'db2'].set_index('old_author_id')
    ba2_enriched = ba2.merge(map_db2[['old_first_name', 'old_last_name', 'new_book_author_id']], 
                             left_on='AuthorID', right_index=True, how='left')
    
    # Filter out orphaned BookAuthors (AuthorID not in Authors table)
    orphaned_db1 = ba1_enriched['new_book_author_id'].isna().sum()
    orphaned_db2 = ba2_enriched['new_book_author_id'].isna().sum()
    if orphaned_db1 > 0:
        print(f"  Filtering {orphaned_db1} DB1 entries with AuthorIDs not in Authors table")
        ba1_enriched = ba1_enriched[ba1_enriched['new_book_author_id'].notna()].copy()
    if orphaned_db2 > 0:
        print(f"  Filtering {orphaned_db2} DB2 entries with AuthorIDs not in Authors table")
        ba2_enriched = ba2_enriched[ba2_enriched['new_book_author_id'].notna()].copy()

    # 5. Save Outputs
    print("Saving files...")
    
    final_table = unique_authors[['book_author_id', 'final_first_name', 'final_last_name']].rename(
        columns={'final_first_name': 'first_name', 'final_last_name': 'last_name'}
    )
    final_table.to_csv(out_dir / "BOOK_AUTHORS.csv", index=False)
    
    mapping.drop(columns=['key']).to_csv(out_dir / "author_id_mapping.csv", index=False)
    
    ba1_enriched.to_csv(out_dir / "book_authors_enriched_db1.csv", index=False)
    ba2_enriched.to_csv(out_dir / "book_authors_enriched_db2.csv", index=False)
    
    # Metadata for validation
    meta = {
        'source_db1_authors': len(a1),
        'source_db2_authors': len(a2),
        'source_db1_book_authors': orig_ba1_count,
        'source_db2_book_authors': orig_ba2_count,
        'skipped_incomplete_db1': skipped_db1,
        'skipped_incomplete_db2': skipped_db2,
        'orphaned_db1': orphaned_db1,
        'orphaned_db2': orphaned_db2,
        'enriched_db1_rows': len(ba1_enriched),
        'enriched_db2_rows': len(ba2_enriched),
        'unique_authors': len(unique_authors),
        'id_mappings': len(mapping),
    }
    pd.DataFrame([meta]).to_csv(out_dir / "book_authors_migration_metadata.csv", index=False)
    
    print("Migration complete.")
    print(f"  - {len(unique_authors)} unique authors")
    print(f"  - {len(mapping)} ID mappings")
    print(f"  - {len(ba1_enriched)} enriched DB1 links")
    print(f"  - {len(ba2_enriched)} enriched DB2 links")

if __name__ == "__main__":
    main()