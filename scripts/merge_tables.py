"""
Merge tables from db1 and db2 into a unified merged database.

This script:
1. Loads matching tables from both databases
2. Aligns columns to handle schema differences
3. Concatenates data and removes duplicates
4. Saves merged tables to the output directory
"""
import pandas as pd
from database_utils.io import load_csv
from database_utils.align import align_columns
from database_utils.config import get_path
from database_utils.reporting import (
    print_section,
    print_success,
    print_warning,
    print_error
)


def merge_table(
    table_name: str,
    db1_path,
    db2_path,
    output_path
) -> bool:
    """
    Merge a single table from two databases.
    
    Args:
        table_name: Name of the CSV file (e.g., "Books.csv")
        db1_path: Path to first database directory
        db2_path: Path to second database directory
        output_path: Path to output directory
        
    Returns:
        True if merge was successful, False otherwise
    """
    dfs = []
    
    # Try to load from db1
    file1 = db1_path / table_name
    if file1.exists():
        df1 = load_csv(file1)
        if df1 is not None:
            dfs.append(df1)
    
    # Try to load from db2
    file2 = db2_path / table_name
    if file2.exists():
        df2 = load_csv(file2)
        if df2 is not None:
            dfs.append(df2)
    
    # Skip if no data found
    if not dfs:
        print_warning(f"No data found in either database", table_name)
        return False
    
    # If only one database has this table, use it directly
    if len(dfs) == 1:
        merged = dfs[0]
        source = "db1" if file1.exists() and not file2.exists() else "db2"
        print(f"{table_name}: {len(merged)} rows (from {source} only)")
    else:
        # Align columns to handle schema differences
        aligned = align_columns(dfs)
        
        # Concatenate and remove duplicates
        merged = pd.concat(aligned, ignore_index=True).drop_duplicates()
        print_success(f"{table_name}: {len(merged)} rows (merged)")
    
    # Save merged table
    try:
        merged.to_csv(output_path / table_name, index=False)
        return True
    except Exception as e:
        print_error(f"Failed to save: {e}", table_name)
        return False


def main():
    """Main entry point for merging tables."""
    # Load paths from config
    db1 = get_path("db1")
    db2 = get_path("db2")
    output = get_path("merged")
    
    # Create output directory
    output.mkdir(exist_ok=True)

    print_section("Merging Tables from Both Databases")
    print(f"Database 1: {db1}")
    print(f"Database 2: {db2}")
    print(f"Output: {output}\n")

    # Get all unique table names from both databases
    tables_db1 = {p.name for p in db1.glob("*.csv")}
    tables_db2 = {p.name for p in db2.glob("*.csv")}
    all_tables = sorted(tables_db1 | tables_db2)

    print(f"Found {len(all_tables)} unique tables")
    print(f"   In db1: {len(tables_db1)}")
    print(f"   In db2: {len(tables_db2)}")
    print(f"   In both: {len(tables_db1 & tables_db2)}\n")

    # Merge each table
    success_count = 0
    for table_name in all_tables:
        if merge_table(table_name, db1, db2, output):
            success_count += 1

    # Print summary
    print(f"\nSummary: Successfully merged {success_count}/{len(all_tables)} tables")


if __name__ == "__main__":
    main()