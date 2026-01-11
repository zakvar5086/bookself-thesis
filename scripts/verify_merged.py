"""
Verify that merged database contains all data from source databases.

This script checks that no unique rows were lost during the merge process
by comparing row hashes between source and merged tables.
"""
from database_utils.config import get_path
from database_utils.comparison import load_table_pair, find_missing_rows
from database_utils.align import align_two
from database_utils.reporting import (
    print_section,
    print_table_summary,
    print_warning,
    print_error
)


def verify_table(
    table_name: str,
    source_path,
    merged_path,
    source_label: str
) -> dict:
    """
    Verify a single table from source is fully represented in merged.
    
    Args:
        table_name: Name of the CSV file
        source_path: Path to source database directory
        merged_path: Path to merged database directory
        source_label: Label for the source (e.g., "db1" or "db2")
        
    Returns:
        Dict with verification stats or None if verification failed
    """
    source_file = source_path / table_name
    merged_file = merged_path / table_name
    
    # Check if files exist
    if not source_file.exists():
        return None
        
    if not merged_file.exists():
        print_error("Not found in merged database", table_name)
        return {"missing": True}
    
    # Load tables
    source_df, merged_df = load_table_pair(
        table_name,
        source_path,
        merged_path
    )
    
    if source_df is None or merged_df is None:
        print_error("Failed to load", table_name)
        return {"error": True}
    
    # Align columns for fair comparison
    source_df, merged_df = align_two(source_df, merged_df)
    
    # Find missing rows
    missing = find_missing_rows(source_df, merged_df)
    
    # Print results
    print_table_summary(
        table_name,
        len(source_df),
        len(merged_df),
        missing
    )
    
    return {
        "source_rows": len(source_df),
        "merged_rows": len(merged_df),
        "missing_rows": missing
    }


def verify_database(db_path, db_label: str, merged_path):
    """
    Verify all tables from a source database.
    
    Args:
        db_path: Path to source database
        db_label: Label for the database (e.g., "db1")
        merged_path: Path to merged database
    """
    print_section(f"Verifying {db_label.upper()}")
    
    tables = sorted(db_path.glob("*.csv"))
    
    if not tables:
        print_warning(f"No tables found in {db_path}")
        return
    
    total_missing = 0
    tables_with_issues = 0
    
    # Verify each table
    for table_file in tables:
        result = verify_table(
            table_file.name,
            db_path,
            merged_path,
            db_label
        )
        
        if result and result.get("missing_rows", 0) > 0:
            total_missing += result["missing_rows"]
            tables_with_issues += 1
    
    # Print summary
    print(f"\nSummary for {db_label}:")
    print(f"   Tables checked: {len(tables)}")
    print(f"   Tables with missing rows: {tables_with_issues}")
    print(f"   Total missing unique rows: {total_missing}")


def main():
    """Main entry point for verifying merged database."""
    # Load paths from config
    db1 = get_path("db1")
    db2 = get_path("db2")
    merged = get_path("merged")

    print_section("Merged Database Verification")
    print(f"Checking that merged database contains all data from sources\n")

    # Verify both source databases
    verify_database(db1, "db1", merged)
    verify_database(db2, "db2", merged)


if __name__ == "__main__":
    main()