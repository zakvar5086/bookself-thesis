"""
Universal table comparison tool.

This script provides flexible table comparison capabilities:
1. Compare same table across databases (e.g., Books in db1 vs db2 vs merged)
2. Compare different tables within one database (e.g., Books vs Books1 in db1)
3. Find duplicate rows between databases
4. Verify merge integrity

Usage examples:
    # Compare Books table across all databases
    python compare_tables.py Books --databases db1 db2 merged
    
    # Compare Books and Books1 within db1 only
    python compare_tables.py Books Books1 --database db1
    
    # Find duplicates between db1 and db2 for all tables
    python compare_tables.py --find-duplicates
    
    # Verify merged database contains all data from sources
    python compare_tables.py --verify-merge
"""

import sys
import argparse
from pathlib import Path
from typing import List, Optional, Dict
import pandas as pd

from database_utils.config import get_path
from database_utils.comparison import load_table_pair, compare_row_sets, find_missing_rows
from database_utils.io import load_csv
from database_utils.align import align_columns, align_two
from database_utils.reporting import (
    print_section,
    print_comparison_stats,
    print_table_summary,
    print_error,
    print_warning,
    print_success
)


def compare_across_databases(
    table_name: str,
    databases: List[str]
) -> None:
    """
    Compare the same table across multiple databases.
    
    Args:
        table_name: Name of the table to compare
        databases: List of database keys (e.g., ['db1', 'db2', 'merged'])
    """
    print_section(f"Cross-Database Comparison: {table_name}")
    
    csv_name = f"{table_name}.csv"
    
    # Load tables from all databases
    dataframes = {}
    for db_key in databases:
        db_path = get_path(db_key)
        df = load_csv(db_path / csv_name)
        if df is not None:
            dataframes[db_key] = df
            print(f"   {db_key}: {len(df)} rows")
        else:
            print_warning(f"Table not found in {db_key}")
    
    if len(dataframes) < 2:
        print_error("Need at least 2 databases with this table for comparison")
        return
    
    # If comparing 3 databases (typical: db1, db2, merged)
    if len(dataframes) == 3 and 'merged' in dataframes:
        db_keys = [k for k in databases if k != 'merged']
        if len(db_keys) == 2:
            compare_with_merged(
                table_name,
                dataframes[db_keys[0]],
                dataframes[db_keys[1]],
                dataframes['merged'],
                db_keys[0],
                db_keys[1]
            )
    else:
        # General pairwise comparison
        db_list = list(dataframes.keys())
        for i in range(len(db_list)):
            for j in range(i + 1, len(db_list)):
                compare_two_tables(
                    table_name,
                    dataframes[db_list[i]],
                    dataframes[db_list[j]],
                    db_list[i],
                    db_list[j]
                )


def compare_with_merged(
    table_name: str,
    df1: pd.DataFrame,
    df2: pd.DataFrame,
    df_merged: pd.DataFrame,
    label1: str,
    label2: str
) -> None:
    """
    Compare two source databases and their merge result.
    
    Args:
        table_name: Name of the table
        df1, df2: Source dataframes
        df_merged: Merged dataframe
        label1, label2: Labels for source databases
    """
    print(f"\nAnalyzing merge from {label1} and {label2}:")
    
    # Align columns
    df1, df2, df_merged = align_columns([df1, df2, df_merged])
    
    # Compare row sets
    stats = compare_row_sets(df1, df2, df_merged)
    print_comparison_stats(stats)
    
    # Calculate overlap
    both = stats["from_df1"] + stats["from_df2"] - stats["total"]
    if both < 0:
        both = 0
    
    print(f"\nOverlap Analysis:")
    print(f"   In both {label1} and {label2}: {both}")
    print(f"   Only in {label1}: {stats['from_df1'] - both}")
    print(f"   Only in {label2}: {stats['from_df2'] - both}")
    print(f"   Unique to merged: {stats['unique']}")
    
    # Check for data loss
    missing_from_1 = find_missing_rows(df1, df_merged)
    missing_from_2 = find_missing_rows(df2, df_merged)
    
    if missing_from_1 > 0 or missing_from_2 > 0:
        print_error(f"Data loss detected!")
        if missing_from_1 > 0:
            print(f"   Missing from {label1}: {missing_from_1} rows")
        if missing_from_2 > 0:
            print(f"   Missing from {label2}: {missing_from_2} rows")
    else:
        print_success("No data loss - all rows accounted for")


def compare_two_tables(
    context: str,
    df1: pd.DataFrame,
    df2: pd.DataFrame,
    label1: str,
    label2: str
) -> None:
    """
    Compare two tables and show differences.
    
    Args:
        context: Description of comparison (table name or context)
        df1, df2: Dataframes to compare
        label1, label2: Labels for the dataframes
    """
    print(f"\n{context}: {label1} vs {label2}")
    
    # Align columns
    df1, df2 = align_two(df1, df2)
    
    # Find duplicates and unique rows
    duplicates = df1.merge(df2, how="inner")
    only_in_1 = len(df1) - len(duplicates)
    only_in_2 = len(df2) - len(duplicates)
    
    print(f"   {label1}: {len(df1)} rows")
    print(f"   {label2}: {len(df2)} rows")
    print(f"   Duplicate rows: {len(duplicates)}")
    print(f"   Only in {label1}: {only_in_1}")
    print(f"   Only in {label2}: {only_in_2}")


def compare_within_database(
    table1_name: str,
    table2_name: str,
    database: str
) -> None:
    """
    Compare two different tables within the same database.
    
    Args:
        table1_name: Name of first table
        table2_name: Name of second table
        database: Database key to use
    """
    print_section(f"Within-Database Comparison: {table1_name} vs {table2_name}")
    print(f"Database: {database}\n")
    
    db_path = get_path(database)
    
    # Load both tables
    df1 = load_csv(db_path / f"{table1_name}.csv")
    df2 = load_csv(db_path / f"{table2_name}.csv")
    
    if df1 is None:
        print_error(f"Table not found: {table1_name}")
        return
    if df2 is None:
        print_error(f"Table not found: {table2_name}")
        return
    
    compare_two_tables(
        f"{table1_name} vs {table2_name}",
        df1,
        df2,
        table1_name,
        table2_name
    )


def find_all_duplicates() -> None:
    """Find duplicate rows between db1 and db2 for all tables."""
    print_section("Finding Duplicates Between Databases")
    
    db1_path = get_path("db1")
    db2_path = get_path("db2")
    output_dir = get_path("duplicates")
    output_dir.mkdir(exist_ok=True)
    
    print(f"Database 1: {db1_path}")
    print(f"Database 2: {db2_path}")
    print(f"Output: {output_dir}\n")
    
    total_duplicates = 0
    tables_with_duplicates = 0
    
    # Check each table
    for file1 in sorted(db1_path.glob("*.csv")):
        file2 = db2_path / file1.name
        
        if not file2.exists():
            continue
        
        # Load tables
        df1 = load_csv(file1)
        df2 = load_csv(file2)
        
        if df1 is None or df2 is None:
            continue
        
        # Find duplicates
        duplicates = df1.merge(df2, how="inner")
        
        if len(duplicates) > 0:
            # Save duplicates
            output_file = output_dir / f"duplicates_{file1.name}"
            duplicates.to_csv(output_file, index=False)
            print_success(f"{file1.name}: {len(duplicates)} duplicates (saved)")
            
            total_duplicates += len(duplicates)
            tables_with_duplicates += 1
        else:
            print(f"{file1.name}: No duplicates")
    
    print(f"\nSummary:")
    print(f"   Tables with duplicates: {tables_with_duplicates}")
    print(f"   Total duplicate rows: {total_duplicates}")


def verify_merge() -> None:
    """Verify that merged database contains all data from sources."""
    print_section("Merge Verification")
    print("Checking that merged database contains all data from sources\n")
    
    db1_path = get_path("db1")
    db2_path = get_path("db2")
    merged_path = get_path("merged")
    
    for db_path, db_label in [(db1_path, "db1"), (db2_path, "db2")]:
        print_section(f"Verifying {db_label.upper()}")
        
        total_missing = 0
        tables_with_issues = 0
        tables_checked = 0
        
        for table_file in sorted(db_path.glob("*.csv")):
            merged_file = merged_path / table_file.name
            
            if not merged_file.exists():
                print_error(f"Not found in merged", table_file.name)
                tables_with_issues += 1
                continue
            
            # Load tables
            source_df = load_csv(table_file)
            merged_df = load_csv(merged_file)
            
            if source_df is None or merged_df is None:
                continue
            
            # Align and compare
            source_df, merged_df = align_two(source_df, merged_df)
            missing = find_missing_rows(source_df, merged_df)
            
            print_table_summary(
                table_file.name,
                len(source_df),
                len(merged_df),
                missing
            )
            
            if missing > 0:
                total_missing += missing
                tables_with_issues += 1
            
            tables_checked += 1
        
        print(f"\nSummary for {db_label}:")
        print(f"   Tables checked: {tables_checked}")
        print(f"   Tables with missing rows: {tables_with_issues}")
        print(f"   Total missing unique rows: {total_missing}")
        
        if total_missing == 0:
            print_success(f"All data from {db_label} preserved in merge")


def compare_all_tables(databases: List[str]) -> None:
    """Compare all tables across specified databases."""
    # Get all unique table names
    table_names = set()
    for db_key in databases:
        db_path = get_path(db_key)
        table_names.update(p.stem for p in db_path.glob("*.csv"))
    
    print_section(f"Comparing All Tables Across: {', '.join(databases)}")
    print(f"Found {len(table_names)} unique tables\n")
    
    for table_name in sorted(table_names):
        compare_across_databases(table_name, databases)
        print()


def main():
    """Main entry point for table comparison."""
    parser = argparse.ArgumentParser(
        description="Universal table comparison tool for database analysis",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Compare Books across all databases
  python compare_tables.py Books --databases db1 db2 merged
  
  # Compare Books and Books1 within db1
  python compare_tables.py Books Books1 --database db1
  
  # Compare all tables across db1 and db2
  python compare_tables.py --all --databases db1 db2
  
  # Find all duplicates between db1 and db2
  python compare_tables.py --find-duplicates
  
  # Verify merge integrity
  python compare_tables.py --verify-merge
        """
    )
    
    parser.add_argument(
        "tables",
        nargs="*",
        help="Table name(s) to compare (without .csv extension)"
    )
    
    parser.add_argument(
        "--database",
        help="Single database to use for within-database comparison"
    )
    
    parser.add_argument(
        "--databases",
        nargs="+",
        help="Multiple databases for cross-database comparison"
    )
    
    parser.add_argument(
        "--all",
        action="store_true",
        help="Compare all tables"
    )
    
    parser.add_argument(
        "--find-duplicates",
        action="store_true",
        help="Find duplicate rows between db1 and db2"
    )
    
    parser.add_argument(
        "--verify-merge",
        action="store_true",
        help="Verify merged database integrity"
    )
    
    args = parser.parse_args()
    
    # Handle special operations
    if args.find_duplicates:
        find_all_duplicates()
        return
    
    if args.verify_merge:
        verify_merge()
        return
    
    # Handle table comparisons
    if args.all:
        if not args.databases:
            print_error("--all requires --databases to be specified")
            sys.exit(1)
        compare_all_tables(args.databases)
        return
    
    if not args.tables:
        parser.print_help()
        sys.exit(1)
    
    # Remove .csv extension if provided
    tables = [t.removesuffix(".csv") for t in args.tables]
    
    # Within-database comparison (two different tables in same database)
    if len(tables) == 2 and args.database:
        compare_within_database(tables[0], tables[1], args.database)
    
    # Cross-database comparison (same table across multiple databases)
    elif len(tables) == 1 and args.databases:
        compare_across_databases(tables[0], args.databases)
    
    # Default: compare across db1, db2, merged
    elif len(tables) == 1:
        compare_across_databases(tables[0], ["db1", "db2", "merged"])
    
    else:
        print_error("Invalid combination of arguments")
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()