"""
Inspect CSV files for embedded newline characters.

This script helps diagnose data quality issues by detecting rows
that contain embedded newline characters (\n or \r), which can
cause CSV parsing problems.
"""
import sys
from pathlib import Path
import pandas as pd
from database_utils.reporting import (
    print_section,
    print_error,
    print_warning,
    print_success
)


def inspect_for_newlines(file_path: Path) -> None:
    """
    Inspect a CSV file for rows containing embedded newlines.
    
    This function:
    1. Loads the CSV and counts DataFrame rows
    2. Counts physical lines in the file
    3. Identifies rows with embedded newlines
    4. Displays detailed information about problematic rows
    
    Args:
        file_path: Path to CSV file to inspect
    """
    # Check if file exists
    if not file_path.exists():
        print_error(f"File not found: {file_path}")
        return

    print_section(f"Newline Inspection: {file_path.name}")
    
    # Load CSV into DataFrame
    try:
        df = pd.read_csv(file_path, dtype=str)
        print(f"DataFrame rows: {len(df)}")
    except Exception as e:
        print_error(f"Failed to load CSV: {e}")
        return

    # Count physical lines in file
    try:
        with file_path.open("r", encoding="utf-8", errors="ignore") as f:
            physical_lines = sum(1 for _ in f)
        print(f"Physical file lines: {physical_lines}")
    except Exception as e:
        print_error(f"Failed to read file: {e}")
        return

    # Detect rows with embedded newlines
    has_newlines = df.apply(
        lambda col: col.str.contains(r"\r|\n", na=False),
        axis=0,
    ).any(axis=1)
    
    rows_with_newlines = df[has_newlines]
    problematic_count = len(rows_with_newlines)
    
    # Report findings
    if problematic_count == 0:
        print_success("No embedded newlines found")
        return
    
    print_warning(f"Found {problematic_count} rows with embedded newlines")
    
    # Get indices of problematic rows
    bad_indices = rows_with_newlines.index.tolist()
    print(f"\nProblematic row indices: {bad_indices}")
    
    # Display problematic rows in detail
    print("\nDetailed inspection of problematic rows:")
    print("=" * 80)
    
    for idx in bad_indices:
        print(f"\n--- Row {idx} ---")
        
        # Show each column value with repr() to reveal hidden characters
        for col, val in df.loc[idx].items():
            if pd.notna(val) and ("\n" in str(val) or "\r" in str(val)):
                print(f"{col:20} => {repr(val)}")
        
        print("-" * 80)


def main():
    """Main entry point for newline inspection."""
    # Determine which file to inspect
    if len(sys.argv) > 1:
        # File specified as command line argument
        file_path = Path(sys.argv[1])
    else:
        # Default to Authors.csv in merged directory
        file_path = Path("merged_csv/Authors.csv")
        print(f"No file specified, using default: {file_path}")
    
    # Inspect the file
    inspect_for_newlines(file_path)


if __name__ == "__main__":
    main()