"""
Universal table matching tool.

This script provides flexible table matching capabilities with support for:
- Exact matching on specified columns
- Fuzzy matching on text columns
- Configurable matching strategies
- Works with any database and any table pair

Usage examples:
    # Match Books and Books1 using ISBN+Title+Publisher
    python match_tables.py Books Books1 --database merged \\
        --exact-match ISBN Title Publisher
    
    # Match with fuzzy title matching
    python match_tables.py Books Books1 --database db1 \\
        --fuzzy-match Title --threshold 85
    
    # Match Journals and OldJournals on ISSN and date fields
    python match_tables.py Journals OldJournals --database merged \\
        --exact-match ISSN StartMonth EndMonth Year
    
    # Use predefined matching strategy
    python match_tables.py Books Books1 --database merged --strategy books
"""

import sys
import argparse
from pathlib import Path
from typing import List, Optional, Dict
import pandas as pd

from database_utils.config import get_path, get_config_value
from database_utils.io import load_csv
from database_utils.normalize import add_normalized_columns, get_normalized_column_names
from database_utils.comparison import find_exact_matches
from database_utils.fuzzy import fuzzy_title_score, confidence_label
from database_utils.reporting import (
    print_section,
    print_match_summary,
    print_success,
    print_error,
    print_warning
)


# Predefined matching strategies for common table pairs
MATCHING_STRATEGIES = {
    "books": {
        "description": "Match books by ISBN, Title, and Publisher",
        "exact_columns": ["ISBN", "Title", "Publisher"],
        "fuzzy_columns": ["Title"],
        "fuzzy_match_on": "Publisher"  # Same publisher required for fuzzy match
    },
    "journals": {
        "description": "Match journals by ISSN and publication period",
        "exact_columns": ["ISSN", "StartMonth", "EndMonth", "Year"],
        "fuzzy_columns": None
    },
    "authors": {
        "description": "Match authors by name",
        "exact_columns": ["FirstName", "LastName"],
        "fuzzy_columns": ["FirstName", "LastName"]
    }
}


def prepare_dataframes(
    df1: pd.DataFrame,
    df2: pd.DataFrame,
    columns: List[str]
) -> tuple:
    """
    Prepare dataframes for matching by normalizing specified columns.
    
    Args:
        df1, df2: Dataframes to prepare
        columns: Columns to normalize
        
    Returns:
        Tuple of (df1, df2, norm_map)
    """
    # Create normalization mapping
    norm_map = {col: f"{col}_norm" for col in columns}
    
    # Add normalized columns
    df1 = add_normalized_columns(df1, norm_map)
    df2 = add_normalized_columns(df2, norm_map)
    
    return df1, df2, norm_map


def find_exact_column_matches(
    df1: pd.DataFrame,
    df2: pd.DataFrame,
    columns: List[str],
    label1: str,
    label2: str,
    output_dir: Path
) -> pd.DataFrame:
    """
    Find exact matches based on specified columns.
    
    Args:
        df1, df2: Dataframes to match
        columns: Columns to match on
        label1, label2: Labels for the dataframes
        output_dir: Directory to save results
        
    Returns:
        DataFrame of exact matches
    """
    # Prepare dataframes
    df1, df2, norm_map = prepare_dataframes(df1, df2, columns)
    
    # Get normalized column names
    merge_keys = get_normalized_column_names(norm_map)
    
    # Find exact matches
    matches = find_exact_matches(
        df1,
        df2,
        merge_keys=merge_keys,
        suffixes=(f"_{label1}", f"_{label2}"),
        filter_empty=True
    )
    
    # Add metadata
    matches["Confidence"] = f"Exact {'+'.join(columns)}"
    matches["MatchType"] = "Exact"
    
    return matches


def find_fuzzy_column_matches(
    df1: pd.DataFrame,
    df2: pd.DataFrame,
    fuzzy_column: str,
    match_on: Optional[str],
    threshold: float,
    label1: str,
    label2: str,
    output_dir: Path
) -> pd.DataFrame:
    """
    Find fuzzy matches based on text similarity.
    
    Args:
        df1, df2: Dataframes to match
        fuzzy_column: Column to perform fuzzy matching on
        match_on: Optional column that must match exactly (e.g., Publisher)
        threshold: Minimum similarity score (0-100)
        label1, label2: Labels for the dataframes
        output_dir: Directory to save results
        
    Returns:
        DataFrame of fuzzy matches above threshold
    """
    # Prepare dataframes for fuzzy column
    df1, df2, fuzzy_map = prepare_dataframes(df1, df2, [fuzzy_column])
    fuzzy_col_norm = f"{fuzzy_column}_norm"
    
    # If there's a column to match on, prepare it too
    if match_on:
        df1, df2, match_map = prepare_dataframes(df1, df2, [match_on])
        match_col_norm = f"{match_on}_norm"
        
        # Join on the match column to reduce comparison space
        pairs = df1.merge(
            df2,
            on=match_col_norm,
            suffixes=(f"_{label1}", f"_{label2}")
        )
    else:
        # Cross join (all combinations)
        df1['_key'] = 1
        df2['_key'] = 1
        pairs = df1.merge(df2, on='_key', suffixes=(f"_{label1}", f"_{label2}"))
        pairs = pairs.drop('_key', axis=1)
    
    if len(pairs) == 0:
        return pd.DataFrame()
    
    # Calculate fuzzy scores
    pairs["similarity_score"] = pairs.apply(
        lambda row: fuzzy_title_score(
            row[f"{fuzzy_col_norm}_{label1}"],
            row[f"{fuzzy_col_norm}_{label2}"]
        ),
        axis=1
    )
    
    # Add confidence labels
    pairs["Confidence"] = pairs["similarity_score"].apply(confidence_label)
    pairs["MatchType"] = "Fuzzy"
    
    # Filter to matches above threshold
    high_confidence = pairs[pairs["similarity_score"] >= threshold]
    
    return high_confidence


def match_tables_with_strategy(
    table1_name: str,
    table2_name: str,
    database: str,
    strategy: str,
    output_dir: Path
) -> None:
    """
    Match two tables using a predefined strategy.
    
    Args:
        table1_name: Name of first table
        table2_name: Name of second table
        database: Database key
        strategy: Name of predefined strategy
        output_dir: Directory to save results
    """
    if strategy not in MATCHING_STRATEGIES:
        print_error(f"Unknown strategy: {strategy}")
        print(f"Available strategies: {', '.join(MATCHING_STRATEGIES.keys())}")
        sys.exit(1)
    
    strat = MATCHING_STRATEGIES[strategy]
    print_section(f"Matching with '{strategy}' Strategy")
    print(f"Description: {strat['description']}\n")
    
    # Load tables
    db_path = get_path(database)
    df1 = load_csv(db_path / f"{table1_name}.csv", normalize=False)
    df2 = load_csv(db_path / f"{table2_name}.csv", normalize=False)
    
    if df1 is None or df2 is None:
        print_error("Failed to load one or both tables")
        return
    
    # Fill NaN values
    df1 = df1.fillna("")
    df2 = df2.fillna("")
    
    print(f"   {table1_name}: {len(df1)} rows")
    print(f"   {table2_name}: {len(df2)} rows\n")
    
    # Exact matching
    if strat["exact_columns"]:
        print(f"Finding exact matches on: {', '.join(strat['exact_columns'])}")
        exact_matches = find_exact_column_matches(
            df1, df2,
            strat["exact_columns"],
            table1_name,
            table2_name,
            output_dir
        )
        
        output_file = output_dir / f"{table1_name}_{table2_name}_exact.csv"
        exact_matches.to_csv(output_file, index=False)
        print_match_summary(
            "Exact Matches",
            len(exact_matches),
            output_file
        )
    
    # Fuzzy matching
    if strat["fuzzy_columns"]:
        threshold = get_config_value("fuzzy", "score_threshold")
        fuzzy_col = strat["fuzzy_columns"][0]  # Use first fuzzy column
        match_on = strat.get("fuzzy_match_on")
        
        print(f"\nFinding fuzzy matches on: {fuzzy_col}")
        if match_on:
            print(f"   Requiring exact match on: {match_on}")
        
        fuzzy_matches = find_fuzzy_column_matches(
            df1, df2,
            fuzzy_col,
            match_on,
            threshold,
            table1_name,
            table2_name,
            output_dir
        )
        
        output_file = output_dir / f"{table1_name}_{table2_name}_fuzzy.csv"
        fuzzy_matches.to_csv(output_file, index=False)
        print_match_summary(
            f"Fuzzy Matches (>= {threshold})",
            len(fuzzy_matches),
            output_file
        )


def match_tables_custom(
    table1_name: str,
    table2_name: str,
    database: str,
    exact_columns: Optional[List[str]],
    fuzzy_column: Optional[str],
    fuzzy_threshold: float,
    output_dir: Path
) -> None:
    """
    Match two tables using custom column specifications.
    
    Args:
        table1_name: Name of first table
        table2_name: Name of second table
        database: Database key
        exact_columns: Columns for exact matching
        fuzzy_column: Column for fuzzy matching
        fuzzy_threshold: Minimum similarity score
        output_dir: Directory to save results
    """
    print_section(f"Custom Matching: {table1_name} vs {table2_name}")
    print(f"Database: {database}\n")
    
    # Load tables
    db_path = get_path(database)
    df1 = load_csv(db_path / f"{table1_name}.csv", normalize=False)
    df2 = load_csv(db_path / f"{table2_name}.csv", normalize=False)
    
    if df1 is None or df2 is None:
        print_error("Failed to load one or both tables")
        return
    
    # Fill NaN values
    df1 = df1.fillna("")
    df2 = df2.fillna("")
    
    print(f"   {table1_name}: {len(df1)} rows")
    print(f"   {table2_name}: {len(df2)} rows\n")
    
    # Exact matching
    if exact_columns:
        # Verify columns exist
        missing_cols = []
        for col in exact_columns:
            if col not in df1.columns:
                missing_cols.append(f"{col} (in {table1_name})")
            if col not in df2.columns:
                missing_cols.append(f"{col} (in {table2_name})")
        
        if missing_cols:
            print_error(f"Missing columns: {', '.join(missing_cols)}")
            return
        
        print(f"Finding exact matches on: {', '.join(exact_columns)}")
        exact_matches = find_exact_column_matches(
            df1, df2,
            exact_columns,
            table1_name,
            table2_name,
            output_dir
        )
        
        output_file = output_dir / f"{table1_name}_{table2_name}_exact.csv"
        exact_matches.to_csv(output_file, index=False)
        print_match_summary(
            "Exact Matches",
            len(exact_matches),
            output_file
        )
    
    # Fuzzy matching
    if fuzzy_column:
        # Verify column exists
        if fuzzy_column not in df1.columns:
            print_error(f"Column '{fuzzy_column}' not found in {table1_name}")
            return
        if fuzzy_column not in df2.columns:
            print_error(f"Column '{fuzzy_column}' not found in {table2_name}")
            return
        
        print(f"\nFinding fuzzy matches on: {fuzzy_column} (threshold >= {fuzzy_threshold})")
        fuzzy_matches = find_fuzzy_column_matches(
            df1, df2,
            fuzzy_column,
            None,  # No match_on column for custom matching
            fuzzy_threshold,
            table1_name,
            table2_name,
            output_dir
        )
        
        output_file = output_dir / f"{table1_name}_{table2_name}_fuzzy.csv"
        fuzzy_matches.to_csv(output_file, index=False)
        print_match_summary(
            f"Fuzzy Matches (>= {fuzzy_threshold})",
            len(fuzzy_matches),
            output_file
        )


def main():
    """Main entry point for table matching."""
    parser = argparse.ArgumentParser(
        description="Universal table matching tool with exact and fuzzy matching",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Match Books and Books1 using predefined strategy
  python match_tables.py Books Books1 --database merged --strategy books
  
  # Match with custom exact columns
  python match_tables.py Books Books1 --database db1 \\
      --exact-match ISBN Title Publisher
  
  # Match with fuzzy matching
  python match_tables.py Books Books1 --database merged \\
      --fuzzy-match Title --threshold 85
  
  # Match Journals using predefined strategy
  python match_tables.py Journals OldJournals --database merged \\
      --strategy journals

Available strategies: books, journals, authors
        """
    )
    
    parser.add_argument(
        "table1",
        help="First table name (without .csv extension)"
    )
    
    parser.add_argument(
        "table2",
        help="Second table name (without .csv extension)"
    )
    
    parser.add_argument(
        "--database",
        default="merged",
        help="Database to use (default: merged)"
    )
    
    parser.add_argument(
        "--strategy",
        choices=list(MATCHING_STRATEGIES.keys()),
        help="Use a predefined matching strategy"
    )
    
    parser.add_argument(
        "--exact-match",
        nargs="+",
        metavar="COLUMN",
        help="Columns for exact matching"
    )
    
    parser.add_argument(
        "--fuzzy-match",
        metavar="COLUMN",
        help="Column for fuzzy text matching"
    )
    
    parser.add_argument(
        "--threshold",
        type=float,
        default=80.0,
        help="Fuzzy match threshold 0-100 (default: 80)"
    )
    
    parser.add_argument(
        "--output-dir",
        help="Custom output directory (default: matched_results)"
    )
    
    args = parser.parse_args()
    
    # Remove .csv extension if provided
    table1 = args.table1.removesuffix(".csv")
    table2 = args.table2.removesuffix(".csv")
    
    # Setup output directory
    if args.output_dir:
        output_dir = Path(args.output_dir)
    else:
        output_dir = get_path("matched_results")
    output_dir.mkdir(exist_ok=True)
    
    print(f"Output directory: {output_dir}\n")
    
    # Use strategy or custom matching
    if args.strategy:
        match_tables_with_strategy(
            table1,
            table2,
            args.database,
            args.strategy,
            output_dir
        )
    elif args.exact_match or args.fuzzy_match:
        match_tables_custom(
            table1,
            table2,
            args.database,
            args.exact_match,
            args.fuzzy_match,
            args.threshold,
            output_dir
        )
    else:
        print_error("Must specify either --strategy or --exact-match/--fuzzy-match")
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()