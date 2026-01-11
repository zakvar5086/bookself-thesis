"""
Utilities for comparing tables and finding matches.
"""
import pandas as pd
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from .io import load_csv
from .hashing import row_hash


def load_table_pair(
    table_name: str,
    path1: Path,
    path2: Path,
    normalize: bool = True
) -> Tuple[Optional[pd.DataFrame], Optional[pd.DataFrame]]:
    """
    Load a table from two different database directories.
    
    Args:
        table_name: Name of the table (e.g., "Books.csv")
        path1: First database directory
        path2: Second database directory
        normalize: Whether to normalize the data
        
    Returns:
        Tuple of (df1, df2), either may be None if file doesn't exist
    """
    file1 = path1 / table_name
    file2 = path2 / table_name
    
    df1 = load_csv(file1, normalize=normalize) if file1.exists() else None
    df2 = load_csv(file2, normalize=normalize) if file2.exists() else None
    
    return df1, df2


def find_exact_matches(
    df1: pd.DataFrame,
    df2: pd.DataFrame,
    merge_keys: List[str],
    suffixes: Tuple[str, str] = ("_1", "_2"),
    filter_empty: bool = True
) -> pd.DataFrame:
    """
    Find exact matches between two DataFrames based on merge keys.
    
    Args:
        df1: First DataFrame
        df2: Second DataFrame
        merge_keys: List of column names to merge on
        suffixes: Tuple of suffixes for overlapping columns
        filter_empty: If True, remove rows where any merge key is empty
        
    Returns:
        DataFrame of matched rows
    """
    # Perform inner join on merge keys
    matches = pd.merge(
        df1,
        df2,
        on=merge_keys,
        how="inner",
        suffixes=suffixes
    )
    
    # Filter out rows with empty merge key values if requested
    if filter_empty:
        matches = matches[(matches[merge_keys] != "").all(axis=1)]
    
    return matches


def compare_row_sets(
    df1: pd.DataFrame,
    df2: pd.DataFrame,
    df3: pd.DataFrame,
    common_columns: Optional[List[str]] = None
) -> Dict[str, int]:
    """
    Compare row sets across three DataFrames using row hashing.
    
    Args:
        df1: First DataFrame
        df2: Second DataFrame  
        df3: Third DataFrame (typically merged)
        common_columns: List of columns to compare (uses intersection if None)
        
    Returns:
        Dict with counts: total, from_df1, from_df2, unique
    """
    # Find common columns if not provided
    if common_columns is None:
        sets = [set(df.columns) for df in [df1, df2, df3]]
        common_columns = list(set.intersection(*sets))
    
    # Create hash sets for each DataFrame
    def get_hashes(df):
        return {row_hash(row) for _, row in df[common_columns].iterrows()}
    
    hashes_1 = get_hashes(df1)
    hashes_2 = get_hashes(df2)
    hashes_3 = get_hashes(df3)
    
    return {
        "total": len(hashes_3),
        "from_df1": len(hashes_3 & hashes_1),
        "from_df2": len(hashes_3 & hashes_2),
        "unique": len(hashes_3 - hashes_1 - hashes_2)
    }


def find_missing_rows(
    source_df: pd.DataFrame,
    target_df: pd.DataFrame,
    common_columns: Optional[List[str]] = None
) -> int:
    """
    Find how many unique rows from source are missing in target.
    
    Args:
        source_df: Source DataFrame
        target_df: Target DataFrame
        common_columns: Columns to compare (uses intersection if None)
        
    Returns:
        Count of missing unique rows
    """
    # Find common columns if not provided
    if common_columns is None:
        common_columns = list(set(source_df.columns) & set(target_df.columns))
    
    # Convert rows to tuples for comparison
    source_rows = source_df[common_columns].apply(
        lambda r: tuple(r.values), axis=1
    ).value_counts()
    
    target_rows = target_df[common_columns].apply(
        lambda r: tuple(r.values), axis=1
    ).value_counts()
    
    # Count unique rows in source that don't exist in target
    missing = sum(1 for row in source_rows.index if target_rows.get(row, 0) == 0)
    
    return missing