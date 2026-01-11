"""
Enhanced normalization utilities with column mapping support.
"""
import pandas as pd
from typing import Dict


def normalize_value(v) -> str:
    """
    Normalize a single value: handle NaN, strip, remove newlines, lowercase.
    """
    if pd.isna(v):
        return ""
    return (
        str(v)
        .replace("\r", " ")
        .replace("\n", " ")
        .strip()
        .lower()
    )


def normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply normalize_value to all cells of a DataFrame.
    """
    df = df.fillna("")
    return df.map(normalize_value)


def add_normalized_columns(df: pd.DataFrame, column_map: Dict[str, str]) -> pd.DataFrame:
    """
    Add normalized versions of specified columns to a DataFrame.
    
    Args:
        df: DataFrame to process
        column_map: Dict mapping {original_column: normalized_column_name}
        
    Returns:
        DataFrame with additional normalized columns
        
    Example:
        column_map = {"ISBN": "ISBN_norm", "Title": "Title_norm"}
        df = add_normalized_columns(df, column_map)
    """
    df = df.copy()
    for raw_col, norm_col in column_map.items():
        if raw_col in df.columns:
            df[norm_col] = df[raw_col].apply(normalize_value)
    return df


def get_normalized_column_names(column_map: Dict[str, str]) -> list:
    """
    Get list of normalized column names from a column map.
    
    Args:
        column_map: Dict mapping {original_column: normalized_column_name}
        
    Returns:
        List of normalized column names
    """
    return list(column_map.values())