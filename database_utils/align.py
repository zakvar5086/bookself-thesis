from typing import List, Tuple
import pandas as pd


def align_columns(dfs: List[pd.DataFrame]) -> List[pd.DataFrame]:
    """
    Given a list of DataFrames, align them to have the same set of columns.
    Missing columns are filled with empty strings.
    """
    if not dfs:
        return []

    all_cols = sorted({col for df in dfs for col in df.columns})
    aligned = []

    for df in dfs:
        for col in all_cols:
            if col not in df.columns:
                df[col] = ""
        aligned.append(df[all_cols])

    return aligned


def align_two(df_a: pd.DataFrame, df_b: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Align two DataFrames to the same column set.
    """
    return tuple(align_columns([df_a, df_b]))  # type: ignore[return-value]