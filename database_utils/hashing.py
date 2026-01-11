import hashlib
import pandas as pd


def row_hash(row: pd.Series) -> str:
    """
    Stable hash over the row values using MD5.
    Used for comparing rows across tables.
    """
    s = "||".join([str(x) for x in row.values])
    return hashlib.md5(s.encode()).hexdigest()