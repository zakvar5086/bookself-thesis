from pathlib import Path
from typing import Optional, Union

import pandas as pd
from .normalize import normalize_df

PathLike = Union[str, Path]


def load_csv(path: PathLike, normalize: bool = True) -> Optional[pd.DataFrame]:
    path = Path(path)
    try:
        df = pd.read_csv(path, dtype=str)
        if normalize:
            df = normalize_df(df)
        return df
    except Exception as e:
        print(f"[ERROR] Cannot read {path}: {e}")
        return None