import pandas as pd
from typing import Any, List, Optional

def check_duplicates(
    df: pd.DataFrame,
    subset: Optional[List[Any]]=None
) -> None:
    if df.duplicated(keep=False, subset=subset).any():
        raise RuntimeError(
            "There are duplicates in the data\n"
            f"{df[df.duplicated(keep=False)]}"
        )