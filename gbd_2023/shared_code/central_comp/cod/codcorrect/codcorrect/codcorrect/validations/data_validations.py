from typing import Any, List, Optional

import pandas as pd


def check_duplicates(df: pd.DataFrame, subset: Optional[List[Any]] = None) -> None:
    """Validates there are no duplicates within columns in subset."""
    if df.duplicated(keep=False, subset=subset).any():
        raise RuntimeError(
            f"There are duplicates in the data\n{df[df.duplicated(keep=False)]}"
        )


def one_row_returned(sql_results: pd.DataFrame) -> bool:
    """Returns whether the sql_results returned a single row."""
    return sql_results.shape[0] == 1
