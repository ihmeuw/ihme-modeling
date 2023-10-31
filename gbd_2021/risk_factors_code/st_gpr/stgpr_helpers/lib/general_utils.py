"""General helper functions."""
from typing import Any, List, Optional

import pandas as pd

from stgpr_helpers.lib.constants import columns


def nan_to_none(df: pd.DataFrame) -> pd.DataFrame:
    """Converts NaNs in a DataFrame to Nones."""
    return df.where((pd.notnull(df)), None)


def join_or_none(to_join: List[Any]) -> Optional[str]:
    """Returns a joined string or None if `to_join` is None."""
    return ",".join([str(item) for item in to_join]) if to_join else None


def sort_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Arranges columns starting with demographics then sorts by demographic values."""
    return df[
        columns.DEMOGRAPHICS + [col for col in df if col not in columns.DEMOGRAPHICS]
    ].sort_values(columns.DEMOGRAPHICS)


def reset_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """Resets DataFrame column names after a pivot operation."""
    df.columns.rename(None, inplace=True)
    return df
