from typing import List, Tuple

import pandas as pd

from gbd.constants import tool_types


def draw_column_list(n_draws: int) -> List[str]:
    """Returns a numerically-ordered list of n_draws column names."""
    return [f"draw_{i}" for i in range(n_draws)]


def get_index_draw_columns(df: pd.DataFrame) -> Tuple[List[str], List[str]]:
    """
    Return a list of index columns and draw columns in this dataframe.

    Any column beginning with 'draw_' is assumed to be a non-index column,
    everything else is an index. Draw columns are returned in numerical order.
    Index columns are returned in order determined by list.sort()

    Args:
      df (dataframe): The dataframe on which to extract the index column names

    Returns:
      index_columns, draw_columns: The column names split into index and draw type.

    """
    draw_columns = list(df.filter(like="draw_").columns)
    draw_columns.sort(key=lambda X: int(X.rsplit("_", 1)[1]))
    index_columns = [X for X in df.columns if X not in draw_columns]
    index_columns.sort()
    return index_columns, draw_columns


def standard_frame_sort(df: pd.DataFrame) -> pd.DataFrame:
    """Consistently sort rows and columns."""
    index_cols, draw_cols = get_index_draw_columns(df)
    output = df[index_cols + draw_cols].sort_values(by=index_cols).reset_index(drop=True)
    return output
