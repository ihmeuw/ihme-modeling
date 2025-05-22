"""
Wraps pandas groupby method and validates that values to sum are not lost after aggregation
"""

from typing import List

import pandas as pd


def sum_marketscan(df: pd.DataFrame, agg_cols: List[str], sum_cols: List[str]) -> pd.DataFrame:
    """Perform a groupby sum on Marketcan data"""
    pre_sums = {}
    for col in sum_cols:
        pre_sums[col] = df[col].sum()

    df = (
        df.groupby(agg_cols, dropna=False)
        .agg(dict(zip(sum_cols, ["sum"] * len(sum_cols))))
        .reset_index()
    )

    for col in sum_cols:
        diff = pre_sums[col] - df[col].sum()
        if diff != 0:
            raise ValueError(f"{diff} values in the {col} column have been lost.")
    return df