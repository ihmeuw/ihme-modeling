"""
There is some inconsistency, mostly in Medicaid, where a benes race is recorded as both
and unknown and a known value. Sometimes this occurs across years, sometimes within a year

In order to resolve we've developed a 3 step algorithm. This module contains the functions
to perform these fill methods

1) back fill (non-unknown race codes from the most recent years will be preferred)
2) forward fill (non-unknown race codes from the oldest years will be fill forward)
3) cross fill (rows with unknown race codes will be merged between systems and filled)
"""
import warnings
from typing import List

import numpy as np
import pandas as pd


def swap_unknown(df: pd.DataFrame, col: str) -> pd.DataFrame:
    """swap between null and unknown vals"""
    warnings.warn(
        "This function is using the code '0' to represent unknown values"
        " if another column is used this may need to change"
    )

    if df[col].isnull().any():
        df.loc[df[col].isnull(), col] = 0  # zero is code for unknown
    elif 0 in df[col].unique().tolist():
        df.loc[df[col] == 0, col] = np.nan
    else:
        # unless the df is empty
        # check added to fill_in_sys_unknown()
        assert False, "One of the two if states shouldve tripped"
    return df


def naive_crossfill(
    kdf: pd.DataFrame,
    udf: pd.DataFrame,
    validate: str,
    merge_cols: List[str] = ["bene_id", "year_id"],
    clean_output_cols: bool = False,
) -> pd.DataFrame:
    """
    Params:
        kdf (pd.DataFrame): known values in this df will crossfill unknowns in udf
        udf (pd.DataFrame): unknowns in this df will be replaced by knowns from kdf

    Returns:
        udf with any unknown values filled when kdf is not unknown
    """
    if validate == "1:m":
        print(f"The validation method {validate} could allow duplication")

    udf = udf.merge(
        kdf[merge_cols + ["rti_race_cd"]],
        how="left",
        on=merge_cols,
        validate=validate,
        suffixes=("_unknown", "_known"),
    )

    udf["rti_race_cd_unknown_crossfill"] = udf["rti_race_cd_unknown"]
    cond = "(udf.rti_race_cd_unknown_crossfill == 0) & (udf.rti_race_cd_known != 0) & (udf.rti_race_cd_known.notnull())"
    print(
        f"Crossfilling will now replace {len(udf[eval(cond)])} rows of unknown values with known values"
    )
    udf.loc[eval(cond), "rti_race_cd_unknown_crossfill"] = udf.loc[
        eval(cond), "rti_race_cd_known"
    ]

    if clean_output_cols:
        drops = ["rti_race_cd_known", "rti_race_cd_unknown"]
        udf.drop(drops, axis=1, inplace=True)
        udf.rename(columns={"rti_race_cd_unknown_crossfill": "rti_race_cd"}, inplace=True)

    return udf


def fill_in_sys_unknown(
    df: pd.DataFrame, method: str, review_fill: bool = False
) -> pd.DataFrame:
    """replace the 'unknown' value with a known value when present in other years
    for a single system (max or mdcr)
    Order matters here, so there is a difference between forward filling then backfilling
    vs backfilling then forward filling, hence the method arg

    Params:
        df: (pd.DataFrame) to apply fill method to
        method: (str) either forward_back, or back_forward currently
    """
    pre_rows = len(df)

    if df.rti_race_cd.isnull().sum() > 0:
        raise ValueError("We don't expect missing race codes yet")

    unknown_benes = df.loc[df.rti_race_cd == 0, "bene_id"].unique()

    # split the data so our operations run on much smaller df
    kdf = df[~df.bene_id.isin(unknown_benes)].copy()
    df = df[df.bene_id.isin(unknown_benes)].copy()
    assert 0 not in kdf.rti_race_cd.unique().tolist(), "unknowns are still present in known df"

    if df.empty:
        # kdf should be the same in this case
        print("There is no unknown benes within this df to fill in.")
        # add a copy of race cd as filled column when review_fill is False
        # so we drop the right column at the end
        kdf["race_cd_filled"] = kdf["rti_race_cd"]
    else:
        # sort bene_ids
        df.sort_values(["bene_id", "year_id"], inplace=True)

        # replace unknown with null
        df = swap_unknown(df, "rti_race_cd")

        if method == "forward_back":
            df["race_cd_filled"] = df.groupby(["bene_id"])["rti_race_cd"].apply(
                lambda x: x.ffill().bfill()
            )
        elif method == "back_forward":
            df["race_cd_filled"] = df.groupby(["bene_id"])["rti_race_cd"].apply(
                lambda x: x.bfill().ffill()
            )
        else:
            raise ValueError(
                f"This function doesn't understand {method}. "
                "Must be forward_back or back_forward"
            )
        filled_count = df.rti_race_cd.isnull().sum() - df.race_cd_filled.isnull().sum()
        print(f"{filled_count} rows were filled with known values")

        df = swap_unknown(df, "rti_race_cd")
        df = swap_unknown(df, "race_cd_filled")

        kdf = pd.concat([df, kdf], sort=False, ignore_index=False)

        # add the known values
        cond = "(kdf.race_cd_filled.isnull()) & (kdf.rti_race_cd.notnull())"
        kdf.loc[eval(cond), "race_cd_filled"] = kdf.loc[eval(cond), "rti_race_cd"]

    assert (
        len(kdf) == pre_rows
    ), f"The row counts have changed somehow from {pre_rows} to {len(kdf)}"

    if not review_fill:
        kdf.drop("rti_race_cd", axis=1, inplace=True)
        kdf.rename(columns={"race_cd_filled": "rti_race_cd"}, inplace=True)

    return kdf
