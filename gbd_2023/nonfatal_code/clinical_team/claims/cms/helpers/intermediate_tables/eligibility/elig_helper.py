import functools
import re
from typing import Dict, Generator, List

import pandas as pd

"""'
Helper functions that are used in both
max and mdcr processing
"""


def exclusion_dict() -> Dict[str, Dict[str, List[str]]]:
    """'
    Eligibility flags that should remove a bene month
    in our estimates
    """
    return {
        "max": {"MAX_ELG_CD_MO": ["00"]},
        "mdcr": {"MDCR_STATUS_CODE": ["00"], "MDCR_ENTLMT_BUYIN_IND": ["0"]},
    }


def inclusion_dict() -> Dict[str, Dict[str, List[str]]]:
    """
    Eligibility flags that should be included in our
    estimates
    """
    return {"max": {"EL_RSTRCT_BNFT_FLG": ["1", "2", "4", "6"], "MC_COMBO_MO": ["16", "2"]}}


def apply_elig_reqs(df: pd.DataFrame, cms_system: str, exclude=True) -> pd.DataFrame:
    """
    Function is flexiable enough to append future eligibilty requirments
    """
    cond = "df[k].isin(v)"

    if exclude:
        # NOTE: cond += '~' appends the
        # ~ at the end of the string
        cond = "~" + cond
        criteria_dict = exclusion_dict()[cms_system]
    else:
        criteria_dict = inclusion_dict()[cms_system]

    for k, v in criteria_dict.items():
        try:
            return df[eval(cond)]
        except KeyError:
            continue

    return df


def full_year_coverage(df: pd.DataFrame) -> pd.DataFrame:
    temp = df.groupby("bene_id").agg({"month_id": "count"}).reset_index()
    return (temp["month_id"] == 12).all()


def process_wide(df: pd.DataFrame) -> Generator:
    """
    Create seperate dfs for each eligibility column
    """
    wide_cols = {re.sub("_[0-9]+", "", e) for e in df.variable.unique().tolist()}

    for col in wide_cols:
        temp = df[df.variable.str.startswith(col)]
        temp["month_id"] = [int(e.split("_")[-1]) for e in temp.variable]

        temp.rename({"value": col}, axis=1, inplace=True)
        temp[col] = temp[col].astype(str)
        # assert full_year_coverage(temp), f'Missing month_ids for {col}'
        yield temp.drop("variable", axis=1)


def transform_wide(df: pd.DataFrame) -> Generator:
    """
    Transform eligibility columns from wide to long.

    Function assumes that elg columns are uppercase and demo cols
    (e.g. bene_id) are lowercase
    """

    ids = [e for e in df.columns if e[0].islower()]
    wide_cols = [e for e in df.columns if e not in ids]

    df_w = df.melt(
        id_vars=ids,
        value_vars=wide_cols,
    ).sort_values(by=["bene_id", "variable"])

    return process_wide(df_w)


def merge_dfs(iterable: List[pd.DataFrame]) -> pd.DataFrame:
    """
    Combine n+ dataframes. Note that
    if an inculsion dict is used for processing
    in future a how='outer' arg should be included
    """
    return functools.reduce(
        lambda left, right: pd.merge(
            left,
            right,
            on=["bene_id", "month_id", "year_id"],
        ),
        iterable,
    )
