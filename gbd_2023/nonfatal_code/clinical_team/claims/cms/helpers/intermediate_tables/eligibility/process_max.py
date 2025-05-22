import argparse
from typing import Dict, Generator

import numpy as np
import pandas as pd
from crosscutting_functions.clinical_constants.pipeline.cms import max_state_years
from crosscutting_functions.clinical_metadata_utils.parser import filepath_parser

import cms.helpers.intermediate_tables.eligibility.elig_helper as elig_help
from cms.helpers.intermediate_tables.funcs import parquet_writer, validate_data
from cms.lookups.benes.bene_helper import BeneHelper


def col_rename() -> Dict[str, str]:
    return {
        "BENE_ID": "bene_id",
        "MAX_YR_DT": "year_id",
    }


def wide_cols() -> Generator:
    for col in ["MAX_ELG_CD_MO", "EL_RSTRCT_BNFT_FLG", "MC_COMBO_MO"]:
        yield [f"{col}_{e}" for e in np.arange(1, 13)]


def record_dup_bene_elg(df: pd.DataFrame, year: int, group: int) -> None:
    """
    Record dups how they appear in the db.
    Useful to later check the interaction between each column
    """
    dup_ids = df[df.bene_id.duplicated()].bene_id.tolist()
    if len(dup_ids) == 0:
        return

    dups = df[df.bene_id.isin(dup_ids)].reset_index(drop=True)
    base = (
        "FILEPATH"
    )
    path = "FILEPATH"
    for col in dups.columns:
        # Raw column names are UPPERCASE
        if not col.islower():
            dups[col] = pd.to_numeric(df[col], errors="coerce", downcast="integer")

    parquet_writer(df=dups, path="FILEPATH")


def coin_flip(df: pd.DataFrame) -> pd.DataFrame:
    """
    Randomly choose an elig flag
    """
    bene_ids = df[df.bene_id.duplicated()].bene_id.tolist()

    if len(bene_ids) == 0:
        return df

    coin = df[df.bene_id.isin(bene_ids)]
    coin = coin.sample(frac=1, random_state=1).drop_duplicates(
        subset=["bene_id"], keep="first"
    )

    df = df[~df.bene_id.isin(bene_ids)]

    return pd.concat([df, coin], sort=False)


def restricted_benefit_dedup(df: pd.DataFrame) -> pd.DataFrame:
    """
    Dedup restricted benefit dataframe.

    All months in the input dataframe at this point should have multiple flags
    associated with them so we want to either enforce flag or select randomly.
    "1" (full benefit) is prioritized, meaning we enforce "1" if "1" is present
    at all in any month.
    Then non-"0" or non-"9" flag is prioritized next, meaning if "0" or "9" is present
    we ignore such record(s) and randomly select a flag from the rest. If there's
    none available outside of "0" or "9", we pick "9" for that bene-month.
    If none of "0" "1" "9" is present, we randomly select a flag for that bene
    month.

    Because it's relatively rare for benes to move states, the input df should
    be relatively small at this point.
    """
    # list of extracted dfs for each bene-year-month
    results = []
    for _, temp in df.groupby(["bene_id", "month_id"]):
        flags = temp.EL_RSTRCT_BNFT_FLG.unique().tolist()
        known_benefit = temp.loc[~temp.EL_RSTRCT_BNFT_FLG.isin(["9"])]
        if "1" in flags:
            results.append(temp.loc[temp.EL_RSTRCT_BNFT_FLG == "1"])
        elif "9" in flags:
            # this if condition happens when the bene had "0" and "9"
            # in the same month_id
            if known_benefit.empty:
                results.append(temp.loc[temp.EL_RSTRCT_BNFT_FLG == "9"])
            else:
                results.append(known_benefit.sample(n=1, random_state=1))
        else:
            results.append(temp.sample(n=1, random_state=1))

    results = pd.concat(results)
    check = results.groupby(["bene_id", "month_id"]).size().reset_index(name="vc")  # type: ignore
    assert (check.vc == 1).all(), "Multiple flags associated with certain month_id still"

    return results


def dup_bene_eligibility(df: pd.DataFrame) -> pd.DataFrame:
    """
    Edge case for dup bene ids. For the same location, the bene has
    multiple rows of eligibility.

    Easier to process dups when the data is long
    """
    # not uncommon to see benes with two seperate MSIS IDs
    # but identical enrollment flags for the same month
    df = df.drop_duplicates().reset_index(drop=True)

    # check to see that a given month / bene is not duplicated
    gb = df.groupby(["bene_id", "month_id"]).size().reset_index(name="vc")
    dup_ids = gb[gb.vc > 1].bene_id.unique().tolist()
    del gb

    if len(dup_ids) == 0:
        return df

    col = [e for e in df.columns if e[0].isupper()][0]
    if col == "EL_RSTRCT_BNFT_FLG":
        flag_by_month = df.groupby(["bene_id", "month_id"]).size().reset_index(name="counts")
        flag_by_month = df.merge(flag_by_month, on=["bene_id", "month_id"], how="left")
        # this dataframe should have the exact dup_ids in it
        multi_months = flag_by_month.loc[flag_by_month.counts > 1]
        single_months = flag_by_month[~flag_by_month.index.isin(multi_months.index)]
        # "0"s in the multi_month df are guaranteed to be dropped
        # drop it now to reduce run time
        multi_months = multi_months.loc[multi_months[col] != "0"]

        # dedup subset of df with multiple flags associated with certain month_ids
        results = restricted_benefit_dedup(multi_months)

        df = pd.concat([single_months, results])
        df = df.sort_values(by=["bene_id", "year_id", "month_id"]).drop(["counts"], axis=1)
    else:
        dups = df[df.bene_id.isin(dup_ids)]

        # frequency counts of eligibility flags in the time series
        # for each bene
        df_elg_vc = dups.groupby("bene_id")[col].value_counts().reset_index(name="counts")
        df_elg_vc["max_counts"] = df_elg_vc.groupby("bene_id")["counts"].transform("max")
        df_elg_vc = df_elg_vc[df_elg_vc.counts == df_elg_vc.max_counts]

        # verify that there is only one bene id per row
        # in df_elg_vc
        df_elg_vc = coin_flip(df_elg_vc)

        # find the month that needs to be updated
        df_mo_vc = (
            dups.groupby(["bene_id"])["month_id"].value_counts().reset_index(name="mo_vc")
        )
        df_mo_vc = df_mo_vc[df_mo_vc.mo_vc > 1]

        # the correct flag for duplicate bene months
        df_corrections = pd.merge(df_mo_vc, df_elg_vc, on="bene_id").drop(
            ["mo_vc", "counts", "max_counts"], axis=1
        )

        df = pd.merge(
            df,
            df_corrections,
            on=["bene_id", "month_id"],
            how="outer",
            suffixes=["_og", "_up"],
        )

        df[col] = np.where(df[f"{col}_up"].isnull(), df[f"{col}_og"], df[f"{col}_up"])
        df.drop([f"{col}_up", f"{col}_og"], axis=1, inplace=True)
        df.drop_duplicates(inplace=True)

    # final check
    gb = df.groupby(["bene_id", "month_id"]).size().reset_index(name="vc")
    assert (gb.vc == 1).all(), "Duplicated bene_ids"

    return df


def correct_elig_cds(df: pd.DataFrame) -> pd.DataFrame:
    """Corrects issues in the MAX elig code cols where conversion
    dropped leading zeros by changing dtype.

    Args:
        df (pd.DataFrame): Filtered ps file.

    Returns:
       pd.DataFrame: Input dataframe with corrected MAX_ELG_CD_MO cols
    """

    elig_cd_cols = df.columns[df.columns.str.startswith("MAX_ELG_CD_MO")]

    for col in elig_cd_cols:
        df[col] = df[col].astype(str)
        df[col] = df[col].str.zfill(2)

    return df


def get_bene_eligibility(year: int, group: int) -> pd.DataFrame:
    """Reads reads in data for for eligibility for bene_id in a given
    group-year for Medicaid.

    Args:
        year (int): Year of data to filter to.
        group (int): bene group that points to a unique set of bene_ids

    Returns:
        pd.DataFrame: Filtered data for a given group-year and column selection.
    """
    bh = BeneHelper()
    root = "FILEPATH"
    df_list = []

    # Create list if cols to filter to
    cols = [e for e in col_rename().keys()]
    cols += list(np.array([e for e in wide_cols()]).flatten())
    cols += ["EL_RSDNC_CNTY_CD_LTST", "STATE_CD"]

    # Get unique bene_id for a given group-year
    bene_group_table = bh.bene_helper(year_id=year, group_id=group, cms_systems=["max"])
    benes = bene_group_table.bene_id.to_list()

    # Get filtered eilibility data from all states for the unique benes
    states = max_state_years[year]
    for state in states:
        df = pd.read_parquet("FILEPATH", columns=cols)
        df = df.loc[df.BENE_ID.isin(benes)]
        df_list.append(df)

    df = pd.concat(df_list, ignore_index=True)

    # Ensure monthly elig codes are 2 digit strings.
    df = correct_elig_cds(df=df)

    return df


def eligibility_reqs(year: int, group: int, record_dup: bool) -> Generator:
    """
    Create a list of dataframes remove unwanted eligibility
    flags
    """
    df = get_bene_eligibility(year=year, group=group)
    df.rename(col_rename(), axis=1, inplace=True)
    cols = [e for e in df.columns if e not in ["EL_RSDNC_CNTY_CD_LTST", "STATE_CD"]]
    # handle eligibilty based duplication
    if record_dup:
        record_dup_bene_elg(df, year, group)

    df = df[cols]

    dfs = elig_help.transform_wide(df)

    for df in dfs:
        yield elig_help.apply_elig_reqs(df, "max")


def combine_dfs(year: int, group: int, record_dup: bool) -> pd.DataFrame:
    df_list = []
    for e in eligibility_reqs(year, group, record_dup):
        temp = dup_bene_eligibility(e)
        df_list.append(temp)

    return elig_help.merge_dfs(df_list)


if __name__ == "__main__":
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument(
        "--group", type=int, required=True, help="group number for the target file"
    )
    arg_parser.add_argument("--year", type=int, required=True, help="year for the target file")

    args = arg_parser.parse_args()

    base = (
        "FILEPATH"
    )

    df = combine_dfs(args.year, args.group, record_dup=True)
    df = df.sort_values(by=["bene_id", "month_id"]).reset_index(drop=True)
    df = df[
        [
            "bene_id",
            "year_id",
            "month_id",
            "MAX_ELG_CD_MO",
            "EL_RSTRCT_BNFT_FLG",
            "MC_COMBO_MO",
        ]
    ]
    df.rename(
        {
            "MAX_ELG_CD_MO": "eligibility_reason",
            "EL_RSTRCT_BNFT_FLG": "restricted_benefit",
            "MC_COMBO_MO": "managed_care",
        },
        axis=1,
        inplace=True,
    )

    df["restricted_benefit"] = df.restricted_benefit.astype(str)

    validate_data(df=df, cms_system="max", table="elig")

    parquet_writer(df=df, path="FILEPATH")
