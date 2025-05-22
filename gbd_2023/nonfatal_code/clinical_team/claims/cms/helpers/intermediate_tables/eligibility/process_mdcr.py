import argparse
import collections
from typing import Dict, Generator, List

import numpy as np
import pandas as pd
from crosscutting_functions.clinical_metadata_utils.parser import filepath_parser

import cms.helpers.intermediate_tables.eligibility.elig_helper as elig_help
from cms.helpers.intermediate_tables.eligibility.five_percent import Five_Percent
from cms.helpers.intermediate_tables.funcs import parquet_writer, validate_data
from cms.lookups.benes.bene_helper import BeneHelper


def col_rename(MDCR_YR) -> Dict[str, str]:
    return {
        "BENE_ID": "bene_id",
        "BENE_BIRTH_DT": "dob",
        "BENE_ENROLLMT_REF_YR": "year_id",
        "ENHANCED_FIVE_PERCENT_FLAG": "eh_five_percent_sample",
        MDCR_YR.five_percent: "five_percent_sample",
    }


def wide_cols() -> Generator:
    for col in ["MDCR_STATUS_CODE", "MDCR_ENTLMT_BUYIN_IND", "HMO_IND"]:
        yield [f"{col}_{str(e).zfill(2)}" for e in np.arange(1, 13)]


def disabled_ESRD_65(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate monthly ages and drop bene months for
    benes under 65. These benese may have been enrolled
    due to disability or ESRD
    """
    df["EOY_age"] = df.year_id - df.dob.dt.year
    temp = df[df.EOY_age == 65]
    ids = temp.bene_id.unique().tolist()

    # drop months before the bene turned 65
    temp = temp[temp.dob.dt.month <= temp.month_id]

    df = pd.concat([temp, df[~df.bene_id.isin(ids)]], sort=False)

    return df.drop("EOY_age", axis=1)


def drop_under_65(df: pd.DataFrame) -> pd.DataFrame:
    """
    drop an individuals under 65
    """
    return df[(df.year_id - df.dob.dt.year) >= 65]


def agg_buyin_codes(df: pd.DataFrame) -> pd.DataFrame:
    """
    There are buyin codes for both state and federal. This stratification
    has no impact on our estimates so we will be combining them.
    """
    buyin_dict = {
        1: ["1", "A"],  # part A
        2: ["2", "B"],  # part B
        3: ["3", "C"],  # part A and B
    }
    for k, v in buyin_dict.items():
        df.loc[df.MDCR_ENTLMT_BUYIN_IND.isin(v), "MDCR_ENTLMT_BUYIN_IND"] = k

    return df


def convert_part_c(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert HMO long vals to bool
    """

    df["HMO_IND"] = np.where(df["HMO_IND"].isin(["0", "4"]), False, True)
    return df


def check_group_data(year: int, group: int) -> bool:
    """Inspects a bene-group-year and if there is no data for the specified cms system-group-year
    then returns a flag for downstream processing.

    Args:
        group (int): bene group that points to a unique set of bene_ids
        year (int): Year of data to filter to.

    Returns:
        bool: Indicate if the group should be processed or skipped.
    """

    bh = BeneHelper()
    bene_group_table = bh.bene_helper(year_id=year, group_id=group, cms_systems=["mdcr"])
    if bene_group_table.shape[0] > 0:
        process_group_flag = True
    else:
        process_group_flag = False

    return process_group_flag


def get_bene_eligibility(MDCR_YR, group: int, year: int) -> pd.DataFrame:
    """Reads reads in data for for eligibility for bene_id in a given
    group-year for Medicare.

    Args:
        MDCR_YR (collections.namedtuple): Global var set in if name == main
        group (int): bene group that points to a unique set of bene_ids
        year (int): Year of data to filter to.

    Returns:
        pd.DataFrame: Filtered data for a given group-year and column selection.
    """
    bh = BeneHelper()
    table_name = MDCR_YR.tbl_name

    cols = list(col_rename(MDCR_YR).keys())
    cols += MDCR_YR.wide_cols

    bene_group_table = bh.bene_helper(year_id=year, group_id=group, cms_systems=["mdcr"])
    benes = bene_group_table.bene_id.to_list()

    root = "FILEPATH"
    df = pd.read_parquet("FILEPATH", columns=cols)

    df = df.loc[df.BENE_ID.isin(benes)]

    if year == 2000:
        # Cast year statuses to monthly statuses to replicate later year schema. Same status for each month.
        status_codes = [f"MDCR_STATUS_CODE_{e}" for e in np.arange(0, 13)]
        for month_status in status_codes:
            df[month_status] = df["BENE_MDCR_STATUS_CD"]

        df = df.drop("BENE_MDCR_STATUS_CD", axis=1)

    return df


def enforce_type(df: pd.DataFrame) -> pd.DataFrame:
    df["dob"] = pd.to_datetime(df.dob)
    df["year_id"] = df.year_id.astype(int)

    return df


def eligibility_reqs(MDCR_YR, year: int, group: int) -> List[pd.DataFrame]:
    """
    Create a list of dataframes remove unwanted eligibility
    flags
    """
    dfs = prep_eligibility(
        MDCR_YR,
        year,
        group,
    )
    proc_dict = {
        "HMO_IND": convert_part_c,
        "MDCR_STATUS_CODE": disabled_ESRD_65,
        "MDCR_ENTLMT_BUYIN_IND": agg_buyin_codes,
    }
    df_list: List[pd.DataFrame] = []
    for df in dfs:
        df_type = [e for e in df.columns if e.isupper()][0]
        func = proc_dict[df_type]
        df = func(df)
        df.drop("dob", axis=1, inplace=True)
        df_list.append(elig_help.apply_elig_reqs(df, "mdcr"))

    return df_list


def create_processing_tuple(year: int):
    tbl_suffix = "summary_res000052318_req008140_"
    cols = list(np.array([e for e in wide_cols()]).flatten())
    mdcr_year = collections.namedtuple("mdcr_year", ["five_percent", "wide_cols", "tbl_name"])

    if year == 2000:
        cols = [f"BENE_{e}" for e in cols if not e.startswith("MDCR_STATUS_CODE")]

        return mdcr_year(
            five_percent="FIVE_PERCENT_FLAG",
            wide_cols=cols + ["BENE_MDCR_STATUS_CD"],
            tbl_name=f"mbsf_ab_{tbl_suffix}{year}",
        )
    else:
        return mdcr_year(
            five_percent="SAMPLE_GROUP",
            wide_cols=cols,
            tbl_name=f"mbsf_abcd_{tbl_suffix}{year}",
        )


def correct_five_percent_sample(df: pd.DataFrame) -> pd.DataFrame:
    """Corrects issues in the five percent sample column formatting
    where mixed dtype caused issues.
    https://resdac.org/cms-data/variables/medicare-sample-group-indicator

    Args:
        df (pd.DataFrame): Expected is output of drop_under_65()

    Raises:
        ValueError: If there is more than one five_percent column found.

    Returns:
        pd.DataFrame: Input dataframe with corrected flag values.
    """
    # Name changes during worker from five_percent to five_percent_sample.
    cols = df.columns[df.columns.str.startswith("five_percent")]

    # Expected cols = ['five_percent*'].  Should only return 1 match.
    if len(cols) > 1:
        raise ValueError(f"Expected only one 5% column, found {len(cols)}")

    # Used replace over zfill to avoid altering values such as 'Y'
    df.replace({cols[0]: {"1": "01", "4": "04", 1: "01", 4: "04"}}, inplace=True)

    return df


def prep_eligibility(MDCR_YR, year: int, group: int) -> Generator:
    df = get_bene_eligibility(MDCR_YR, group=group, year=year)
    print(df.shape)
    df.rename(col_rename(MDCR_YR), axis=1, inplace=True)
    print(df.shape)
    if year == 2000:
        cols = [e for e in df.columns if e.isupper()]
        rename_dict = {e: e.replace("BENE_", "") for e in cols if e.startswith("BENE")}
        df.rename(rename_dict, axis=1, inplace=True)

    df = drop_under_65(enforce_type(df))

    # Correct values to be found in generating the sample table.
    # dtype inference dropped leading zeros from numeric like values.
    df = correct_five_percent_sample(df=df)

    # write the five percent table for a given year
    fp = Five_Percent(df=df, group=group, year=year)

    return elig_help.transform_wide(fp.process())


if __name__ == "__main__":

    output_cols = [
        "bene_id",
        "month_id",
        "year_id",
        "eligibility_reason",
        "entitlement",
        "part_c",
    ]

    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument(
        "--group", type=int, required=True, help="group number for the target file"
    )
    arg_parser.add_argument("--year", type=int, required=True, help="year for the target file")

    args = arg_parser.parse_args()

    base = "FILEPATH"

    # Base to write empty files (if needed) for 5% sample.
    sample_base = "FILEPATH"
    sample_cols = ["bene_id", "eh_five_percent_sample", "five_percent_sample", "year_id"]

    # Catch groups with no bene_id for the system-year
    flag = check_group_data(year=args.year, group=args.group)

    if flag:
        # Process if group has benes in the group-year
        MDCR_YR = create_processing_tuple(args.year)
        df_list = eligibility_reqs(MDCR_YR, args.year, args.group)
        # If all bene_id were excluded via exclusion_dict() in elig_helper
        if len(df_list) < 1:
            df = pd.DataFrame(columns=output_cols)
            # Write dummy sample file for group with excluded benes
            empty_sample = pd.DataFrame(columns=sample_cols)
            validate_data(df=empty_sample, cms_system="mdcr", table="five_percent")
            parquet_writer(
                df=empty_sample,
                path="FILEPATH",
            )
        else:
            df = elig_help.merge_dfs(df_list)
            df = df.sort_values(by=["bene_id", "month_id"]).reset_index(drop=True)
            df.rename(
                {
                    "MDCR_STATUS_CODE": "eligibility_reason",
                    "MDCR_ENTLMT_BUYIN_IND": "entitlement",
                    "HMO_IND": "part_c",
                },
                axis=1,
                inplace=True,
            )
    else:
        # Create dummy eligibility file for group-year with no benes
        df = pd.DataFrame(columns=output_cols)
        # Write dummy sample file for group flagged with no benes
        empty_sample = pd.DataFrame(columns=sample_cols)
        validate_data(df=empty_sample, cms_system="mdcr", table="five_percent")
        parquet_writer(
            df=empty_sample, path="FILEPATH"
        )

    df = df[output_cols]

    validate_data(df=df, cms_system="mdcr", table="elig")

    # May write an empty df if there were no bene for a given group-year or if all bene were intentionally excluded.
    parquet_writer(df=df, path="FILEPATH")
