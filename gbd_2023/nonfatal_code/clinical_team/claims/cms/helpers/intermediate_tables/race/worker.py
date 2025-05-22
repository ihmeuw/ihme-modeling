"""
Processes the set of unique beneficiary IDs for a given range. Note this script
queries data across ALL years and states for both MDCR and MAX.
"""
import argparse
from typing import List, Tuple

import pandas as pd
from crosscutting_functions.clinical_constants.pipeline import cms as constants
from crosscutting_functions.clinical_metadata_utils.parser import filepath_parser

from cms.helpers.intermediate_tables.funcs import parquet_writer
from cms.helpers.intermediate_tables.race import fill_unknown, select_race
from cms.lookups.benes.bene_helper import BeneHelper


def get_cols(cms_system: str) -> List[str]:
    """
    Keep needed columns based on cms_system type
    """
    cols = ["BENE_ID"]

    if cms_system == "mdcr":
        cols += ["BENE_ENROLLMT_REF_YR", "RTI_RACE_CD", "BENE_RACE_CD"]
    elif cms_system == "max":
        cols += [
            "MAX_YR_DT",
            "MDCR_RACE_ETHNCY_CD",
            "EL_RACE_ETHNCY_CD",
            "EL_SEX_RACE_CD",
            "RACE_CODE_1",
            "RACE_CODE_2",
            "RACE_CODE_3",
            "RACE_CODE_4",
            "RACE_CODE_5",
        ]

    return cols


def get_mdcr(group: int) -> pd.DataFrame:
    """
    Get mdcr dataframe from converted parquet files on disk
    based on specified group id.
    """
    cols = get_cols(cms_system="mdcr")
    tbl_suffix = "summary_res000052318_req008140_"

    # get bene_ids from parquet based on group id
    # need all years' so make year_id -1
    bh = BeneHelper()
    bene_group_table = bh.bene_helper(year_id=-1, group_id=group, cms_systems=["mdcr"])
    benes = bene_group_table.bene_id.to_list()

    df_list = []
    for year in constants.mdcr_years:
        root = "FILEPATH"

        if year == 2000:
            table = f"mbsf_ab_{tbl_suffix}"
        else:
            table = f"mbsf_abcd_{tbl_suffix}"

        df = pd.read_parquet("FILEPATH", columns=cols)
        df = df.loc[df.BENE_ID.isin(benes)]
        df_list.append(df)
        del df

    df = pd.concat(df_list, sort=False, ignore_index=True)
    df["RTI_RACE_CD"] = df["RTI_RACE_CD"].astype(int)
    return df


def get_max(group: int) -> pd.DataFrame:
    """
    Get medicaid dataframe from converted parquet files on disk
    based on specified group id.
    """
    cols = get_cols(cms_system="max")

    # get bene_ids from parquet based on group id
    # need all years' so make year_id -1
    bh = BeneHelper()
    bene_group_table = bh.bene_helper(year_id=-1, group_id=group, cms_systems=["max"])
    benes = bene_group_table.bene_id.to_list()

    df_list = []
    for year in constants.max_years:
        root = "FILEPATH"
        states = constants.max_state_years[year]
        for state in states:
            df = pd.read_parquet(
                "FILEPATH", columns=cols
            )
            df = df.loc[df.BENE_ID.isin(benes)]
            df_list.append(df)
            del df

    return pd.concat(df_list, sort=False, ignore_index=True)


def convert_el_race_to_rti(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert the original el race column to the official rti race codes.
    """
    df["EL_RACE_ETHNCY_CD"] = df["EL_RACE_ETHNCY_CD"].astype(int)
    df["RTI_RACE_CD"] = None  # copy el codes into new column

    # convert them to rti values
    for el, rti in constants.max_race_convert_dict.items():
        df.loc[df.EL_RACE_ETHNCY_CD == el, "RTI_RACE_CD"] = rti

    if df.RTI_RACE_CD.isnull().sum() != 0:
        raise ValueError("There are missing RTI codes")
    return df


def write_raw_data(maxdf: pd.DataFrame, mdcr: pd.DataFrame, group: int) -> None:
    """
    Write intermediate files to disk.
    """

    raw_dir = filepath_parser(ini="pipeline.cms", section="table_outputs", section_key="race")

    maxdf.to_parquet("FILEPATH")
    mdcr.to_parquet("FILEPATH")
    print("Raw race data written to disk.")

    return


def unify_cols(maxdf: pd.DataFrame, mdcr: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Standardize column names for both mdcr and medicaid dataframes.
    """
    maxdf.rename(
        columns={
            "BENE_ID": "bene_id",
            "MAX_YR_DT": "year_id",
            "RTI_RACE_CD": "rti_race_cd",
        },
        inplace=True,
    )
    mdcr.rename(
        columns={
            "BENE_ID": "bene_id",
            "BENE_ENROLLMT_REF_YR": "year_id",
            "RTI_RACE_CD": "rti_race_cd",
        },
        inplace=True,
    )
    keep_cols = ["bene_id", "year_id", "rti_race_cd"]
    mdcr = mdcr[keep_cols]
    maxdf = maxdf[keep_cols]

    col_diff = set(maxdf.columns).symmetric_difference(mdcr.columns)
    assert not col_diff, f"We can't have a column name difference {col_diff}"
    return maxdf, mdcr


def main(group: int) -> None:
    """
    Main processing method for the race intermediate tables.
    Read in both systems' converted parquets across years and states
    Format columns and convert original medicaid race codes to rti values
    Use a few different methods to correct/fill in race codes.
    """

    # get the data from all tables
    maxdf = get_max(group=group)
    mdcr = get_mdcr(group=group)

    # store intermediate data
    write_raw_data(maxdf, mdcr, group=group)

    # convert el_race codes in max to rti values
    maxdf = convert_el_race_to_rti(maxdf)

    # unify col names and drop unused race cols
    maxdf, mdcr = unify_cols(maxdf, mdcr)

    # backfill, forwardfill per decision-making
    method = "back_forward"
    maxdf = fill_unknown.fill_in_sys_unknown(df=maxdf, method=method, review_fill=False)
    mdcr = fill_unknown.fill_in_sys_unknown(df=mdcr, method=method, review_fill=False)

    # resolve disagreement. Take a 'vote' of each bene-race-year then go with
    # most common or randomly pick
    maxdf = select_race.resolve_disagreement_by_year(maxdf)
    mdcr = select_race.resolve_disagreement_by_year(mdcr)

    # now crossfill
    maxdf = fill_unknown.naive_crossfill(
        kdf=mdcr,
        udf=maxdf,
        validate="1:1",
        merge_cols=["bene_id", "year_id"],
        clean_output_cols=True,
    )
    mdcr = fill_unknown.naive_crossfill(
        kdf=maxdf,
        udf=mdcr,
        validate="1:1",
        merge_cols=["bene_id", "year_id"],
        clean_output_cols=True,
    )
    # one last in system fill to complete the cross system fill
    maxdf = fill_unknown.fill_in_sys_unknown(df=maxdf, method=method, review_fill=False)
    mdcr = fill_unknown.fill_in_sys_unknown(df=mdcr, method=method, review_fill=False)

    # sort final results
    sortcols = ["bene_id", "year_id", "rti_race_cd"]
    maxdf.sort_values(sortcols, inplace=True)
    mdcr.sort_values(sortcols, inplace=True)

    maxdf = select_race.set_dtypes(maxdf)
    mdcr = select_race.set_dtypes(mdcr)

    maxdf = select_race.unique_coding_across_years(maxdf)
    mdcr = select_race.unique_coding_across_years(mdcr)

    # write output to intermediate table dir
    outdir = filepath_parser(ini="pipeline.cms", section="table_outputs", section_key="race")

    parquet_writer(df=maxdf, path="FILEPATH")
    parquet_writer(df=mdcr, path="FILEPATH")


if __name__ == "__main__":
    arg_parser = argparse.ArgumentParser()

    arg_parser.add_argument("--group", type=int, required=True, help="group number")

    args = arg_parser.parse_args()

    print(f"Beginning to process benes in group {args.group}")
    main(group=args.group)
