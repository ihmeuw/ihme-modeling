import argparse
import collections
from typing import Dict, List

import pandas as pd
from crosscutting_functions.clinical_metadata_utils.parser import filepath_parser

from cms.helpers import build_CI_lookups
from cms.helpers.intermediate_tables.funcs import (
    date_formatter,
    parquet_writer,
    validate_data,
)
from cms.helpers.intermediate_tables.location_selection import Location_Select
from cms.lookups.benes.bene_helper import BeneHelper
from cms.lookups.inputs import location_type as lt


def col_rename(loc_sys) -> Dict[str, str]:
    return {
        "BENE_ID": "bene_id",
        "BENE_ENROLLMT_REF_YR": "year_id",
        "BENE_BIRTH_DT": "dob",
        "BENE_DEATH_DT": "dod",
        loc_sys.sex_col: "sex_id",
    }


def mdcr_cols(loc_sys) -> List[str]:
    """Builds list of needed mdcr columns to subset data."""
    demo = list(col_rename(loc_sys).keys())

    return demo + loc_sys.cols


def get_benes(mdcr_sum_tbl: str, loc_sys, year: int, group: int) -> pd.DataFrame:

    bh = BeneHelper()
    bene_group_table = bh.bene_helper(year_id=year, group_id=group, cms_systems=["mdcr"])
    benes = bene_group_table.bene_id.to_list()

    cols = mdcr_cols(loc_sys=loc_sys)
    table_name = f"{mdcr_sum_tbl}{year}"

    root = "FILEPATH"
    cols = mdcr_cols(loc_sys=loc_sys)
    df = pd.read_parquet("FILEPATH", columns=cols)
    df = df.loc[df.BENE_ID.isin(benes)]

    return df


def transform_wide(df: pd.DataFrame, loc_sys) -> pd.DataFrame:
    df_w = df.melt(
        id_vars=["bene_id"],
        value_vars=[e for e in loc_sys.cols],
        value_name="location_id",
        var_name="order",
    )
    df_w = df_w.sort_values(by=["bene_id", "order"]).reset_index(drop=True)

    return df_w


def apply_gbd_location(df: pd.DataFrame, loc_sys) -> pd.DataFrame:
    """
    Apply the GBD location state map
    """
    loc_map = build_CI_lookups.state_map_wide(lt.loc_lookup)
    loc_map = loc_map[["location_id", loc_sys.code]]

    loc_map[loc_sys.code] = loc_map[loc_sys.code].astype(str)
    df[loc_sys.code] = df[loc_sys.code].astype(str)

    m = pd.merge(df, loc_map, on=loc_sys.code, suffixes=("_cnty", "_gbd"), how="left")

    return m.drop(loc_sys.code, axis=1)


def gbd_location(df: pd.DataFrame, location_selector, year: int, loc_sys) -> pd.DataFrame:
    """
    Convert reporting source locations
    to GBD location ids. Retain the original
    location for USHD demo table
    """
    if year == 2000:
        df.fillna(value={"STATE_CODE": "00", "BENE_COUNTY_CD": "999"}, inplace=True)

        df["location_id"] = df["STATE_CODE"] + df["BENE_COUNTY_CD"]
        df.drop("BENE_COUNTY_CD", axis=1, inplace=True)
        df.rename({"STATE_CODE": loc_sys.code}, axis=1, inplace=True)
        return apply_gbd_location(df, loc_sys)

    # data must be wide before selecting the correct
    # location for the bene
    df_w = transform_wide(df, loc_sys)
    bene_locs = location_selector.run(df_w)

    missing = set(df_w.bene_id.tolist()) - set(bene_locs.bene_id.tolist())
    assert len(missing) == 0, "bene ids went missing during location transformation"

    m = pd.merge(df, bene_locs, on="bene_id", how="outer")
    m.drop(loc_sys.cols, axis=1, inplace=True)
    m[loc_sys.code] = [e[:2] for e in m.location_id]

    return apply_gbd_location(m, loc_sys)


def process(mdcr_sum_tbl: str, loc_sys, year: int, group: int) -> pd.DataFrame:
    """
    Fetch and shape the data in a usable format
    """
    df = get_benes(mdcr_sum_tbl, loc_sys, year, group)

    col_dict = col_rename(loc_sys)
    df.rename(col_dict, axis=1, inplace=True)

    # These columns need to be strings and retain leading zeros.
    # Stored as float which removed the leading zeros.
    # Add leading zeros to the code columns
    for col in loc_sys.cols:
        # Impossible int placeholder for nulls
        df.fillna(value={col: -1}, inplace=True)

        # Make in to remove decimal, make string to zfill.
        df[col] = df[col].astype(int).astype(str)

        # Set number of characters based on column name.
        if col == "STATE_CODE":
            zfill_len = 2
        elif col == "BENE_COUNTY_CD":
            zfill_len = 3
        elif "STATE_CNTY_FIPS_CD_" in col:
            zfill_len = 5

        # Change placeholder back to null.
        df.replace({col: {"-1": None}}, inplace=True)

        df[col] = df[col].str.zfill(zfill_len)

    df["dob"] = pd.to_datetime(df.dob)
    df["dod"] = pd.to_datetime(df.dod)

    df.loc[df.sex_id == 0, "sex_id"] = 3

    return df


if __name__ == "__main__":
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument("--year", type=int, required=True, help="target year")
    arg_parser.add_argument("--group", type=int, required=True, help="CMS bene group_id")

    args = arg_parser.parse_args()

    tbl_suffix = "summary_res000052318_req008140_"

    loc_typ = collections.namedtuple("loc_typ", ["code", "cols", "sex_col"])

    if args.year == 2000:
        mdcr_sum_tbl = f"mbsf_ab_{tbl_suffix}"
        loc_sys = loc_typ(
            code="MDCR_STATE_CD",
            cols=["STATE_CODE", "BENE_COUNTY_CD"],
            sex_col="BENE_SEX_IDENT_CD",
        )
    else:
        mdcr_sum_tbl = f"mbsf_abcd_{tbl_suffix}"
        loc_sys = loc_typ(
            code="STATE_CNTY_FIPS",
            cols=[f"STATE_CNTY_FIPS_CD_{str(e).zfill(2)}" for e in range(1, 13)],
            sex_col="SEX_IDENT_CD",
        )

    df = process(
        mdcr_sum_tbl,
        loc_sys,
        args.year,
        args.group,
    )
    print(f"Process {df.shape}")

    ls = Location_Select(year=args.year, group=args.group, loc_sys=loc_sys.code)
    final = gbd_location(df, ls, args.year, loc_sys)
    final = date_formatter(df=final)
    validate_data(df=final, cms_system="mdcr", table="demo")

    base = filepath_parser(ini="pipeline.cms", section="table_outputs", section_key="demo")
    parquet_writer(
        df=final, path="FILEPATH"
    )
