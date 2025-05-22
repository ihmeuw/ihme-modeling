import argparse
from typing import Dict, List

import numpy as np
import pandas as pd
from crosscutting_functions.clinical_constants.pipeline.cms import max_state_years
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
from cms.utils.transformations import max_loc_fips


def col_rename() -> Dict[str, str]:
    return {
        "BENE_ID": "bene_id",
        "MAX_YR_DT": "year_id",
        "EL_SEX_CD": "sex_id",
        "EL_DOB": "dob",
        "EL_DOD": "dod",
        "EL_ELGBLTY_MO_CNT": "location_vc",  # number of eligibility months is a
    }  # proxy for months at a location


def max_cols() -> List[str]:
    demo = list(col_rename().keys())
    locs = ["STATE_CD", "EL_RSDNC_CNTY_CD_LTST"]
    return demo + locs


def convert_datetime(df: pd.DataFrame) -> pd.DataFrame:
    for col in ["dob", "dod"]:
        df[col] = df[col].astype(str)

        df["up"] = [
            pd.datetime(year=int(e[0:4]), month=int(e[4:6]), day=int(e[6:8]))
            if e not in ["None", "nan"]
            else np.datetime64("NaT")
            for e in df[col]
        ]
        df.drop(col, axis=1, inplace=True)
        df.rename({"up": col}, axis=1, inplace=True)
    return df


def get_benes(year: int, group: int) -> pd.DataFrame:

    bh = BeneHelper()
    bene_group_table = bh.bene_helper(year_id=year, group_id=group, cms_systems=["max"])
    benes = bene_group_table.bene_id.to_list()

    cols = max_cols()
    root = "FILEPATH"

    states = max_state_years[year]
    df_list = []
    for state in states:
        df = pd.read_parquet("FILEPATH", columns=cols)
        df = df.loc[df.BENE_ID.isin(benes)]
        df_list.append(df)

    df = pd.concat(df_list, ignore_index=True)

    return df


def copy_dup_demo(df: pd.DataFrame, year: int, group: int, write_temp: bool) -> pd.DataFrame:
    """
    For benes that have multiple locs, copy demo info if
    one entry has null demo values.
    """
    ids = df[df.bene_id.duplicated()].bene_id.tolist()
    ids = df[df.bene_id.isin(ids) & (df.dob.isnull())].bene_id.tolist()
    if len(ids) == 0:
        return df

    temp = df[df.bene_id.isin(ids)]
    if write_temp:
        temp.to_parquet(
            "FILEPATH"
        )

    for col in ["dob", "dod", "location_vc", "sex_id"]:
        t_type = "max"
        if col == "sex_id":
            t_type = "min"

        temp[f"t_{col}"] = temp.groupby("bene_id")[col].transform(t_type)
        temp[col] = temp[f"t_{col}"]
        temp.drop(f"t_{col}", axis=1, inplace=True)

    return pd.concat([df[~df.bene_id.isin(ids)], temp], sort=False)


def remove_invalid_cnty(
    df: pd.DataFrame, year: int, group: int, write_log_out: bool, write_temp: bool
) -> pd.DataFrame:
    """
    For benes with multiple location
    keep locations with real county codes
    (eg. when the two locations is either 56000
    or 56003 choose 56003). If both / all locations
    are 0 or 9 filled, then do not drop those rows
    """

    df["cnty"] = [e[2:] for e in df["location_id"]]
    df["valid_cnty"] = False
    df.loc[~df.cnty.isin(["000", "999"]), "valid_cnty"] = True

    df_valid = df[df.valid_cnty == True]  # noqa

    # find benes where all cnty locations for that bene
    # are 0 / 9 filled
    missing = set(df.bene_id) - set(df_valid.bene_id)

    # record benes that had both T & F values
    # for valid_cnty.
    if write_log_out:
        ids = df_valid.bene_id.tolist()
        log_out = df[(df.bene_id.isin(ids)) & (df.valid_cnty == False)]  # noqa

        base = filepath_parser(ini="pipeline.cms", section="table_outputs", section_key="demo")
        log_out.to_parquet("FILEPATH")

    if len(missing) > 0:
        df_valid = pd.concat([df_valid, df[df.bene_id.isin(missing)]], sort=False)

    df_valid.drop(["cnty", "valid_cnty"], axis=1, inplace=True)

    return copy_dup_demo(df_valid, year, group, write_temp)


def location_prep(
    df: pd.DataFrame, year: int, group: int, write_log_out: bool, write_temp: bool
) -> pd.DataFrame:
    """
    Additonal data cleaning before passing onto
    location algorithm
    """
    # create the dup ids
    ids = df[df.bene_id.duplicated()].bene_id.tolist()
    dups = df[df.bene_id.isin(ids)]
    dups = remove_invalid_cnty(dups, year, group, write_log_out, write_temp)

    # regardless of months of eligibility (eg. location_vc) drop
    # duplicates where the location is the same
    dups.drop_duplicates(subset=["bene_id", "location_id"], inplace=True)

    return pd.concat([df[~df.bene_id.isin(ids)], dups], sort=False)


def apply_gbd_location(df: pd.DataFrame) -> pd.DataFrame:
    """
    Append the GBD location state map
    """
    loc_map = build_CI_lookups.state_map_wide(lt.loc_lookup)
    loc_map = loc_map[["location_id", "STATE_CNTY_FIPS"]]

    df["STATE_CNTY_FIPS"] = [e[:2] for e in df.location_id]
    m = pd.merge(df, loc_map, on="STATE_CNTY_FIPS", suffixes=("_cnty", "_gbd"), how="left")

    return m.drop("STATE_CNTY_FIPS", axis=1)


def gbd_location(
    df: pd.DataFrame, year: int, group: int, write_log_out: bool, write_temp: bool
):
    df = location_prep(df, year, group, write_log_out, write_temp)
    ls = Location_Select(year, group, "max")
    return apply_gbd_location(ls.run(df))


def process(year: int, group: int) -> pd.DataFrame:
    """
    Clean up the data.
    """
    df = get_benes(year=year, group=group)

    df = max_loc_fips.create_fips(df)

    col_dict = col_rename()
    df.rename(col_dict, axis=1, inplace=True)

    df["sex_id"] = df.sex_id.map({"M": 1, "F": 2, "U": 3})
    return convert_datetime(df)


if __name__ == "__main__":
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument("--year", type=int, required=True, help="target year")
    arg_parser.add_argument("--group", type=int, required=True, help="CMS bene group_id")

    args = arg_parser.parse_args()

    df = process(year=args.year, group=args.group)
    final = gbd_location(df, args.year, args.group, write_log_out=True, write_temp=True)

    missing = set(df.bene_id) - set(final.bene_id)
    if len(missing) > 0:
        raise RuntimeError(f"{len(missing)} bene_id(s) were lost.")
    if final.bene_id.duplicated().sum() != 0:
        raise RuntimeError(f"{final.bene_id.duplicated().sum()} duplicate bene_id(s).")

    final = date_formatter(df=final, columns=["dob", "dod"])
    validate_data(df=final, cms_system="max", table="demo")

    base = filepath_parser(ini="pipeline.cms", section="table_outputs", section_key="demo")
    parquet_writer(df=final, path="FILEPATH")
