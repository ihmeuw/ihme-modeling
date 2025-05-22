import argparse
from typing import Generator, List

import pandas as pd
from crosscutting_functions.clinical_constants.pipeline.cms import bene_int_tbl_type
from crosscutting_functions.clinical_metadata_utils.parser import filepath_parser

from cms.helpers.intermediate_tables.funcs import parquet_writer, validate_data
from cms.utils.filters.elig_filters import MDCR_Filter


def type_dict(cms_system: str, dimension: str) -> dict:
    """
    When reading files make sure that each table
    is reading in each col with the same dtype
    """
    col_type = bene_int_tbl_type(cms_system)

    temp = {}
    temp.update(col_type["base"])
    temp.update(col_type[dimension])
    return temp


def read_files(cms_system: str, group: int, year: int) -> dict:
    df_dict = {}
    bene_tbls = ["elig", "demo", "age"]

    if cms_system == "mdcr":
        bene_tbls += ["sample"]

    for e in bene_tbls:
        path = (
            "FILEPATH"
        )

        if e == "sample":
            path = "FILEPATH"

        df_dict.update({e: pd.read_parquet("FILEPATH")})
    return df_dict


def clean_up(df: pd.DataFrame, cms_system: str, group: int, year: int) -> pd.DataFrame:
    """
    Correct for pandas datatype casting
    and remove null dob (mostly for MAX)
    """
    base = (
        "FILEPATH"
    )

    if df.dob.isnull().sum() > 0:
        parquet_writer(
            df=df[df.dob.isnull()], path="FILEPATH"
        )

    return df[df.dob.notnull()]


def merge_check(left_m_list: List[str], m_list: List[str]) -> None:
    missing = set(left_m_list) - set(m_list)
    assert len(missing) == 0, "Lost benes in the merge"


def race_file_gen(bene_ids: List[str], cms_system: str, year: int) -> Generator:
    """
    Read rti_race_cd flat files
    """
    files = (
        "FILEPATH"
    )
    for e in files.glob("*.parquet"):
        temp = pd.read_parquet(e)
        temp = temp[(temp.year_id == year) & (temp.bene_id.isin(bene_ids))]
        yield temp


def append_rti_race(
    df: pd.DataFrame, merge_cols: List[str], cms_system: str, year: int
) -> pd.DataFrame:
    """
    Add the rti_race column onto the dataframe
    """
    ids = df.bene_id.tolist()
    race = race_file_gen(bene_ids=ids, cms_system=cms_system, year=year)
    race = pd.concat(race)
    df = df.merge(race, on=merge_cols)
    merge_check(ids, df.bene_id.tolist())
    return df


def merge_files(cms_system: str, group: int, year: int) -> pd.DataFrame:
    """
    Combine the ages, demo, and elig files
    """
    df_dict = read_files(cms_system=cms_system, group=group, year=year)
    merge_cols = ["bene_id", "year_id"]
    temp = pd.merge(df_dict["age"], df_dict["elig"], on=merge_cols + ["month_id"])

    temp = pd.merge(temp, df_dict["demo"], on=merge_cols)

    if cms_system == "mdcr":
        temp = pd.merge(temp, df_dict["sample"], on=merge_cols)
    # the age files have the least number of bene ids (e.g the common denominator)
    # so we do not expect to loose any benes from this source

    # drop all possible null benes
    temp = temp.loc[temp.bene_id.notnull()]
    temp = temp.loc[~temp.bene_id.astype(str).str.lower().isin(["nan", "none"])]

    merge_check(df_dict["age"].bene_id.tolist(), temp.bene_id.tolist())

    temp = clean_up(temp, cms_system=cms_system, group=group, year=year)
    return append_rti_race(temp, merge_cols, cms_system=cms_system, year=year)


def gb_outputs(cms_system: str, group: int, year: int) -> pd.DataFrame:
    """
    Perform a groupby on all col dimensions
    """
    gb_cols = [
        "age",
        "sex_id",
        "year_id",
        "rti_race_cd",
        "location_id_gbd",
        "location_id_cnty",
    ]

    df = merge_files(cms_system=cms_system, group=group, year=year)
    if cms_system == "mdcr":
        # remove part c
        cms_system_filter = MDCR_Filter(df)
        df = cms_system_filter.filter()
        gb_cols += ["entitlement", "eh_five_percent_sample", "five_percent_sample"]
    elif cms_system == "max":
        gb_cols += ["restricted_benefit"]
    else:
        raise ValueError(
            f"Expected one of mdcr and max for cms_system but received {cms_system}"
        )

    df.rename({"days": "sample_size"}, axis=1, inplace=True)
    df = df.fillna(value={"location_id_gbd": "-1"})
    gb = df.groupby(gb_cols).agg({"sample_size": "sum"}).reset_index()
    gb.loc[gb.location_id_gbd == "-1", "location_id_gbd"] = None
    return gb


if __name__ == "__main__":

    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument(
        "--cms_system", type=str, required=True, help="CMS system, max or mdcr"
    )
    arg_parser.add_argument("--year", type=int, required=True, help="year for the target file")
    arg_parser.add_argument("--group", type=int, required=True, help="bene group_id")

    args = arg_parser.parse_args()
    year = int(args.year)

    base = (
        "FILEPATH"
    )

    df = gb_outputs(cms_system=args.cms_system, group=args.group, year=args.year)

    validate_data(df=df, cms_system=args.cms_system, table="denom")

    parquet_writer(df=df, path="FILEPATH")
