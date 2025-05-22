import argparse
from pathlib import PosixPath
from typing import Generator, List

import pandas as pd
from crosscutting_functions.clinical_metadata_utils.parser import filepath_parser

from cms.helpers.intermediate_tables.funcs import parquet_writer, validate_data


def gb_col_helper(denom: str, cms_system: str) -> List[str]:
    """
    Returns cols to groupby and a dict of cols to sum
    """
    gb_cols = ["age", "sex_id", "year_id", f"location_id_{denom}"]

    if denom == "cnty":
        gb_cols += ["rti_race_cd"]

    if cms_system == "mdcr":
        gb_cols += ["entitlement", "eh_five_percent_sample", "five_percent_sample"]
    else:
        gb_cols += ["restricted_benefit"]

    return gb_cols


def read_files(base: PosixPath) -> Generator:
    for e in base.glob("*.parquet"):
        yield pd.read_parquet(e)


def prep(df: pd.DataFrame) -> pd.DataFrame:
    """
    Fll na cols and format for tables produced by apply_bene_tbls.py
    """
    df = df.fillna(value={"location_id_gbd": "-1"})
    df["location_id_cnty"] = [str(e).zfill(5) for e in df.location_id_cnty]
    return df


def agg(denom: str, cms_system: str, base: PosixPath, year: int) -> None:
    """
    Main method
    """
    df = pd.concat(read_files(base), sort=False)
    df = prep(df)
    gb_cols = gb_col_helper(denom, cms_system)
    gb = df.groupby(gb_cols).agg({"sample_size": "sum"}).reset_index()

    if denom == "gbd":
        gb.loc[gb.location_id_gbd == "-1", "location_id_gbd"] = None

    convert_year_space(gb, denom, cms_system, base, year)


def convert_year_space(
    df: pd.DataFrame, denom: str, cms_system: str, base: PosixPath, year: int
) -> None:
    """
    Counts (created from the 'days' column in the age files) are in
    day space. Need to convert to year space
    """

    df["sample_size"] = df["sample_size"] / 365
    fo_path = "FILEPATH"

    validate_data(df=df, cms_system=cms_system, table=f"denom_{denom}")

    parquet_writer(df=df, path="FILEPATH")


if __name__ == "__main__":
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument(
        "--cms_system", type=str, required=True, help="CMS system, max or mdcr"
    )
    arg_parser.add_argument("--year", type=int, required=True, help="year for the target file")
    arg_parser.add_argument("--denom", type=str, required=True, help="denom type, cnty or gbd")

    args = arg_parser.parse_args()

    base = (
        "FILEPATH"
    )
    agg(args.denom, args.cms_system, base, args.year)
