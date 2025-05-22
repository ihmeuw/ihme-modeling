"""
Transform the staging tables to the ICD E-code table in our schema

"""
import argparse
import logging
import time
from pathlib import Path
from typing import Dict

import pandas as pd
from crosscutting_functions.clinical_constants.pipeline import cms as constants
from crosscutting_functions.clinical_constants.shared.mapping import ECODE_HEADERS
from crosscutting_functions.clinical_metadata_utils.parser import filepath_parser

from cms.helpers.intermediate_tables import funcs


def confirm_diag_cols(df, diag_cols):
    """Validation to ensure 100% of e-code columns are not null"""
    all_null = (df[diag_cols].isnull().sum() / len(df) == 1).all()
    if all_null:
        e = "Processing E-codes for this source year will fail because all dx cols are null"
        logging.error(e)
        raise ValueError(e)

    return


def validate_ecode_claims(df: pd.DataFrame, tabdf: pd.DataFrame) -> None:
    """Confirm only the expected columns are present and that the diagnosis codes in a
    tabulated dataframe created before the reshape operation match the diagnosis codes in the
    reshaped dataframe."""
    diff = set(constants.ICD_ECODE_COLUMNS).symmetric_difference(set(df.columns))
    if diff:
        e = f"We expect exact columns ({constants.ICD_ECODE_COLUMNS}). There is a setdiff of {diff}"
        logging.error(e)
        raise ValueError(e)

    df.sort_values(constants.ICD_ECODE_COLUMNS, inplace=True)

    funcs.validate_dx(df, tabdf)
    logging.info("Tabulated Diagnosis Test has passed")


def remove_entirely_null_dx_rows(df, diag_cols):
    """E-codes are relatively rare, especially in certain facilities
    This func removes rows where there is not a single non-null dx"""
    pre = len(df)
    cond = " | ".join([f"(df.dx_{i}.notnull())" for i in range(1, len(diag_cols) + 1)])

    df = df[eval(cond)].reset_index(drop=True)
    post = len(df)
    logging.warning(f"{pre-post} rows have been dropped because they are entirely null")

    return df


def prep_for_ecode_read() -> Dict[str, str]:
    """Return the raw data table to read and the columns to select."""
    # Pull in select columns, all rows from the db table
    cols = constants.icd_col_names["e_codes"]
    cols = funcs.convert_db_col_list(cols)
    return cols


def transform_to_ecode_claims(
    df: pd.DataFrame, claim_path: str, year: int, review_ecodes: bool = False
):
    """Drop rows without any ecodes and then reshape data from wide by diagnosis to long.
    Assign code system to the df and perform some validations."""
    diag_cols = df.filter(regex="dx_").columns.tolist()
    # cast all diagnosis columns to string
    for col in diag_cols:
        df[col] = df[col].astype("string")  # the string "string" should retain nulls
        df[col] = df[col].str.replace("\W", "", regex=True)  # noqa
    tabdf = funcs.tab_dx(df, diag_cols, pyspark=False)

    # check dx cols and remove rows with no E-code information
    confirm_diag_cols(df, diag_cols)
    df = remove_entirely_null_dx_rows(df, diag_cols)

    # make a datetime object column
    df["date"] = pd.to_datetime(df["clm_thru_dt"], infer_datetime_format=True)

    # Identify the code system ID using clm_thru_dt and heuristic
    df = funcs.assign_code_sys_by_date(df, col="date", year=year)

    # reshape from long to wide on ICD code. claim_id and dx_id identify rows
    df = funcs.reshape_dx(df)

    if review_ecodes:
        # we expect only certain ICD chapters in e-codes. Flag those that don't
        # match and then write them to LU for review
        output_path = "FILEPATH"

        df = funcs.flag_odd_code_sys(df, ECODE_HEADERS, output_path)

        logging.warning(
            (
                f"There are {len(df[df.code_sys_mismatch])} of {len(df)} rows with "
                "odd External cause of injury codes for a given code system"
            )
        )

        df.drop("code_sys_mismatch", 1, inplace=True)

    # add year after reshaping data, agnostic of data pull method (db or parquet)
    df["year_id"] = year

    validate_ecode_claims(df, tabdf)

    return df


def write_data(df: pd.DataFrame, claim_path: str, year: int) -> None:
    """Write to LU_CMS. Existing data will not be overwritten."""
    write_path = "FILEPATH"
    if not isinstance(df, pd.DataFrame):
        raise TypeError("The df is not a pandas dataframe!")
    funcs.parquet_writer(df=df, path=write_path)
    logging.info("A file has been written, overwritting an existing file if necessary.")


if __name__ == "__main__":
    arg_parser = argparse.ArgumentParser(description="Create ICD claims tables")
    arg_parser.add_argument(
        "--claim_path",
        help="Identifies the input file to extract ecodes from",
        type=str,
        required=True,
    )
    arg_parser.add_argument(
        "--year", help="Year corresponding to input file to process", type=int, required=True
    )
    args = arg_parser.parse_args()

    funcs.validate_year_in_path(year=args.year, claim_path=args.claim_path)

    WRITE_DIR = f"{filepath_parser(ini='pipeline.cms', section='table_outputs', section_key='inter_ecode')}"

    logname = time.strftime(f"{Path(args.claim_path).stem}_%Y_%m_%d-%H-%M.log")
    logpath = "FILEPATH"
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-10s %(processName)s  %(name)s %(message)s",
        filename=logpath,
    )

    cols_dict = prep_for_ecode_read()

    df = funcs.get_claims_data(args.claim_path, cols_dict)

    df = transform_to_ecode_claims(df=df, claim_path=args.claim_path, year=args.year)

    write_data(df=df, claim_path=args.claim_path, year=args.year)
