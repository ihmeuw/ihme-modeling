import argparse
import logging
import os
import string
import time
from pathlib import Path
from typing import Dict, List, Tuple

from crosscutting_functions.clinical_constants.pipeline import cms as constants
from crosscutting_functions.clinical_metadata_utils.parser import filepath_parser


from pyspark import SparkConf, SparkContext
from pyspark import pandas as ps

from cms.helpers.intermediate_tables import funcs


def exp_icd_codes() -> Dict[int, List[str]]:
    """Identify the starting digit that we'd expect for either ICD 9 or
    ICD 10 codes"""

    icd9 = [str(i) for i in range(0, 10, 1)] + ["E", "V"]
    icd10 = list(string.ascii_uppercase)

    return {1: icd9, 2: icd10}


def get_raw_data(
    claim_path: str,
    cols_dict: Dict[str, str],
) -> ps.DataFrame:
    """Rather than 1 huge query statement which could cause memory issues with the
    database loop over a set of claim_ids (clms) and extract them in groups of (step) rows
    """
    df = funcs.get_claims_data(read_path=claim_path, cols_dict=cols_dict, pyspark=True)

    if "_ot_" in claim_path:
        # remove MAX otp rows which are completely missing any dx
        df = df[(df.dx_1.notnull()) | (df.dx_2.notnull())]

    funcs.validate_claim_id(df)

    return df


def prep_for_read(cms_system: str, claim_path: str) -> Dict[str, str]:
    """Get a dictionary of raw data and clinical column names for a given file."""
    known_facilities = [
        "inpatient",
        "outpatient",
        "hospice",
        "hha",
        "bcarrier",
        "all_states_ip",
        "ot",
    ]

    # extract the facility name from the stem
    file_name = Path(claim_path).stem
    facility = [fac for fac in known_facilities if fac in file_name]
    if len(facility) != 1:
        raise ValueError(f"Identifying the correct facility for {file_name} failed.")
    facility_name = facility[0]

    if cms_system == "mdcr":
        claim_col = [
            "CLM_ID as claim_id",
            "CLM_FROM_DT as clm_from_dt",
            "CLM_THRU_DT as clm_thru_dt",
        ]
    else:
        claim_col = [
            "clm_id as claim_id",
            "SRVC_BGN_DT as clm_from_dt",
            "SRVC_END_DT as clm_thru_dt",
        ]

    cols = (
        claim_col
        + constants.icd_col_names[
            constants.cms_system_icd_lookup[f"{cms_system}_{facility_name}"]
        ]
    )
    col_dict = funcs.convert_db_col_list(cols)
    return col_dict


def transform_to_icd_claims(
    df: ps.DataFrame,
    cms_system: str,
    claim_path: str,
    year: int,
    review_icd_codes: bool = False,
) -> Tuple[ps.DataFrame, ps.DataFrame]:
    """Replace placeholder nulls with true nulls in max data, create a tabulation of ICD codes
    for validation, assign code system id and reshape the diagnosis columns from wide to long.
    """
    if cms_system == "max":
        # Replace placeholder null ICD codes with true nulls
        max_null_ph = ["00000000", "8888888", "0", "00000"]
        df = funcs.coerce_nulls(df=df, null_placeholders=max_null_ph)
        logging.info("nulls removed")

    diag_cols = df.filter(regex="dx_").columns.tolist()
    tabdf = funcs.tab_dx(df, diag_cols, pyspark=True)
    tabdf.sort_values("dx_count", ascending=False, inplace=True)
    logging.info(f"tabdf created. Most common codes are \n{tabdf.head(10)}")

    # make a datetime object column
    if year == 2015:
        df["date"] = ps.to_datetime(df["clm_thru_dt"], infer_datetime_format=True)
        logging.info("finished processing date col for 2015")

    # Identify the code system ID using CLM_THRU_DT and heuristic
    df = funcs.assign_code_sys_by_date(df, col="date", year=year)
    logging.info("Code system IDs have been assigned based on date")

    # reshape from long to wide on ICD code. claim_id and dx_id identify rows
    df = funcs.reshape_dx(df)
    logging.info("The reshape from wide to long is complete")

    if review_icd_codes:
        # we expect only certain ICD chapters in e-codes. Flag those that don't
        # match and then write them to LU for review
        output_path = "FILEPATH"
        code_dict = exp_icd_codes()
        df = funcs.flag_odd_code_sys(df, code_dict, output_path)
        logging.warning(
            (
                f"There are {len(df[df.code_sys_mismatch])} of {len(df)} rows with "
                "odd ICD codes for a given code system"
            )
        )

        df.drop("code_sys_mismatch", 1, inplace=True)

    # add year after reshaping data, agnostic of data pull method (db or parquet)
    df["year_id"] = year

    validate_transformed_icd_claims(df, tabdf)
    return df


def validate_transformed_icd_claims(df: ps.DataFrame, tabdf: ps.DataFrame) -> None:
    diff = set(constants.ICD_ECODE_COLUMNS).symmetric_difference(set(df.columns))
    if diff:
        e = f"We expect exactly 4 columns ({constants.ICD_ECODE_COLUMNS}). There is a setdiff of {diff}"
        logging.error(e)
        raise ValueError(e)

    df.sort_values(constants.ICD_ECODE_COLUMNS, inplace=True)

    funcs.validate_dx(df, tabdf)
    logging.info("Tabulated Diagnosis Test has passed")


def write_data(df: ps.DataFrame, claim_path: str) -> None:
    write_path = "FILEPATH"

    funcs.parquet_writer(df=df, path=write_path, mode="overwrite")

    logging.info("A file has been written, overwritting an existing file if necessary.")


if __name__ == "__main__":
    clinical_env_path = "FILEPATH"
    os.environ["PATH"] = "FILEPATH"

    arg_parser = argparse.ArgumentParser(description="Create ICD claims tables")
    arg_parser.add_argument(
        "--claim_path",
        help=(
            "Identifies the input file, and if relevant, which piece of the input to "
            "convert to an ICD claims table. eg 'max_ot_1_of_5_ca'"
        ),
        type=str,
        required=True,
    )
    arg_parser.add_argument(
        "--year", help="Year of input file to process", type=int, required=True
    )
    args = arg_parser.parse_args()

    funcs.validate_year_in_path(year=args.year, claim_path=args.claim_path)

    tmp_dir = filepath_parser(
        ini="pipeline.cms", section="icd_mart", section_key="spark_tmp_dir"
    )

    # Initialize Spark config which will get passed to the Spark context
    conf = SparkConf()
    conf.set("spark.driver.memory", "MEM")
    conf.set("spark.local.dir", tmp_dir)

    # Set the max number of cores Spark can use in local mode
    conf.setMaster("local[15]")
    sc = SparkContext(conf=conf)

    cms_system = funcs.get_cms_system_from_path(args.claim_path)
    WRITE_DIR = "FILEPATH"

    logname = time.strftime(f"{Path(args.claim_path).stem}_%Y_%m_%d-%H-%M.log")
    logpath = "FILEPATH"
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-10s %(processName)s  %(name)s %(message)s",
        filename=logpath,
    )
    logging.info(f"beginning on year {args.year}")

    print(os.environ)
    logging.info(os.environ)

    cols_dict = prep_for_read(cms_system=cms_system, claim_path=args.claim_path)
    df = get_raw_data(claim_path=args.claim_path, cols_dict=cols_dict)

    df = transform_to_icd_claims(
        df=df, cms_system=cms_system, claim_path=args.claim_path, year=args.year
    )

    write_data(df=df, claim_path=args.claim_path)

    sc.stop()
