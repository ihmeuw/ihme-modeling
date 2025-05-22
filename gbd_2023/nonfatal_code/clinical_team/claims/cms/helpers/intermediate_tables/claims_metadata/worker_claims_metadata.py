import argparse
import re
from pathlib import Path

import pandas as pd
from crosscutting_functions.clinical_constants.pipeline import cms as constants
from crosscutting_functions.clinical_metadata_utils.parser import filepath_parser
from pyarrow import parquet

from cms.helpers.intermediate_tables import funcs, schemas
from cms.utils.database import database


def read_parquet_file(cms_system: str, read_path: str) -> pd.DataFrame:
    """Currently unused but we can swap this function into create_claims_metadata in the future
    when we convert the raw data to parquet.

    Get a list of file columns, list of read columns. Find intersection. Read the Parquet file
    and return data
    """
    path_glob = Path(read_path).glob("*.parquet")
    partition_path = [p for p in path_glob][0]
    data_columns = parquet.read_schema(partition_path, memory_map=True).names
    columns = list(set(constants.RENAME_DICT[cms_system].keys()).intersection(data_columns))

    return pd.read_parquet(path=read_path, columns=columns)


def fill_null_dates(df: pd.DataFrame, preferred_col: str, backup_col: str) -> pd.DataFrame:
    """We noticed that many of these date column values are null. In cases where the preferred
    or higher quality col is null we will replace it with the value from the backup column.
    """
    df.loc[df[preferred_col].isnull(), preferred_col] = df.loc[
        df[preferred_col].isnull(), backup_col
    ]
    return df


def dedup_date_cols(df: pd.DataFrame, claim_path: str) -> pd.DataFrame:
    """If columns for both an admission or discharge date as well as a claim from or thru
    date exist then drop the claim from/thru date after filling any nulls."""

    is_hha = "hha_base_claims" in claim_path
    cols = df.columns
    if "NCH_BENE_DSCHRG_DT" in cols and "CLM_THRU_DT" in cols:
        df = fill_null_dates(
            df=df, preferred_col="NCH_BENE_DSCHRG_DT", backup_col="CLM_THRU_DT"
        )
        df = df.drop("CLM_THRU_DT", axis=1)

    if "CLM_ADMSN_DT" in cols and "CLM_FROM_DT" in cols:
        if is_hha:
            # this is the date of initial service for hha, rather than encounter date
            df = df.drop("CLM_ADMSN_DT", axis=1)
        else:
            df = fill_null_dates(df=df, preferred_col="CLM_ADMSN_DT", backup_col="CLM_FROM_DT")
            df = df.drop("CLM_FROM_DT", axis=1)

    if "ADMSN_DT" in cols and "SRVC_BGN_DT" in cols:
        df = fill_null_dates(df=df, preferred_col="ADMSN_DT", backup_col="SRVC_BGN_DT")
        df = df.drop("SRVC_BGN_DT", axis=1)

    if "CLM_HOSPC_START_DT_ID" in cols and "CLM_FROM_DT" in cols:
        df = fill_null_dates(
            df=df, preferred_col="CLM_HOSPC_START_DT_ID", backup_col="CLM_FROM_DT"
        )
        df = df.drop("CLM_FROM_DT", axis=1)

    return df


def rename_cols(df: pd.DataFrame, cms_system: str, claim_path: str) -> pd.DataFrame:
    """Change raw data column names to the values we use in our intermediate schema. The rename
    dictionary contains duplicate values and some raw data tables contain both of the keys
    associated with a single final column. One of the overlapping columns is dropped."""
    cols = df.columns
    df = dedup_date_cols(df, claim_path)
    source_names = constants.RENAME_DICT[cms_system]
    df = df.rename(columns=source_names)

    renamed_cols = df.columns
    if len(renamed_cols) != len(set(renamed_cols)):
        raise ValueError(
            f"Duplicate columns present after renaming. Original columns: {cols}. "
            f"Renamed columns: {renamed_cols}"
        )

    return df


def clean_fac_zip_codes(df: pd.DataFrame) -> pd.DataFrame:
    """Convert raw data facility zip codes to zero filled strings."""
    fac_zip_col = "clm_srvc_fac_zip_cd"
    if fac_zip_col in df.columns:
        df[fac_zip_col] = pd.to_numeric(df[fac_zip_col], errors="coerce")
        df.loc[df[fac_zip_col].notnull(), fac_zip_col] = (
            df.loc[df[fac_zip_col].notnull(), fac_zip_col].astype(int).astype(str)
        )
        df[fac_zip_col] = df[fac_zip_col].str.zfill(9)
    return df


def convert_dates(df: pd.DataFrame, year: str) -> pd.DataFrame:
    """Convert date columns to datetime."""
    date_cols = ["service_start", "service_end"]
    for col in date_cols:  # cast potential numeric cols to strings
        col_dtype = str(df[col].dtype)
        if "int" in col_dtype or "float" in col_dtype:
            df.loc[df[col].notnull(), col] = (
                df.loc[df[col].notnull(), col].astype(int).astype(str)
            )
        df[col] = pd.to_datetime(df[col], errors="coerce")
    df = funcs.date_formatter(df=df, columns=date_cols)

    year_counts = (
        df["service_start"].str[0:4].value_counts(dropna=False) / len(df)
    ).reset_index()
    out_of_year_pct = year_counts[year_counts["index"] != year].service_start.sum() * 100
    if out_of_year_pct > 15:
        raise RuntimeError(
            f"We're seeing too many service start dates for a different year {year_counts}"
        )
    return df


def convert_to_floats(df: pd.DataFrame) -> pd.DataFrame:
    """Convert null containing columns to float from str or int"""
    float_cols = [
        "cms_facility_id",
        "month_id",
        "patient_status_cd",
        "ptnt_dschrg_stus_cd",
    ]
    for float_col in float_cols:
        if float_col in df.columns:
            df[float_col] = pd.to_numeric(df[float_col], errors="coerce").astype(float)
    return df


def get_file_source_ids() -> pd.DataFrame:
    """File source IDs connect a row of data to the raw data file it was derived from. This
    pulls the lookup table from the CMS database which is used to attach these values.
    """
    file_source_query = """
    QUERY
    """
    cms = database.CmsDatabase()
    cms.load_odbc()
    return cms.read_table(file_source_query)


def assign_file_source_id(df: pd.DataFrame, table: str) -> pd.DataFrame:
    """Use the clinical_lookup table to pull and attach a file source id value to the data.

    Raises: ValueError if unable to filter file source table to a single row."""
    # remove trailing split file suffix if present
    file_name = re.sub("_00[0-9]$", "", table)

    file_source_lookup = get_file_source_ids()
    tmp = file_source_lookup.query(f"file_source_name == '{file_name}'")
    file_source_id = tmp["file_source_id"].iloc[0]
    if len(tmp) != 1:
        raise ValueError(f"Unable to find a suitable ID for {file_name} within {tmp}")
    df["file_source_id"] = file_source_id
    return df


def attach_month_from_service_start(df: pd.DataFrame) -> pd.DataFrame:
    """The schema requires a separate month_id column. We extract this from the service
    start date.
    """
    # This relies implicitly on the structure of funcs.date_formatter
    df["month_id"] = pd.to_numeric(df.service_start.str.split("-", expand=True)[1])
    if df["month_id"].max() > 12 or df["month_id"].min() < 1:
        raise ValueError("Months were not created correctly")

    return df


def add_facility_ids(df: pd.DataFrame, src: str) -> pd.DataFrame:
    """Add facility related columns if they weren't renamed from raw data columns.
    """
    facility_cols = ["clm_srvc_fac_zip_cd", "cms_facility_id"]
    # facility zip code does not exist in medicaid max data
    for facility_col in facility_cols:
        if facility_col not in df and not (
            facility_col == "clm_srvc_fac_zip_cd" and src == "max"
        ):
            df[facility_col] = None
    return df


def add_missing_outcome_col(df: pd.DataFrame, cms_system: str, table: str) -> pd.DataFrame:
    """MDCR carrier and MAX outpatient files do not have any type of outcome variables. Add
    These to the data with null values."""
    outcome_col_name = constants.OUTCOME_COL_NAMES[cms_system]
    have_no_col_pattern = constants.NO_OUTCOME_COL_TABLE_PATTERN[cms_system]

    if outcome_col_name not in df.columns:
        if have_no_col_pattern not in table:
            raise ValueError(
                f"The outcome column {outcome_col_name} should be present in the "
                f"table {table} but was not found. Only table names containing the pattern "
                f"{have_no_col_pattern} for this cms_system should be missing "
                f"{outcome_col_name}"
            )
        df[outcome_col_name] = None

    return df


def create_claims_metadata(claim_path: str, cms_system: str, year: int) -> pd.DataFrame:
    """Handles all steps required to create the claims metadata table in memory."""
    # Pull the data from a raw data source
    df = read_parquet_file(cms_system=cms_system, read_path=claim_path)

    # Rename to our standard columns, resolve duplicate column names
    df = rename_cols(df, cms_system, claim_path)

    # Generate new columns which were not already present in the raw data
    table = Path(claim_path).stem
    df = add_facility_ids(df, cms_system)
    df = assign_file_source_id(df, table)
    df["year_id"] = year
    df = convert_dates(df, str(year))
    df = attach_month_from_service_start(df)
    df = add_missing_outcome_col(df=df, cms_system=cms_system, table=table)

    df = convert_to_floats(df)
    df = clean_fac_zip_codes(df)
    validate_data(df=df, cms_system=cms_system)
    return df


def validate_data(df: pd.DataFrame, cms_system: str) -> None:
    """Enforce a defined schema on worker data and confirm df is not empty."""
    if df.empty:
        raise RuntimeError("No data is present.")

    schemas.SCHEMAS[f"{cms_system}_claims_metadata"].validate(df)


if __name__ == "__main__":
    arg_parser = argparse.ArgumentParser(
        description="Create claims_metadata intermediate table from raw data"
    )
    arg_parser.add_argument(
        "--cms_system",
        help=("Identifies the CMS source to use when creating a claims metadata table."),
        choices=constants.cms_systems,
        type=str,
        required=True,
    )
    arg_parser.add_argument(
        "--claim_path", help="Identifies the table(s) to process", type=str, required=True
    )
    arg_parser.add_argument(
        "--year", help="Year of input file to process", type=int, required=True
    )
    arg_parser.add_argument(
        "--memory_gb", help="Slurm memory request in Gb for this job", type=int, required=False
    )
    arg_parser.add_argument(
        "--cores", help="Slurm core request for this job", type=int, required=False
    )
    args = arg_parser.parse_args()

    meta_path = filepath_parser(
        ini="pipeline.cms", section="table_outputs", section_key="claims_metadata"
    )
    WRITE_PATH = "FILEPATH"

    funcs.validate_year_in_path(year=args.year, claim_path=args.claim_path)

    df = create_claims_metadata(
        claim_path=args.claim_path, cms_system=args.cms_system, year=args.year
    )

    if not isinstance(df, pd.DataFrame):
        raise TypeError("The df is not a pandas dataframe!")

    funcs.parquet_writer(df=df, path=WRITE_PATH)
