"""
Collection of functions to use for processing the CMS data from the staging
to Intermediate tables

So far this is just code relevant to the e-code claims and icd-code claims
tables
"""
import datetime
import glob
import os
import subprocess
import time
import warnings
from pathlib import Path
from typing import Dict, List, Optional, Union

import numpy as np
import pandas as pd
from crosscutting_functions.clinical_constants.pipeline import cms as constants
from crosscutting_functions.clinical_metadata_utils.parser import filepath_parser
from pyspark import pandas as ps

from cms.helpers.intermediate_tables import schemas
from cms.utils import cms_path, get_csv_paths
from cms.utils.transformations.wide_to_long import wide_to_long


def get_claims_data(
    read_path: str, cols_dict: Dict[str, str], pyspark: bool = False
) -> Union[pd.DataFrame, ps.DataFrame]:
    """Wrapper to just get ICD/E coded data from a given input file."""
    raw_data_cols = list(cols_dict.keys())
    if pyspark:
        df = ps.read_parquet(read_path, columns=raw_data_cols)
    else:
        df = pd.read_parquet(read_path, columns=raw_data_cols)

    df = df.rename(columns=cols_dict)
    validate_claim_id(df)
    return df


def flag_odd_code_sys(df, code_dict, output_path=None):
    """ICD codes should start with certain letters depending on code system
    Flag rows after the reshape that don't match these letters
    """

    df["cause_code_first"] = df["cause_code"].str[0:1]
    df["code_sys_mismatch"] = False
    for code_sys, code_firsts in code_dict.items():
        cond = "(df.code_system_id == code_sys) & (~df.cause_code_first.isin(code_firsts))"
        df.loc[eval(cond), "code_sys_mismatch"] = True

    print(
        (
            f"There are {len(df[df.code_sys_mismatch])} of {len(df)} rows with "
            "odd codes for a given code system"
        )
    )

    if output_path:
        df[df.code_sys_mismatch].to_csv(output_path, index=False)

    return df.drop("cause_code_first", axis=1)


def reshape_dx(df):
    """
    wraps around the eda transformer and makes some small adjustments
    """
    id_cols = ["claim_id", "code_system_id"]

    diagnosis_vars = df.columns[df.columns.str.startswith("dx_")].tolist()
    df = wide_to_long(df, wide_cols=diagnosis_vars, id_cols=id_cols, review_group_counts=False)
    df.rename(columns={"order": "diagnosis_id", "col": "cause_code"}, inplace=True)

    df["diagnosis_id"] = df["diagnosis_id"].str.replace("dx_", "")
    df["diagnosis_id"] = df["diagnosis_id"].astype(int)

    return df


def assign_code_sys_by_date(df, col, year):
    """Use a date column to assign the ICD code system to CMS data. The US
    switched from ICD 9 to ICD 10 on October 1st 2015"""
    if year <= 2014 and year > 1975:
        df["code_system_id"] = 1
    elif year == 2015:
        if "datetime" not in str(df[col].dtype):
            df[col] = pd.to_datetime(df[col])

        switch_day = pd.Timestamp(datetime.date(2015, 10, 1))

        df["code_system_id"] = None
        df.loc[df.date < switch_day, "code_system_id"] = 1
        df.loc[df.date >= switch_day, "code_system_id"] = 2
    elif year >= 2016 and year < 2023:
        df["code_system_id"] = 2
    else:
        raise ValueError(f"This function can't handle year {year}")

    if df.code_system_id.isnull().any():
        raise ValueError("There are null code system ids!")

    df["code_system_id"] = df["code_system_id"].astype(int)

    return df


def safe_writer(df, write_path):
    """Fail if a CSV already exists"""
    if os.path.exists(write_path):
        raise ValueError(
            f"This script will NOT currently overwrite existing "
            f"data. You must manually delete {write_path} to "
            f"continue"
        )
    if Path(write_path).suffix in (".csv", ".CSV"):
        df.to_csv(write_path, index=False)
    elif Path(write_path).suffix in (".parquet", ".pq"):
        df.to_parquet(write_path)
    return


def claims_table_maker(cms_system: str, year: int) -> str:
    """
    Get the claims staging tables for a given source and year
    """
    db_table = DB_TABLE

    return db_table


def validate_claim_id(df):
    """Claim id must be a unique value by row"""
    if len(df) != df.claim_id.unique().size:
        raise ValueError("This df does not have unique claim_ids")
    return


def tab_dx(
    df: Union[pd.DataFrame, ps.DataFrame], diag_cols: List[str], pyspark: bool
) -> Union[pd.DataFrame, ps.DataFrame]:
    """tabulate dx in wide form"""
    tmp_list = []
    for col in diag_cols:
        tmp = df[col].value_counts().reset_index()
        tmp.columns = ["cause_code", "dx_count"]
        tmp_list.append(tmp)
    if pyspark:
        tabdf = ps.concat(tmp_list, sort=False, ignore_index=True)
    else:
        tabdf = pd.concat(tmp_list, sort=False, ignore_index=True)
    tabdf = tabdf.groupby("cause_code").dx_count.sum().reset_index()

    return tabdf


def validate_dx(long, tabdf, breaktest=False):
    if breaktest:
        tabdf.loc[2, "cause_code"] = ""
    longtab = long.cause_code.value_counts().reset_index()
    longtab.columns = ["cause_code", "dx_count"]

    m = longtab.merge(tabdf, how="outer", on="cause_code", suffixes=("_long", "_tab"))
    if not (m["dx_count_long"] == m["dx_count_tab"]).all():
        faildf = m[m.dx_count_long != m.dx_count_tab]
        raise ValueError(f"Tabbed cause code validation has failed {faildf}")

    return


def write_token(cms_system, year, clm):
    """Write an empty file to a token folder to show that a job
    has finished querying the db."""
    write_dir = "FILEPATH"
    pd.DataFrame().to_csv("FILEPATH")
    return


def limiter(qsub_list, token_dir, jobs_at_once=3, verbose=False):
    """Limit how many qsubs are able to actively query a database at one time
    Takes a list of qsub commands in string form.

    This requires each worker to write a .csv to the 'token_dir' after they've
    finished their query. It doesn't matter what's in the csv file
    The limiter automatically removes all existing token CSVs when called
    """

    jobs = 0
    finished_jobs = []
    tokens = glob.glob("FILEPATH")
    [os.remove(t) for t in tokens]
    status = "go"
    for qsub in qsub_list:
        running_jobs = jobs - len(finished_jobs)
        if verbose:
            print(f"There are {running_jobs} jobs running")

        if running_jobs >= jobs_at_once:
            status = "wait"
            while status == "wait":
                time.sleep(60)
                finished_jobs = glob.glob(f"{token_dir}/*.csv")
                running_jobs = jobs - len(finished_jobs)
                if running_jobs < jobs_at_once:
                    status = "go"
                    print(qsub)
                    subprocess.call(qsub, shell=True)
                    jobs += 1
        else:
            print(qsub)
            subprocess.call(qsub, shell=True)
            jobs += 1
            status = "go"
    return


def coerce_nulls(df, null_placeholders):
    dx_cols = df.filter(regex="dx_").columns.tolist()
    for col in dx_cols:
        df.loc[df[col].isin(null_placeholders), col] = None

    return df


def make_year_list(list_string: str) -> List[int]:
    """Takes an year list represented as a string and converts to numeric list.
    Ex: '[2000, 2010, 2014]' -> [2000, 2010, 2014]
    Step 1: grab all string characters to right of bracket '['
    Step 2: grab all string characters to left of bracket ']'
    Step 3: split string into segments based on comma and make into list
    Step 4: convert all string representations to integers.
    """

    year_list = list_string.split("[")[1].split("]")[0].split(",")
    for i in year_list:
        assert len(i.strip()) == 4, "Values need to be year-like by having 4-digits."
    numeric_years = [int(yr) for yr in year_list]
    numeric_years = np.sort(numeric_years).tolist()

    return numeric_years


def validate_processing_group_name(processing_group_name: str):
    """Processing group names for ICD claims are generated by combining a `cms_system` ("mdcr"
    or "max") and a facility type, stored in constants. Additional text is appended onto the
    name to identify how many pieces to split the raw data into and which piece a specific
    task should process.
    Raises a value error if a valid name type is not in the processing group name."""
    valid_group_name_types = []
    for cms_system in constants.cms_systems:
        for fac_type in getattr(constants, f"{cms_system}_fac_types"):
            valid_group_name_types.append(f"{cms_system}_{fac_type}")

    if not any([valid_name in processing_group_name for valid_name in valid_group_name_types]):
        raise ValueError(
            "Processing groups must contain a cms system and facility. The group "
            f"{processing_group_name} did not contain any of the following valid "
            f"values {valid_group_name_types}"
        )


def date_formatter(
    df: Union[pd.DataFrame, ps.DataFrame], columns: Optional[List[str]] = None
) -> Union[pd.DataFrame, ps.DataFrame]:
    """Adjusts datetime columns in a DataFrame to be in the format YYYY-MM-DD.

    Args:
        df (Union[pd.DataFrame, ps.DataFrame]): DataFrame to format.
        columns (Optional[List[str]], optional): Specific columns of datetime like values to convert. Defaults to None.
            If None, will attempt to infer datetime columns to convert via dtype inspection.

    Raises:
        RuntimeError: If any rows were lost or gained.

    Returns:
        Union[pd.DataFrame, ps.DataFrame]: Input DataFrame with any dates as formatted strings.
    """

    start_shape = df.shape
    fmt = "%Y-%m-%d"

    if not isinstance(df, pd.DataFrame) and not isinstance(df, ps.DataFrame):
        warnings.warn(
            "DataFrame is not a pandas-like DataFrame. May produce unexpected date results."
        )

    # Infer date columns if not supplied.
    if not columns:
        columns = [col for col in df.columns if "datetime" in str(df[col].dtype)]
        if len(columns) < 1:
            warnings.warn(
                "No datetime columns inferred and none were passed. \nReturning input table."
            )
            return df

    else:
        for col in columns:
            col_dtype = str(df[col].dtype)
            if "int" in col_dtype or "float" in col_dtype:
                raise RuntimeError(
                    "This function will not perform as expected with numeric column data types."
                )
            if "datetime" not in col_dtype:
                if isinstance(df, pd.DataFrame):
                    # initial conversion to datetime by inferring the date formatting
                    df[col] = pd.to_datetime(df[col], errors="coerce")
                elif isinstance(df, ps.DataFrame):
                    df[col] = ps.to_datetime(df[col], errors="coerce")
                else:
                    raise TypeError(f"Expected a pandas like DataFrame, received {type(df)}")

    for col in columns:
        df[col] = df[col].dt.strftime(date_format=fmt)

        # Replace null with None
        if isinstance(df, pd.DataFrame):
            df[col] = df[col].where(pd.notnull(df[col]), None)
        elif isinstance(df, ps.DataFrame):
            df[col] = df[col].where(ps.notnull(df[col]), None)
        else:
            raise TypeError(f"Expected a pandas like DataFrame, received {type(df)}")

    end_shape = df.shape

    if end_shape != start_shape:
        raise RuntimeError(f"Expected shape of {start_shape}, got {end_shape}")

    return df


def create_claims_filepaths(
    cms_systems: List[str],
    research_ids: List[int],
    mdcr_request_ids: List[int],
    max_request_ids: List[int],
    mdcr_years: List[int],
    max_years: List[int],
) -> List[str]:

    claim_parquet_files = "FILEPATH"

    return claim_parquet_files


def convert_db_col_list(cols: List[str]) -> Dict[str, str]:
    """constants stores a list of diagnosis tables along with sql syntax to change the column
    names to a preferred clinical name. This functions manipulates those lists and returns a
    dictionary with old column names as keys and clinical column names as values. This can be
    passed to pandas rename method.
    """
    raw_names = []
    clinical_names = []
    for col in cols:
        pieces = col.split(" as ")
        raw_names.append(pieces[0])
        if len(pieces) > 1:
            clinical_names.append(pieces[1])
        else:
            clinical_names.append(pieces[0])

    return dict(zip(raw_names, clinical_names))


def validate_year_in_path(year: int, claim_path: str) -> None:
    """Given an absolute path to a claims file and a year validate that the path contains the
    string representation of year.

    Raises: Value error if year value not present in file path
    """
    if str(year) not in claim_path:
        raise ValueError(
            "The year arg and year value in the filepath must match. Unable to "
            "find year {year} in this file path {claim_path}"
        )


def get_cms_system_from_path(claim_path: str) -> str:
    """Given an input filepath use patterns in the path to identify cms system."""
    if constants.mdcr_claim_pattern in claim_path:
        cms_system = "mdcr"
    elif constants.max_claim_pattern in claim_path:
        cms_system = "max"
    else:
        raise ValueError(f"Unable to determine CMS system values for {claim_path}")
    return cms_system


def validate_data(df: pd.DataFrame, cms_system: str, table: str) -> None:
    """Enforce a defined schema on worker data.  Will allow an empty table to pass if it has the correct
    column names. Some bene_groups to not have all cms_system years, but need to write an empty df for compilation.

    Args:
        df (pd.DataFrame): Table to validate schema.
        cms_system (str): Abbreviation for the CMS system. Ex: 'mdcr'
        table (str): Name of table. Ex: 'demo', 'five_percent_sample'.

    Raises:
        RuntimeError: If an empty table has incorrect column names.
    """

    if df.empty:
        col_names = schemas.SCHEMAS[f"{cms_system}_{table}"].columns.keys()
        for col in df.columns:
            if col not in col_names:
                raise RuntimeError(f"{col} not found in schema. Expected one of {col_names}")

    schemas.SCHEMAS[f"{cms_system}_{table}"].validate(df)


def _pandas_writer(df: pd.DataFrame, path: str, **kwargs) -> None:
    """
    Wrapper for pd.to_parquet().

    Can add more default args that we see fit. Currently index is always False

    Args:
        df: pd.DataFrame()
        path: Location where df to written to.
    """

    df.to_parquet(path=path, index=False, **kwargs)


def _pyspark_writer(df: ps.DataFrame, path: str, **kwargs) -> None:
    """
    Wrapper for ps.to_parquet().

    Can add more default args that we see fit.

    Args:
        df: ps.DataFrame()
        path: Location where df to written to.
    """
    df.to_parquet(path=path, **kwargs)


def parquet_writer(df: Union[pd.DataFrame, ps.DataFrame], path: str, **kwargs) -> None:
    """
    Wrapper for either pd.to_parquet() or ps.to_parquet().
    Determines whether the df is pd or ps first as they may require very different args.

    Args:
        df: either pd.DataFrame or ps.DataFrame.
        path: Location where df to written to.
        **kwargs: other keyword arguments that may be needed.
    """
    if isinstance(df, pd.DataFrame):
        _pandas_writer(df=df, path=path, **kwargs)
    elif isinstance(df, ps.DataFrame):
        _pyspark_writer(df=df, path=path, **kwargs)
    else:
        raise ValueError("The df to be saved needs to be either pd or ps.DataFrame")
