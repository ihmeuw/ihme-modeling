"""
Main worker script to convert a .csv to .parquet using the
pandas API on pyspark.
"""

import argparse
import os
import random
from pathlib import Path

import pandas as pd
import pyspark.pandas as ps
import pyspark.sql.functions as funs
from crosscutting_functions.clinical_constants.pipeline import cms as constants
from crosscutting_functions.clinical_metadata_utils.parser import filepath_parser
from config_spark.initialize import ci_spark


def get_cms_args() -> dict:
    """Grabs sys arguments and validates they fit expectation."""

    par = argparse.ArgumentParser()
    par.add_argument("--file_path", type=str, help="Path to a CMS .csv file.")
    par.add_argument("--worker_mem", type=int, help="Allocation for worker memory.")
    par.add_argument("--test", type=str, help="Flag to indicate testing type or none.")
    a = par.parse_args()

    # Confirm arg is csv fn-like
    if a.file_path[-4:] != ".csv":
        raise ValueError(f"{a.file_path} does not point to a .csv")

    dir_loc = os.path.dirname(a.file_path)
    # mdcr has '*_conversions' sub dir where csv are located.
    # Goal for max and mdcr Ex: "FILEPATH"
    if dir_loc[-11:] == "FILEPATH":
        dir_loc = os.path.dirname(dir_loc)
    # all state max ip has'maxdata_all_states_ip_* sub dir where csv is located.'
    if "_all_states_ip_" in dir_loc:
        dir_loc = os.path.dirname(dir_loc)

    year_id = "FILEPATH"
    request_id = "FILEPATH" 

    args = {}
    args["file_path"] = a.file_path
    args["directory"] = dir_loc
    args["request_id"] = int(request_id)
    args["year_id"] = int(year_id)
    args["worker_mem"] = a.worker_mem
    args["test"] = a.test

    print(f"Formatted arg dict: {args} \n")

    return args


def make_parquet_dir(args: dict) -> str:
    """Checks if parquet conversion directory exists.
    If it does not already exist, will create the directory.
    Returns the path to the parquet conversion directory.
    """

    parquet_path = "FILEPATH"
    print(f"Checking if {parquet_path} exists \n")
    par_dir = os.path.isdir(parquet_path)

    if not par_dir:
        os.mkdir(parquet_path)
        print(f"Making new parquet file directory at {parquet_path} \n")
    else:
        print(f"Path {parquet_path} already exists. \n")

    return parquet_path


def add_clm_id(df: ps.DataFrame) -> ps.DataFrame:
    """Takes in a table and adds a column of uuid to the input table.
    The new column is built using spark uuid function.
    https://spark.apache.org/docs/2.3.0/api/sql/index.html#uuid
    New column is labeled as 'clm_id'
    """

    df = df.to_spark()
    df = df.withColumn("clm_id", funs.expr("uuid()"))
    df = df.pandas_api()

    if len(df.clm_id.unique()) != df.shape[0]:
        raise RuntimeError("Column 'clm_id' was not assigned properly.")

    return df


def csv_to_parquet(args: dict, parquet_path: str) -> None:
    """Reads in .csv from *_conversions path, rewrites as .parquet file to 'parquet_path'."""
    # Build path to csv file for mdcr and max.
    # Read either max or mdcr .csv into pandas on spark df.

    file_name = Path(args["file_path"]).name

    if args["test"].lower() != "na":
        # Define to testing path.
        parquet_file_path = "FILEPATH"
    else:
        # Define prod path.
        parquet_file_path = "FILEPATH"

    # mdcr has headers on csv
    if args["request_id"] in constants.mdcr_req_ids:
        df = ps.read_csv(args["file_path"])

    elif args["request_id"] in constants.max_req_ids:
        # Max all state has headers on the csv
        if "_all_states_ip_" in file_name:
            df = ps.read_csv(args["file_path"])
            df = add_clm_id(df)

        # Max ps and ot files are missing headers.
        elif "_ot_" in file_name:
            df = ps.read_csv(args["file_path"], header=None)
            ot_headers = pd.read_csv(
                "FILEPATH"
            )
            df.columns = ot_headers["column"].to_list()
            # Max ot files do not have claim ID in the table. Create our own via a uuid.
            df = add_clm_id(df)

        elif "_ps_" in file_name:
            df = ps.read_csv(args["file_path"], header=None)
            ps_headers = pd.read_csv(
                "FILEPATH"
            )
            df.columns = ps_headers["column"].to_list()

    df.to_parquet(parquet_file_path, "overwrite")
    validate_parquet(og_df=df, parquet_file_path=parquet_file_path)


def validate_parquet(og_df: ps.DataFrame, parquet_file_path: str) -> None:
    """Compares two dataframes. From pyspark.pandas objects, sorts and
    samples random indices to compare. After sample, converts to pandas
    objects and executes a pandas.testing.assert_frame_equal method.
    og_df: the original .csv loaded dataframe.
    parquet_file_path: path to the parquet converted og_df.
    """

    # Intake as pandas.pyspark object to be same as og_df
    parquet_df = ps.read_parquet(parquet_file_path)

    # read csv assigned int col names, but stored as strings in parquet.
    cols = parquet_df.columns
    og_df.columns = cols

    # Sort by all columns
    print("Sorting tables by all columns. \n")
    og_df = og_df.sort_values(by=cols).reset_index(drop=True)
    parquet_df = parquet_df.sort_values(by=cols).reset_index(drop=True)

    # Sample sorted df
    print("Sampling tables to assert they are the same. \n")
    row_count = og_df.shape[0]
    indices = range(0, row_count)
    # Some files have a very small row count, ex: demo_codes
    if row_count < 10000:
        quantity = row_count
    else:
        quantity = 10000
    sample_index = random.sample(indices, quantity)
    og_df_sample = og_df.iloc[sample_index, :]
    parquet_df_sample = parquet_df.iloc[sample_index, :]
    del og_df
    del parquet_df

    # Convert to vanilla pandas to compare df's
    print("Testing pandas dataframes. \n")
    pandas_og_sample = og_df_sample.to_pandas()
    pandas_parquet_sample = parquet_df_sample.to_pandas()

    # Comparing dfs
    pd.testing.assert_frame_equal(pandas_og_sample, pandas_parquet_sample)

    print("Success: DataFrames are equal. \n")


if __name__ == "__main__":

    clinical_env_path = "FILEPATH"
    os.environ["PATH"] = "FILEPATH"

    args = get_cms_args()
    spark = ci_spark(memory_gb=args["worker_mem"], tmp_dirkey="cms", name="csv_to_parquet")
    parquet_path = make_parquet_dir(args=args)
    csv_to_parquet(args=args, parquet_path=parquet_path)
    spark.sparkContext.stop()
