import argparse
from typing import Dict

import pandas as pd
from crosscutting_functions.clinical_constants.pipeline.cms import all_processing_types
from crosscutting_functions.clinical_metadata_utils.parser import filepath_parser
from config_spark.initialize import ci_spark
from pyspark.sql import DataFrame, SparkSession

from cms.helpers.intermediate_tables.icd_mart import process_sources
from cms.src.pipeline.lib import manage_icd_mart_data


def get_paths(
    process_source: process_sources.SourceProcessing, out_type: str
) -> Dict[str, str]:
    """Create a dictionary of filepaths to use when reading data"""

    if out_type == "test":
        input_dir = filepath_parser(
            ini="pipeline.cms", section="testing", section_key="compiled_parquet"
        )
        output_dir = filepath_parser(
            ini="pipeline.cms", section="testing", section_key="icd_mart_output"
        )

        assert "test" in str(input_dir) and "test" in str(input_dir)

    elif out_type == "prod":
        input_dir = filepath_parser(
            ini="pipeline.cms", section="table_outputs", section_key="compiled_parquet"
        )
        output_dir = filepath_parser(
            ini="pipeline.cms", section="icd_mart", section_key="icd_mart_output"
        )

        assert "production" in str(input_dir) and "production" in str(output_dir)
    else:
        raise ValueError(f"out_type: {out_type} is not supported.")

    # pull in a list of sources from constants
    path_dict = {
        # Data
        "claims_metadata_path": "FILEPATH",
        "eligibility_path": "FILEPATH",
        "race_path": "FILEPATH",
        "demographic_path": "FILEPATH",
        "icd_claims_path": "FILEPATH",
        "icd_group_path": "FILEPATH",
        "icd_mart_row_tracker": "FILEPATH",
    }

    if process_source.requires_sample:
        path_dict["sample_path"] = "FILEPATH",

    path_dict["write_path"] = "FILEPATH",

    # pyspark doesn't read Paths
    for name, path in path_dict.items():
        path_dict[name] = str(path)

    if out_type == "test":
        assert (
            "test" in path_dict["write_path"] and "production" not in path_dict["write_path"]
        )

    if "None" in path_dict["write_path"]:
        raise ValueError(f"The write path is invalid {path_dict['write_path']}")
    return path_dict


def create_icd_group_code(icd_claims: DataFrame) -> DataFrame:
    """Create the truncated 3 digit ICD code use to attach ID"""
    return icd_claims.withColumn("icd_group_code", icd_claims.cause_code.substr(1, 3))


def prepare_icd_claims(
    path_dict: Dict[str, str],
    spark: SparkSession,
) -> DataFrame:
    """Read the ICD claims data and prepare the 3 digit icd_group_code. Then use this to merge
    the icd_group_id column onto the data."""
    icd_claims = spark.read.parquet(path_dict["icd_claims_path"])

    icd_claims = create_icd_group_code(icd_claims)
    icd_claims = attach_icd_group(
        icd_claims=icd_claims, spark=spark, icd_group_path=path_dict["icd_group_path"]
    )

    return icd_claims


def attach_icd_group(
    icd_claims: DataFrame, spark: SparkSession, icd_group_path: str
) -> DataFrame:
    """Merge the icd_group_id column onto the data using the lookup"""
    # read in icd group ID lookup table
    icd_group_df = spark.read.parquet(icd_group_path)

    # merge them on by 3 digits and code system id
    icd_claims = icd_claims.join(
        icd_group_df, on=["icd_group_code", "code_system_id"], how="left"
    )

    # fill missing icd_group_ids with 0
    icd_claims = icd_claims.fillna(value=0, subset=["icd_group_id"])
    return icd_claims


def read_data(
    process_source: process_sources.SourceProcessing,
    path_dict: Dict[str, DataFrame],
    spark: SparkSession,
) -> Dict[str, DataFrame]:
    """Read all of the intermediate tables from Parquet files."""
    df_dict = {}

    df_dict["icd_claims"] = prepare_icd_claims(path_dict=path_dict, spark=spark)
    df_dict["claims_metadata"] = spark.read.parquet(path_dict["claims_metadata_path"])
    df_dict["race"] = spark.read.parquet(path_dict["race_path"])
    df_dict["eligibility"] = spark.read.parquet(path_dict["eligibility_path"])
    df_dict["demographic"] = spark.read.parquet(path_dict["demographic_path"])

    if process_source.requires_sample:
        df_dict["sample"] = spark.read.parquet(path_dict["sample_path"])

    return df_dict


def merge_tables(
    df_dict: Dict[str, DataFrame],
    process_source: process_sources.SourceProcessing,
    spark: SparkSession,
) -> DataFrame:
    """Merge together all of the CMS intermediate tables into a single DataFrame"""
    is_otp_lookup = spark.createDataFrame(
        get_file_source_lookup(cms_system=process_source.name)
    )

    row_tracker = {}

    bene_year = ["bene_id", "year_id"]
    df = df_dict["claims_metadata"].join(df_dict["race"], on=bene_year, how="left")
    row_tracker["meta_race_rows"] = df.count()
    df = df.join(is_otp_lookup, on=["file_source_id"], how="left")
    row_tracker["is_otp_lookup_rows"] = df.count()
    df = df.join(df_dict["icd_claims"], on=["claim_id", "year_id"], how="inner")
    row_tracker["mart_claims_rows"] = df.count()
    df = df.join(df_dict["eligibility"], on=bene_year + ["month_id"], how="inner")
    row_tracker["mart_elig_rows"] = df.count()
    df = df.join(df_dict["demographic"], on=bene_year, how="inner")
    row_tracker["mart_demo_rows"] = df.count()

    if process_source.requires_sample:
        df = df.join(df_dict["sample"], on=bene_year, how="inner")
        row_tracker["mart_sample_rows"] = df.count()
    return df, row_tracker


def validate_icd_mart(df: DataFrame) -> None:
    """Check to confirm that the ICD mart does not contain duplicate rows along a set of
    columns which should form a composite primary key."""
    rows = df.count()
    key_cols = ["bene_id", "claim_id", "year_id", "month_id", "diagnosis_id", "cause_code"]
    no_dupe_rows = df.dropDuplicates(subset=key_cols).count()
    duplicated_rows = rows - no_dupe_rows
    if duplicated_rows > 20_000:  # e-code tables in mdcr create a small number of dupes
        raise ValueError(
            f"There are  {duplicated_rows} duplicate rows in this ICD mart for these columns {key_cols}"
        )


def read_merge_write(
    process_source: process_sources.SourceProcessing, spark: SparkSession, out_type: str
) -> None:
    """Read intermediate table data, merge them into a single DataFrame and then repartition
    and write the results to disk."""
    path_dict = get_paths(process_source=process_source, out_type=out_type)

    df_dict = read_data(process_source=process_source, path_dict=path_dict, spark=spark)
    df, row_tracker = merge_tables(df_dict=df_dict, process_source=process_source, spark=spark)
    pd.DataFrame.from_dict(row_tracker, orient="index").to_csv(
        path_dict["icd_mart_row_tracker"]
    )
    validate_icd_mart(df)
    partition_and_write(df=df, write_path=path_dict["write_path"])


def get_file_source_lookup(cms_system: str) -> pd.DataFrame:
    """Pulls a file_source_id to is_otp lookup table using the file source controller class."""
    file_sourcerer = manage_icd_mart_data.FsIDController(
        cms_system=cms_system,
        processing_type=all_processing_types[cms_system],
        logger=None,
    )
    return file_sourcerer.fid_df[["file_source_id", "is_otp"]].drop_duplicates()


def partition_and_write(df: DataFrame, write_path: str) -> None:
    """Repartition the the ICD-mart data in memory to reduce the quantity of final files
    written due to Spark's auto-partitioning and write the ICD mart data to disk."""
    # Partitioning before writing drastically reduced the number of files on disk.
    df.repartition("year_id", "is_otp", "icd_group_id").write.parquet(
        path=write_path,
        mode="overwrite",
        compression="snappy",
        partitionBy=["year_id", "is_otp", "icd_group_id"],
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ICD-Mart creator via PySpark")
    parser.add_argument(
        "--memory",
        help=(
            "Memory allocated to the Spark driver in stand-alone mode. Should be at least a "
            "few Gb per core"
        ),
        type=int,
        required=True,
    )
    parser.add_argument(
        "--source",
        help=("Identifies the CMS source to use when creating an ICD-mart."),
        choices=["max", "mdcr"],
        type=str,
        required=True,
    )
    parser.add_argument(
        "--out_type",
        help="'prod' or 'test' run. Modifies IO paths.",
        choices=["prod", "test"],
        type=str,
        required=True,
    )
    args = parser.parse_args()

    spark = ci_spark(memory_gb=args.memory, tmp_dirkey="cms", name="cms_icd_mart")
    try:
        process_source = process_sources.get_source_processing(args.source)
        read_merge_write(process_source=process_source, spark=spark, out_type=args.out_type)
    finally:
        spark.stop()
