import argparse
from pathlib import Path
from typing import Tuple

import pyspark.pandas as ps
from crosscutting_functions.clinical_constants.pipeline import marketscan as constants
from crosscutting_functions.clinical_constants.pipeline.marketscan import YEAR_NAMING_SCHEME
from config_spark.initialize import ci_spark
from pyspark.sql import DataFrame


def convert_raw_ms_file(path: str, mode: str = "ignore") -> None:
    """Converts a raw Marketscan SAS file to parquet.

    Args:
        path: Path to raw data file.
        mode: Specifies the behavior of the save operation when data already exists.
            - 'append': Append contents of this DataFrame to existing data.
            - 'overwrite': Overwrite existing data.
            - 'ignore': Silently ignore this operation if data already exists.
            - 'error': Throw an exception if data already exists."
    """
    # memory_gb is based on ram in submit_batch with python buffer.
    spark = ci_spark(
        memory_gb=134,
        tmp_dirkey="marketscan",
        **{
            "spark.jars": constants.SAS_SPARK_JARS,
            "spark.jars.packages": constants.JAR_PARCKAGE,
        },
    )
    fn = align_file_naming(path=path)
    df = spark.read.format(constants.SAS_SPARK_FORMAT_TEMPLATE).load(str(path))
    write_path = "FILEPATH"
    df.write.parquet(write_path, mode=mode)
    validate_shape(df=df, path=write_path)


def align_file_naming(path: str) -> str:
    """Naming convention for files changed for extracted raw files.
    Align new naming convention with what is expected by the pipelines/tools
    for the converted parquet files.

    Args:
        path: Path to raw data file.

    Returns:
        File name aligned with Clinical convention.
    """
    year, pattern = find_year_suffix(path)
    # Post 2020
    if "FILEPATH" in path and Path(path).stem.isupper():
        parts = Path(path).stem.split(pattern)
        fn = "FILEPATH" 
    # Prior years
    else:
        fn = Path(path).stem

    return fn


def find_year_suffix(path: str) -> Tuple[int, str]:
    """Matches a year and suffix to a raw file path.

    Args:
        path: Path to raw data file.

    Raises:
        RuntimeError: If no match was found.

    Returns:
        (year, file-suffix)
    """
    for year, pattern in YEAR_NAMING_SCHEME.items():
        if pattern in Path(path).stem:
            return year, pattern
    raise RuntimeError(f"Could not find any suffix matches to {path}")


def validate_shape(df: DataFrame, path: str) -> None:
    """Compares a spark table and a read table from a path
    to make sure they are the same shape.

    Args:
        df: Spark DataFrame being processed.
        path: Path to written table to compare to 'df'.

    Raises:
        IndexError: If the tables have a different shape.
    """
    df_origin = df.pandas_api().shape
    df_read = ps.read_parquet(path).shape
    if df_origin != df_read:
        raise IndexError(f"Shape changed, \nprocess:{df_origin} \ndisk:{df_read}")


if __name__ == "__main__":
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument(
        "--file_path", type=str, required=True, help="Path to raw MS SAS file."
    )
    arg_parser.add_argument("--mode", type=str, required=True, help="Spark writer mode.")
    args = arg_parser.parse_args()

    convert_raw_ms_file(path=args.file_path, mode=args.mode)
