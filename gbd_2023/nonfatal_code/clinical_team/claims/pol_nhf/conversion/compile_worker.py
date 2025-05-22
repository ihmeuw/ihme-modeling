import os
import shutil
import sys
import warnings
from typing import Dict

import pyspark.pandas as ps
from crosscutting_functions.pipeline_constants import poland as constants
from crosscutting_functions.clinical_metadata_utils.parser import filepath_parser
from crosscutting_functions.clinical_metadata_utils.initialize import ci_spark


def compile_years(year_dict: Dict[str, str], file_source_id: int) -> None:
    """Compiles years together from the single year workflow into
    a single partitioned table. Uses PySpark to write a table with partitions
    defined in clinical_constants and auto-partitions.
    NOTE: If any year is already present in the table, it will be removed
    prior to appended the rerun year.

    Args:
        year_dict (Dict[str, str]): Years to be compiled.
        file_source_id (int): Data version to process.
    """
    years = list(year_dict.values())
    base = filepath_parser(
        ini="pipeline.pol_nhf", section="conversion", section_key="parquet_dir"
    )
    paths = [
        "FILEPATH"
        for year_id in years
    ]

    df = ps.concat([ps.read_parquet(path) for path in paths])

    base = filepath_parser(
        ini="pipeline.pol_nhf", section="conversion", section_key="parquet_dir"
    )
    outpath = "FILEPATH"

    if os.path.exists(outpath):
        og_years = (
            ps.read_parquet(outpath, columns=["visit_year"])["visit_year"].unique().to_numpy()
        )
        og_years = set([int(year) for year in og_years])
        mode = "append"
    else:
        og_years = set()
        mode = "overwrite"

    years_ints = [int(year) for year in years]

    if len(set(years) - og_years) != len(set(years_ints)):
        overlap = og_years.intersection(set(years_ints))
        warnings.warn(f"The following years will be replaced {overlap}")

        for year in overlap:
            shutil.rmtree("FILEPATH")

    df.to_parquet(outpath, mode=mode, partition_cols=constants.PARTITION_COLS)


if __name__ == "__main__":
    clinical_env_path = "FILEPATH"
    os.environ["PATH"] = f"{clinical_env_path}:{os.environ['PATH']}"

    kwargs = {}
    for i, arg in enumerate(sys.argv[1:]):
        if arg.startswith("--"):
            name = arg.lstrip("--")
            kwargs[name] = sys.argv[i + 2]

    spark = ci_spark(
        memory_gb=int(kwargs["worker_mem"]),
        tmp_dirkey="spark_default",
        name="pol_nhf_conversion_compile",
    )
    file_source_id = int(kwargs["file_source_id"])

    # remove non-year_id args from kwargs.
    kwargs.pop("worker_mem")
    kwargs.pop("file_source_id")

    # Only years to run remain in kwargs.
    compile_years(year_dict=kwargs, file_source_id=file_source_id)
