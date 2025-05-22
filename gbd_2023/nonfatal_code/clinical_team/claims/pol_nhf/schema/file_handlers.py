"""
Module to handle writing and reading from disk
"""

import os
from functools import lru_cache
from pathlib import Path
from typing import Dict, List, Union
from uuid import UUID

import pandas as pd
from crosscutting_functions.pipeline_constants import poland as constants
from crosscutting_functions.clinical_metadata_utils.values import COMP
from crosscutting_functions.clinical_metadata_utils import pipeline_wrappers
from crosscutting_functions.clinical_metadata_utils.parser import filepath_parser, section_parser

from pol_nhf.utils.settings import SchemaRunSettings


def read_partitioned_data(settings: SchemaRunSettings) -> List[pd.DataFrame]:
    """Reader for Poland raw data intake. Reads in data filtered by facility type and year.

    Args:
        settings (SchemaRunSettings): Instance of SchemaRunSettings

    Returns:
        List[pd.DataFrame]: Collection of dataframes filtered by year and is_otp
    """
    df_list: List[pd.DataFrame] = []

    sf = SourceFiles()

    # Get file path from cached settings.
    base_path = "FILEPATH"
    facility_partitions = "PARTITIONS"

    # Read the manually- and auto-partitioned raw data file from disk.
    for facility in facility_partitions:
        data_path = "FILEPATH"
        for partition_path in data_path.glob("*.parquet"):
            df = pd.read_parquet(partition_path, columns=constants.RAW_READ_COLS)
            df["visit_year"] = settings.year_id
            df["place_of_visit"] = facility

            df_list.append(df)

    return df_list


def write_icd_mart(df: pd.DataFrame) -> None:
    """Wrapper for writing the ICD-Mart schema to partitioned parquet.

    Args:
        df (pd.DataFrame): Processed table fitting the ICD-Mart schema.
    """
    parquet_dir = (
        "FILEPATH"
    )
    df = df[constants.ORDERED_ICD_MART_COLS]
    icd_groups = df.icd_group_id.unique().tolist()
    for icd_group in icd_groups:
        write_filter = f"icd_group_id == {icd_group}"
        tmp = df.query(write_filter).copy()
        tmp = tmp.reset_index(drop=True)
        # pyarrow engine specifically, will automatically append to a partioned dataset.
        # https://arrow.apache.org/docs/python/parquet.html
        tmp.to_parquet(
            parquet_dir, engine="pyarrow", partition_cols=constants.PARTITION, compression=COMP
        )


def create_schema_log_file(settings: SchemaRunSettings) -> Path:
    """Creates the log file for the overall ICD-Mart processing run.

    Args:
        settings (SchemaRunSettings): Instance of SchemaRunSettings.

    Returns:
        Path: pathlib obj pointing to the new log file.
    """
    logs_path = filepath_parser(ini="pipeline.pol_nhf", section="logs", section_key="schema")
    logs_dir = "FILEPATH"
    if not Path(logs_dir):
        logs_dir.mkdir(parents=True)
    return logs_dir.joinpath(
        "FILEPATH"
    )


def create_jobmon_logs(uuid: Union[str, UUID]) -> Dict[str, str]:
    """Makes the logging directories to pass to Jobmon

    Args:
        uuid (Union[str, UUID]): Worklfow UUID for current run.

    Returns:
        Dict[str, str]: Mappable out and err directories.
    """
    jobmon_base = "FILEPATH"
    logs = {e: jobmon_base / e for e in ["errors", "output"]}
    if not jobmon_base.is_dir():
        for v in logs.values():
            v.mkdir(parents=True)

    return {"stderr": str(logs["errors"]), "stdout": str(logs["output"])}


def save_year_source_table(df: pd.DataFrame) -> None:
    """Saves any updates made to the Poland file tracker table.

    Args:
        df (pd.DataFrame): Updated tracker table.
    """
    path = filepath_parser(ini="pipeline.pol_nhf", section="logs", section_key="base")
    df["is_best"] = df.is_best.astype(int)
    df["event_id"] = df.event_id.astype(int)
    df = df.sort_values(by=["year_id", "event_id"])
    df.to_csv("FILEPATH")


def pull_year_source_best_table() -> pd.DataFrame:
    """Gets a DataFrame of the current BEST Poland file tracker table."""
    path = filepath_parser(ini="pipeline.pol_nhf", section="logs", section_key="base")
    return pd.read_csv("FILEPATH")


def save_year_source_best_table(df: pd.DataFrame) -> None:
    """Saves any updates made to the Poland bested file tracker table.

    Args:
        df (pd.DataFrame): Updated bested tracker table.
    """
    path = filepath_parser(ini="pipeline.pol_nhf", section="logs", section_key="base")
    df.to_csv("FILEPATH")


def validate_initialization(
    run_id: int, odbc_profile: str = "SERVER", clinical_data_type_id: int = 3
):
    """Validates that pre-run preparations have been done.
    - Run exists in run_metadata
    - Output directories have been created for the run_id.

    Args:
        run_id (int): CMS run ID. Must align with Clinical run ID
        odbc_profile (str, optional): Indicates if prod or dev Clinial DB.
            Defaults to "SERVER".
        clinical_data_type_id (int, optional): ID to indcate claims, inpatient, etc.
            Defaults to 3.

    Raises:
        RuntimeError: The run_id provided does not exist in run_metadata.
        RuntimeError: A run path for the provided run_id do not exist.
    """

    cw = pipeline_wrappers.ClaimsWrappers(
        run_id=run_id, odbc_profile=odbc_profile, clinical_data_type_id=clinical_data_type_id
    )
    metadata = cw.pull_run_metadata()
    if metadata.shape[0] == 0:
        raise RuntimeError(f"run_id '{run_id}' does not exist in run.")

    path = "FILEPATH"
    if not os.path.exists(path=path):
        raise RuntimeError(f"Path '{path}' does not exist. Please create it.")


@lru_cache()
def SourceFiles() -> Dict[int, str]:
    """
    Creates dictionary paths to the converted source files keyed by file_source_id.
    """
    input_paths = section_parser(ini="pipeline.pol_nhf", section="input")
    file_source_dict: Dict[int, str] = {}
    for path in input_paths:
        file_source_id = int(path.split("_")[-1])

        if (
            input_paths[path] in file_source_dict.values()
            or file_source_id in file_source_dict.keys()
        ):
            raise ValueError("Non unique file source id or file path")

        file_source_dict.update({file_source_id: input_paths[path]})

    return file_source_dict
