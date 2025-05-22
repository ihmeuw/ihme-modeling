"""
After a run has completed move any appropriate GBD data from CMS to
our standard FILEPATH folder for upload
"""

import os
from pathlib import Path, PosixPath
from typing import List

import pandas as pd
from crosscutting_functions.clinical_metadata_utils.parser import filepath_parser
from clinical_info.Functions.clinical_functions.validations.validation_functions import (
    validate_schema,
)

from cms.utils import readers


def get_dir(run_id: int) -> PosixPath:
    """Get the CMS run directory for data/final outputs.

    Args:
        run_id (int): Clinical run_id found in DATABASE

    Returns:
        PosixPath: Path the run_id 'FILEPATH'.
    """
    # parser to get dir
    run_path = (
        "FILEPATH"
    )

    return run_path


def get_final_run_files(run_path: PosixPath) -> List[Path]:
    """Path rglob to get a list of parquet file(s).

    Args:
        run_path (PosixPath): Path to glob over.

    Returns:
        List[Path]: All parquet files in a directory.
    """
    path = Path(run_path)
    file_paths = list(path.rglob("*parquet"))

    return file_paths


def read_final_files(paths: List[Path]) -> pd.DataFrame:
    """Read all files and filter DataFrame to only non-ident data.

    Args:
        paths (List[Path]): Paths to processed parquet files.

    Returns:
        pd.DataFrame: Processed data limited to only non-ident rows.
    """

    filters = [("is_identifiable", "==", 0)]
    df_list = [readers.read_from_parquet(path, filters=filters) for path in paths]

    return pd.concat(df_list, sort=False, ignore_index=True)


def sort_df(df: pd.DataFrame) -> pd.DataFrame:
    """Use sort_cols list to sort the row order of compiled df.

    Args:
        df (pd.DataFrame): Must have columns found in 'sort_cols' list.

    Returns:
        pd.DataFrame: Table sorted by values in order of the columns
        in 'sort_cols' list.
    """

    sort_cols = [
        "bundle_id",
        "estimate_id",
        "location_id",
        "year_start",
        "sex_id",
        "age_group_id",
    ]
    df = df.sort_values(by=sort_cols).reset_index(drop=True)

    return df


def add_db_cols(df: pd.DataFrame, run_id: int) -> pd.DataFrame:
    """Code static columns to the DataFrame per DB upload requirement.

    Args:
        df (pd.DataFrame): Table of processed estimates.
        run_id (int): Clinical run_id found in DATABASE.

    Returns:
        pd.DataFrame: Table with static DB columns added.
    """

    df["source_type_id"] = 17
    df["diagnosis_id"] = 3
    df["representative_id"] = 4
    df["run_id"] = run_id

    return df


def write_for_upload(df: pd.DataFrame, run_id: int) -> None:
    """Write df as .csv output to hospital clinical run dir.

    Args:
        df (pd.DataFrame): Table of processed estimates.
            MUST be all non-ident.
        run_id (int): Clinical run_id found in DATABASE.

    Raises:
        RuntimeError: If ident data is present in 'df'
        ValueError: Output run directory doesen't exist.
    """

    if not (df["is_identifiable"] == 0).all():
        raise RuntimeError("Cannot write outside LU, there is identifiable data present")

    df = validate_schema(df=df, schema_name="cms_pipeline", coerce=True)

    out_dir = "FILEPATH"
    if not os.path.exists(out_dir):
        msg = f"run_id '{run_id}' doesn't exist in out_dir"
        sol = "Run 'make_dir()' to create clinical run directories."
        raise ValueError(f"{msg}. {sol}.")

    df.to_csv("FILEPATH", index=False)


def main(run_id: int) -> None:
    """Read in all the final CMS data after a swarm, compile, remove identifiable rows,
    sort and then write a csv for upload matching our other pipelines.

    Args:
        run_id (int): Clinical run_id found in DATABASE.
    """

    run_path = get_dir(run_id=run_id)
    file_paths = get_final_run_files(run_path=run_path)
    df = read_final_files(paths=file_paths)
    df = add_db_cols(df=df, run_id=run_id)
    df = sort_df(df=df)

    # all done, now write file for upload
    write_for_upload(df=df, run_id=run_id)
