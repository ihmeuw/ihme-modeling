import os
from pathlib import Path, PosixPath
from typing import Any, Union

import pandas as pd
from crosscutting_functions.mapping import clinical_mapping_db

from crosscutting_functions import pipeline


def get_active_bundle_metadata(run_id: int) -> pd.DataFrame:
    """Using a Clinical run_id gets the DATABASE
    table for the map version associated with the run.

    Args:
        run_id (int): Run ID found in DATABASE.

    Returns:
        pd.DataFrame: DATABASE table
    """
    map_version = pipeline.get_map_version(run_id=run_id)
    metadata = clinical_mapping_db.get_active_bundles(map_version=map_version)

    return metadata


def is_active_bundle_estimate(run_id: int, bundle_id: int, estimate_id: int) -> bool:
    """Indicates if a given bundle-estimate pair is active in a map version.

    Args:
        run_id (int): Run ID found in DATABASE.
        bundle_id (int): Bundle id present in DATABASE
        estimate_id (int): CI specific estimate id from DATABASE

    Returns:
        bool: True if the bundle-estimate pair is active.
    """
    metadata = get_active_bundle_metadata(run_id)

    bundle_metadata = metadata.loc[
        (metadata["bundle_id"] == bundle_id) & (metadata["estimate_id"] == estimate_id)
    ]

    if len(bundle_metadata) == 0:
        is_active = False
    else:
        is_active = True

    return is_active


def write_token(
    pipeline: str,
    tokendir: Union[str, PosixPath],
    run_id: int,
    bundle_id: int,
    estimate_id: int,
    err: Any,
) -> None:
    """Writes small csv for a single bundle-estimate pair that fails processing.
    Expected use is in workers processing a single bundle-estimate.

    Args:
        pipeline (str): Clinical pipeline being ran.
        tokendir (Union[str, PosixPath]): Directory to write token to.
            Expects FILEPATH as a subdirs.
        run_id (int): Run ID found in DATABASE.
        bundle_id (int): Bundle id present in DATABASE
        estimate_id (int): CI specific estimate id from DATABASE
        err (Any): The error or reason for writting the token.
            This value will be converted to a string representation.
    """
    path = Path(tokendir)

    if is_active_bundle_estimate(run_id=run_id, bundle_id=bundle_id, estimate_id=estimate_id):
        path = "FILEPATH"
    else:
        path = "FILEPATH"

    datum = {
        "pipeline": pipeline,
        "bundle_id": bundle_id,
        "estimate_id": estimate_id,
        "error": str(err),
    }
    record = pd.DataFrame.from_dict(datum, orient="index").T

    record.to_csv(f"{path}/{pipeline}_{bundle_id}_{estimate_id}.csv", index=False)


def compile_records(
    pipeline: str, tokendir: Union[str, PosixPath], delete_tokens: bool = True
) -> None:
    """Compiles tokens written by write_token() into a single table,
    and saved at 'data_path'.

    Args:
        pipeline (str): Clinical pipeline being ran.
        tokendir (Union[str, PosixPath]): Directory where tokens were written to.
            Expects FILEPATH as a subdirs.
    """
    records_path = Path(tokendir)

    active_paths = list("FILEPATH")
    inactive_paths = list(
        ("FILEPATH")
    )

    if len(active_paths) > 0:
        active_df = pd.concat([pd.read_csv(path) for path in active_paths], ignore_index=True)
        active_df["is_active"] = 1
    else:
        active_df = pd.DataFrame()

    if len(inactive_paths) > 0:
        inactive_df = pd.concat(
            [pd.read_csv(path) for path in inactive_paths], ignore_index=True
        )
        inactive_df["is_active"] = 0
    else:
        inactive_df = pd.DataFrame()

    records = pd.concat([active_df, inactive_df], ignore_index=True)
    records.to_csv("FILEPATH")

    token_paths = active_paths + inactive_paths

    if delete_tokens:
        for path in token_paths:
            os.remove(path)
