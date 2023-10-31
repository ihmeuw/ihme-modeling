"""
Random helper functions that are generally useful and don't fit into
any other utils files.
"""
from typing import List, Tuple, Union
from functools import lru_cache
import pandas as pd

from gbd import constants as gbd_constants, decomp_step
from db_tools.ezfuncs import query
import db_queries

from fauxcorrect.queries.queries import GbdDatabase, ModelVersion, Shared
from fauxcorrect.utils import step_control
from fauxcorrect.utils.constants import Columns, ConnectionDefinitions, GBD


def get_previous_modeling_round_and_step_id(
        decomp_step_id: int
) -> Tuple[int, int]:
    """
    Gets the GBD round ID and decomp step ID associated with the previous decomp step.
    If the previous decomp step is in the previous GBD round,
    the final modeling step id is returned (not the final step).

    Args:
        decomp_step_id: the decomp step ID for which to get the previous
            step's decomp step ID

    Returns:
        Tuple, GBD round id of previous step and
        decomp step ID of the previous decomp step
    Raises:
        KeyError if decomp_step_id cannot be mapped to a previous decomp step
    """
    if decomp_step_id not in gbd_constants.previous_decomp_step_id:
        raise KeyError(f'Decomp step id \'{decomp_step_id}\' does not exist '
                       f'in the previous decomp step map. Keys: '
                       f'{gbd_constants.previous_decomp_step_id.keys()}')

    previous_step_id = gbd_constants.previous_decomp_step_id[decomp_step_id]

    current_round_id = decomp_step.gbd_round_id_from_decomp_step_id(
        decomp_step_id)
    previous_round_id = decomp_step.gbd_round_id_from_decomp_step_id(
        previous_step_id)

    # If we haven't crossed a round boundary, the previous step is a simple
    # as the previous step
    if current_round_id == previous_round_id:
        round_id = current_round_id
        step_id = previous_step_id
    else:
        round_id = previous_round_id
        step_id = decomp_step.decomp_step_id_from_decomp_step(
            step_control.get_final_modeling_step(previous_round_id),
            previous_round_id
        )

    return round_id, step_id


def get_model_version_round_and_step(
        model_version_id: int
) -> Tuple[int, str]:
    """
    Returns a tuple of the gbd round id, decomp step
    for the given model version id.

    Raises:
        RuntimeError if the model version id does not exist
    """
    result = query(
        ModelVersion.GET_MVID_ROUND_AND_STEP_ID,
        conn_def=ConnectionDefinitions.COD,
        parameters={
            "model_version_id": model_version_id
        }
    )

    if result.empty:
        raise ValueError(
            f"No db entry found for model version id {model_version_id}")

    gbd_round_id = result.gbd_round_id.iat[0]
    decomp_step_id = result.decomp_step_id.iat[0]

    step = decomp_step.decomp_step_from_decomp_step_id(decomp_step_id)

    return gbd_round_id, step


def get_gbd_process_id_from_name(
        process_name: str,
        return_opposite: bool = False
) -> int:
    """
    Returns the GBD process id from the given process name,
    but only for Cod/FauxCorrect

    Args:
        machine_process: options: 'codcorrect', 'fauxcorrect'
        return_opposite: True iff you want the process id for the
            OPPOSITE process to be returned. Defaults to False

    Raises:
        ValueError: if machine_process is not one of the two options
    """
    if process_name == GBD.Process.Name.CODCORRECT:
        if return_opposite:
            return GBD.Process.Id.FAUXCORRECT
        else:
            return GBD.Process.Id.CODCORRECT
    elif process_name == GBD.Process.Name.FAUXCORRECT:
        if return_opposite:
            return GBD.Process.Id.CODCORRECT
        else:
            return GBD.Process.Id.FAUXCORRECT
    else:
        raise ValueError(
            f"Machine process name '{process_name}' is unrecognized.")


def get_metadata_type_id_from_name(process_name: str) -> int:
    """
    Returns the metadata_type_id for the given process's
    internal versioning in the GBD database.

    Args:
        machine_process: options: 'codcorrect', 'fauxcorrect'

    Raises:
        ValueError: if machine_process is not one of the two options
    """
    if process_name == GBD.Process.Name.CODCORRECT:
        return GBD.MetadataTypeIds.CODCORRECT
    elif process_name == GBD.Process.Name.FAUXCORRECT:
        return GBD.MetadataTypeIds.FAUXCORRECT
    else:
        raise ValueError(
            f"Machine process name '{process_name}' is unrecognized.")


def get_gbd_process_version_id(
        process: str,
        version_id: int,
        raise_if_multiple: bool = False,
        return_all: bool = False
) -> Union[List[int], int]:
    """
    Returns the (non-deleted) GBD process version id associated with the
    Cod/FauxCorrect version. Defaults to the most recent GBD process
    version if there is more than one.

    Args:
        process: the name of the process, "codcorrect" or "fauxcorrect"
        version_id: the internal version of the process. Ie. 135
            for CoDCorrect v135
        raise_if_multiple: whether to throw an error if there are more than
            one associated GBD process versions. Defaults to False
        return_all: whether to return a list of all associated GBD process
            version ids if multiple or just the most recent.

    Returns:
        The most recent, non-deleted GBD process version id associated with
        the Cod/FauxCorrect run. Returns all as a list if return-all=True

    Raises:
        ValueError if there are no non-deleted GBD process version ids
            associated;
        RuntimeError if there are multiple associated process version ids
            and raise_if_multiple=True
    """
    process_id = get_gbd_process_id_from_name(process)
    metadata_type_id = get_metadata_type_id_from_name(process)

    undeleted_version_ids = query(
        GbdDatabase.GET_GBD_PROCESS_VERSION_ID,
        parameters={
            "gbd_process_id": process_id,
            "metadata_type_id": metadata_type_id,
            "internal_version_id": version_id
        },
        conn_def=ConnectionDefinitions.GBD
    ).gbd_process_version_id.tolist()

    if not undeleted_version_ids:
        raise ValueError(
            f"There are no GBD process version ids for {process} v"
            f"{version_id} that have not been deleted.")

    if raise_if_multiple and len(undeleted_version_ids) > 1:
        raise RuntimeError(
            "There is more than one GBD process version id associated with "
            f"{process} v{version_id} and raise_if_multiple=True. GBD process "
            f" versions: {undeleted_version_ids}.")

    if return_all:
        return undeleted_version_ids
    else:
        return undeleted_version_ids[0]


def age_group_id_to_age_start(age_group_id: int) -> float:
    """
    Converts an age group id to age group years start to allow comparison
    of age groups.

    This function anticipates being called in rapid succession, so it
    relies on age_group database table that it queries being cached.

    Raises:
        ValueError: if the age_group_id does not exist in shared

    Returns:
        age group year start of the age group id
    """
    age_metadata = db_queries.get_age_spans()
    age_start = age_metadata.loc[
        age_metadata[Columns.AGE_GROUP_ID] == age_group_id,
        Columns.AGE_GROUP_YEARS_START
    ]

    if age_start.empty:
        raise ValueError(
            f"Age group {age_group_id} is not in the age metadata.")

    return age_start.iat[0]
