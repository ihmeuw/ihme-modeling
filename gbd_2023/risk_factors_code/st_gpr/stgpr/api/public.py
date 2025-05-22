"""Top-level public stgpr functions."""

from typing import Any, Dict, List, Optional, Union

import pandas as pd

import stgpr_helpers
import stgpr_schema

from stgpr.lib import client_utils


def register_stgpr_model(path_to_config: str, model_index_id: Optional[int] = None) -> int:
    """Creates an ST-GPR version associated with parameters specified in a config.

    Args:
        path_to_config: Path to config file containing input parameters.
        model_index_id: Optional ID used to select a parameter set if the config file contains
            multiple sets of parameters.

    Returns:
        ID of the created ST-GPR version.
    """
    stgpr_helpers.configure_logging()
    return stgpr_helpers.create_stgpr_version(path_to_config, model_index_id)


def stgpr_sendoff(
    run_id: int, project: str, log_path: Optional[str] = None, nparallel: int = 50
) -> None:
    """Submits an ST-GPR model to the cluster.

    Args:
        run_id: ID of the ST-GPR version to run.
        project: Cluster project/account to which jobs will be submitted.
        log_path: Path to a directory for saving run logs. Default to model output directory
            if not specified. Storing logs on J is not allowed.
        nparallel: Number of parallelizations to split your data over (by location_id).
    """
    stgpr_helpers.configure_logging()
    settings = stgpr_schema.get_settings()
    path_to_python_shell = settings.path_to_model_code / "src/stgpr/lib/run_script_in_env.sh"
    path_to_main_script = settings.path_to_model_code / "src/stgpr/legacy/model/run_main.py"
    stgpr_helpers.launch_model(
        run_id,
        project,
        str(path_to_python_shell),
        str(path_to_main_script),
        log_path,
        nparallel,
    )


def get_model_status(version_id: float, verbose: bool = True) -> int:
    """Gets ST-GPR model status.

    Args:
        version_id: ID of the ST-GPR version to run.
        verbose: if True, additionally logs the name of the model status

    Raises:
        ValueError: if version_id is not a single int; if no record of the ST-GPR version
            is found, possibly due to the ID not existing or the version having been deleted.

    Returns:
        int representing the status of the run. 0 - failure, 1 - success, 2 - running
    """
    stgpr_helpers.configure_logging()
    return client_utils.get_model_status(version_id, verbose)


def get_parameters(
    version_id: Union[List[int], int], unprocess: bool = True
) -> Union[pd.DataFrame, List[Dict[str, Any]]]:
    """Gets parameters for given ST-GPR version(s).

    Unprocessed parameters will resemble the config file modelers provide when registering
    an ST-GPR version. Processed parameters will vary in format compared to how they're
    provided by the modeler.

    There will be additional parameters modelers do not provide as described in the docs.

    Args:
        version_id: ID or IDs of the ST-GPR version(s) to return parameters for.
        unprocess: Whether to unprocess the parameters or not. True will return a dataframe
            similar to the config file provided by modelers when registering a model. False
            will return a list of parameter dictionaries. Defaults to True.

    Raises:
        ValueError: if version_id is not a single int or list of ints.

    Returns:
        a dataframe or a list of parameter dictionaries
    """
    stgpr_helpers.configure_logging()
    return client_utils.get_parameters(version_id, unprocess)


def get_input_data(
    version_id: int,
    data_stage_name: str,
    year_start: Optional[int] = None,
    year_end: Optional[int] = None,
    location_id: Optional[Union[List[int], int]] = None,
    sex_id: Optional[Union[List[int], int]] = None,
    age_group_id: Optional[Union[List[int], int]] = None,
) -> pd.DataFrame:
    """Gets input data for an ST-GPR version.

    Args:
        version_id: ID of the ST-GPR version to run.
        data_stage_name: name of the data stage to retrieve data from, i.e. "original".
        year_start: Start year to pull data for. Optional, if omitted, returns data
            for all years.
        year_end: End year to pull data for. Optional, if omitted, returns data for all years.
        location_id: ID or IDs of the locations to pull data for. Optional, if omitted,
            returns data for all locations.
        sex_id: ID or IDs of the sexes to pull data for. Optional, if omitted,
            returns data for all sexes.
        age_group_id: ID or IDs of the age groups to pull data for. Optional, if omitted,
            returns data for all age groups.

    Raises:
        ValueError: if data_stage_name is not valid.
    """
    stgpr_helpers.configure_logging()
    return client_utils.get_input_data(
        version_id, data_stage_name, year_start, year_end, location_id, sex_id, age_group_id
    )


def get_estimates(
    version_id: int,
    entity: str,
    year_start: Optional[int] = None,
    year_end: Optional[int] = None,
    location_id: Optional[Union[List[int], int]] = None,
    sex_id: Optional[Union[List[int], int]] = None,
    age_group_id: Optional[Union[List[int], int]] = None,
) -> pd.DataFrame:
    """Gets estimates of a particular stage for an ST-GPR version.

    Args:
        version_id: ID of the ST-GPR version to run.
        entity: name of the model stage to pull results from. Options are 'stage1',
            'spacetime', 'gpr', and 'final'. The latter two will return upper/lower columns.
        year_start: Start year to pull data for. Optional, if omitted, returns data
            for all years.
        year_end: End year to pull data for. Optional, if omitted, returns data for all years.
        location_id: ID or IDs of the locations to pull data for. Optional, if omitted,
            returns data for all locations.
        sex_id: ID or IDs of the sexes to pull data for. Optional, if omitted,
            returns data for all sexes.
        age_group_id: ID or IDs of the age groups to pull data for. Optional, if omitted,
            returns data for all age groups.

    Raises:
        ValueError: if entity is not valid.
    """
    stgpr_helpers.configure_logging()
    return client_utils.get_estimates(
        version_id, entity, year_start, year_end, location_id, sex_id, age_group_id
    )


def get_custom_covariates(
    version_id: int,
    year_start: Optional[int] = None,
    year_end: Optional[int] = None,
    location_id: Optional[Union[List[int], int]] = None,
    sex_id: Optional[Union[List[int], int]] = None,
    age_group_id: Optional[Union[List[int], int]] = None,
) -> Optional[pd.DataFrame]:
    """Gets custom covariates associated with a model.

    If the given model has no associated custom covariates, None will be returned.

    Args:
        version_id: ID of the ST-GPR version to run.
        year_start: Start year to pull data for. Optional, if omitted, returns data
            for all years.
        year_end: End year to pull data for. Optional, if omitted, returns data for all years.
        location_id: ID or IDs of the locations to pull data for. Optional, if omitted,
            returns data for all locations.
        sex_id: ID or IDs of the sexes to pull data for. Optional, if omitted,
            returns data for all sexes.
        age_group_id: ID or IDs of the age groups to pull data for. Optional, if omitted,
            returns data for all age groups.
    """
    stgpr_helpers.configure_logging()
    return client_utils.get_custom_covariates(
        version_id, year_start, year_end, location_id, sex_id, age_group_id
    )


def get_stgpr_versions(
    version_id: Optional[List[int]] = None,
    modelable_entity_id: Optional[int] = None,
    release_id: Optional[int] = None,
    model_status_id: Optional[int] = None,
    bundle_id: Optional[List[int]] = None,
    crosswalk_version_id: Optional[List[int]] = None,
) -> pd.DataFrame:
    """Gets ST-GPR versions associated with the given arguments.

    Automatically filters out deleted models. This can be overwritten by supplying
    model_status_id, which will instead filter to the given model status.

    Every argument is optional. If no arguments are given, the function will return all
    ST-GPR models in the database, which will be slow and database-intensive. Because of this,
    it is always recommended to specify at least one argument, in particular
    modelable_entity_id and release_id.

    If no ST-GPR versions are found that match the given criteria, an empty column-less
    dataframe will be returned.

    Args:
        version_id: ID(s) of the ST-GPR versions to filter on.
        modelable_entity_id: ID of the modelable entity to filter on.
        release_id: ID of the release to filter on.
        model_status_id: ID of the model status to filter on.
        bundle_id: ID(s) of the bundle(s) to filter on.
        crosswalk_version_id: ID(s) of the crosswalk version(s) to filter on.
    """
    stgpr_helpers.configure_logging()
    return client_utils.get_stgpr_versions(
        version_id,
        modelable_entity_id,
        release_id,
        model_status_id,
        bundle_id,
        crosswalk_version_id,
    )
