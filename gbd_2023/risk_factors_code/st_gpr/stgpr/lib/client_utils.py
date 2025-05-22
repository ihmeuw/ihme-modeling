"""Utility functions exposing access to the ST-GPR microservice."""

import logging
from typing import Any, Dict, List, Optional, Union

import pandas as pd

import stgpr_client
import stgpr_schema
from db_stgpr import columns as db_columns
from stgpr_helpers import columns, parameters

_RETURN_COLUMNS: List[str] = [
    columns.STGPR_VERSION_ID,
    columns.CROSSWALK_VERSION_ID,
    columns.MODELABLE_ENTITY_ID,
    "modelable_entity_name",
    columns.DESCRIPTION,
    db_columns.RELEASE_ID,
    "release_name",
    "date_inserted_as_created",
    db_columns.USER,
    columns.MODEL_STATUS_ID,
    columns.MODEL_STATUS_NAME,
    "publication_id",
    db_columns.BEST_START,
    db_columns.BEST_END,
    db_columns.GPR_DRAWS,
]


def _package_demographic_filters(
    year_start: Optional[int],
    year_end: Optional[int],
    location_id: Optional[Union[List[int], int]],
    sex_id: Optional[Union[List[int], int]],
    age_group_id: Optional[Union[List[int], int]],
) -> Dict[str, Optional[Union[List[int], int]]]:
    """Validate and package demographic filters into a dictionary to feed to ST-GPR client."""
    return {
        "year_start": _convert_to_int(year_start, "year_start", allow_none=True),
        "year_end": _convert_to_int(year_end, "year_end", allow_none=True),
        "location_ids": _convert_to_int(
            location_id, "location_id", allow_list=True, allow_none=True
        ),
        "sex_ids": _convert_to_int(sex_id, "sex_id", allow_list=True, allow_none=True),
        "age_group_ids": _convert_to_int(
            age_group_id, "age_group_id", allow_list=True, allow_none=True
        ),
    }


def _convert_to_int(
    arg: Optional[Union[List[float], float]],
    arg_name: str,
    allow_list: bool = False,
    allow_none: bool = False,
) -> Optional[Union[List[int], int]]:
    """Convert float or list of floats to int.

    Values given via reticulated R functions will be float. This function quietly converts.
    Safe for int -> int. None values are returned straight away without conversion if allowed.

    Raises:
        ValueError if arg is/has any values that are not convertable to ints.
    """
    if allow_none and arg is None:
        return arg

    if allow_list and isinstance(arg, list):
        return [_convert_to_int_help(a, arg_name) for a in arg]

    return _convert_to_int_help(arg, arg_name)


def _convert_to_int_help(arg: float, arg_name: str) -> int:
    """Helper function for _convert_to_int."""
    try:
        arg = int(arg)
    except (ValueError, TypeError):
        raise ValueError(
            f"Argument '{arg_name}' must be convertable to int, got {arg} of type {type(arg)}"
        ) from None

    return arg


def get_model_status(version_id: float, verbose: bool = True) -> int:
    """Gets ST-GPR model status.

    Simplifies model statuses to three categories:
        * 0 - model failed
        * 1 - model suceeded (including best)
        * 2 - model is running

    Deleted models are filtered out and will cause the function to return a ValueError.
    """
    version_id = _convert_to_int(version_id, "version_id")

    with stgpr_client.client() as client:
        stgpr_versions = client.get_stgpr_versions(stgpr_version_ids=version_id)

    if not stgpr_versions:
        raise ValueError(
            f"No data found for ST-GPR version {version_id}. It may not exist or "
            "it may have been deleted."
        )

    stgpr_versions = stgpr_versions[0]
    model_status_id = stgpr_versions[columns.MODEL_STATUS_ID]
    model_status_name = stgpr_versions[columns.MODEL_STATUS_NAME]

    if verbose:
        logging.info(f"ST-GPR version {version_id}'s status is '{model_status_name}'.\n")

    success_statuses = [stgpr_schema.ModelStatus.success, stgpr_schema.ModelStatus.best]
    if model_status_id in success_statuses:
        return 1
    elif model_status_id == stgpr_schema.ModelStatus.failure:
        return 0
    else:
        return 2


def get_parameters(
    version_id: Union[List[int], int], unprocess: bool
) -> Union[pd.DataFrame, List[Dict[str, Any]]]:
    """Gets parameters for given ST-GPR version(s)."""
    # Convert potential floats to ints and then convert to list if not already list
    version_id = _convert_to_int(version_id, "version_id", allow_list=True)
    version_id = version_id if isinstance(version_id, list) else [version_id]

    with stgpr_client.client() as client:
        params = [
            dict(params) for params in client.get_parameters(stgpr_version_ids=version_id)
        ]

    # Add ST-GPR version ID and unpack GbdCovariate objects into three parameters:
    # gbd_covariates, gbd_covariate_model_version_ids, and custom_mvid_specified
    for i in range(len(params)):
        param = params[i]
        param[columns.STGPR_VERSION_ID] = version_id[i]

        # Collect all GBD covariate data into separate lists
        gbd_covariates = []
        gbd_covariate_model_version_ids = []
        custom_mvid_specified = []

        for gbd_covariate in param[parameters.GBD_COVARIATES]:
            gbd_covariates.append(gbd_covariate.covariate_name_short)
            gbd_covariate_model_version_ids.append(gbd_covariate.covariate_model_version_id)
            custom_mvid_specified.append(gbd_covariate.custom_mvid_specified)

        # Add back to parameters and re-sort
        param[parameters.GBD_COVARIATES] = gbd_covariates
        param[parameters.GBD_COVARIATE_MODEL_VERSION_IDS] = gbd_covariate_model_version_ids
        param[columns.CUSTOM_MVID_SPECIFIED] = custom_mvid_specified

        params[i] = {param: val for param, val in sorted(param.items())}

    # If requested, unprocess parameters by converting all lists to comma-separated strings
    # and return a dataframe
    if unprocess:
        params_df = []
        for parameter_set in params:
            for param, value in parameter_set.items():
                if isinstance(value, list):
                    parameter_set[param] = ", ".join([str(item) for item in value])

            params_df.append(pd.DataFrame(parameter_set, index=[0]))

        return pd.concat(params_df, ignore_index=True)
    else:
        return params


def get_input_data(
    version_id: int,
    data_stage_name: str,
    year_start: Optional[int],
    year_end: Optional[int],
    location_id: Optional[Union[List[int], int]],
    sex_id: Optional[Union[List[int], int]],
    age_group_id: Optional[Union[List[int], int]],
) -> pd.DataFrame:
    """Gets input data for an ST-GPR version."""
    version_id = _convert_to_int(version_id, "version_id")
    demographic_filters = _package_demographic_filters(
        year_start, year_end, location_id, sex_id, age_group_id
    )

    # Confirm given data stage is valid and convert to ID
    data_stage_names = [stage.name for stage in stgpr_schema.DataStage]
    if data_stage_name not in data_stage_names:
        all_data_stage_names = ", ".join([f"'{stage}'" for stage in data_stage_names])
        raise ValueError(
            f"Valid data stage names are {all_data_stage_names}. Got '{data_stage_name}'."
        )

    data_stage_id = stgpr_schema.DataStage[data_stage_name].value

    with stgpr_client.client() as client:
        input_data = client.get_input_data(
            stgpr_version_id=version_id, data_stage_id=data_stage_id, **demographic_filters
        )

    return pd.DataFrame(input_data)


def get_estimates(
    version_id: int,
    entity: str,
    year_start: Optional[int],
    year_end: Optional[int],
    location_id: Optional[Union[List[int], int]],
    sex_id: Optional[Union[List[int], int]],
    age_group_id: Optional[Union[List[int], int]],
) -> pd.DataFrame:
    """Gets estimates of a particular stage for an ST-GPR version."""
    version_id = _convert_to_int(version_id, "version_id")
    demographic_filters = _package_demographic_filters(
        year_start, year_end, location_id, sex_id, age_group_id
    )

    with stgpr_client.client() as client:
        # Confirm given entity is valid and convert to ID
        entity_to_function = {
            "stage1": client.get_stage1_estimates,
            "spacetime": client.get_spacetime_estimates,
            "gpr": client.get_gpr_estimates,
            "final": client.get_final_estimates,
        }
        if entity not in entity_to_function:
            all_entities = ", ".join([f"'{i}'" for i in entity_to_function])
            raise ValueError(
                f"'{entity}' is not a valid entity. Allowed values: {all_entities}."
            )

        estimates = entity_to_function[entity](
            stgpr_version_id=version_id, **demographic_filters
        )

    return pd.DataFrame(estimates)


def get_custom_covariates(
    version_id: int,
    year_start: Optional[int],
    year_end: Optional[int],
    location_id: Optional[Union[List[int], int]],
    sex_id: Optional[Union[List[int], int]],
    age_group_id: Optional[Union[List[int], int]],
) -> Optional[pd.DataFrame]:
    """Gets custom covariates associated with a model."""
    version_id = _convert_to_int(version_id, "version_id")
    demographic_filters = _package_demographic_filters(
        year_start, year_end, location_id, sex_id, age_group_id
    )

    with stgpr_client.client() as client:
        covariates = client.get_custom_covariates(
            stgpr_version_id=version_id, **demographic_filters
        )

    if covariates is None:
        return None

    return pd.DataFrame(covariates)


def get_stgpr_versions(
    version_id: Optional[List[int]],
    modelable_entity_id: Optional[int],
    release_id: Optional[int],
    model_status_id: Optional[int],
    bundle_id: Optional[List[int]],
    crosswalk_version_id: Optional[List[int]],
) -> pd.DataFrame:
    """Gets ST-GPR versions associated with the given parameters."""
    version_id = _convert_to_int(version_id, "version_id", allow_list=True, allow_none=True)
    modelable_entity_id = _convert_to_int(
        modelable_entity_id, "modelable_entity_id", allow_none=True
    )
    release_id = _convert_to_int(release_id, "release_id", allow_none=True)
    model_status_id = _convert_to_int(model_status_id, "model_status_id", allow_none=True)
    bundle_id = _convert_to_int(bundle_id, "bundle_id", allow_list=True, allow_none=True)
    crosswalk_version_id = _convert_to_int(
        crosswalk_version_id, "crosswalk_version_id", allow_list=True, allow_none=True
    )

    with stgpr_client.client() as client:
        stgpr_versions = client.get_stgpr_versions(
            stgpr_version_ids=version_id,
            modelable_entity_id=modelable_entity_id,
            release_id=release_id,
            model_status_id=model_status_id,
            bundle_ids=bundle_id,
            crosswalk_version_ids=crosswalk_version_id,
        )

    stgpr_versions = pd.DataFrame(stgpr_versions)

    # If there's results, subset to columns relevant to modeler. If no results,
    # there's no columns so just return as is
    if stgpr_versions.empty:
        return stgpr_versions
    else:
        return stgpr_versions[_RETURN_COLUMNS]
