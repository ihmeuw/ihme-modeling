"""Helpers for working with parameters."""
import copy
from typing import Any, Dict

from sqlalchemy import orm

import db_stgpr
from db_stgpr import columns as db_columns
from db_stgpr.api.enums import AmplitudeMethod, ModelType, TransformType, Version
from gbd import decomp_step as gbd_decomp_step

from stgpr_helpers.lib.constants import dtype, parameters
from stgpr_helpers.lib.validation import stgpr_version as stgpr_version_validation


def get_parameters(stgpr_version_id: int, session: orm.Session) -> Dict[str, Any]:
    """Pulls parameters associated with an ST-GPR version ID."""
    stgpr_version_validation.validate_stgpr_version_exists(stgpr_version_id, session)

    # Use copy.deepcopy instead of pandas copy since the dataframe contains
    # lists which need to be copied recursively.
    params = copy.deepcopy(parameters.PARAMETER_SKELETON)
    stgpr_version = db_stgpr.get_stgpr_version(stgpr_version_id, session)
    _update_standard_stgpr_version_parameters(params, stgpr_version)
    _update_special_stgpr_version_parameters(params, stgpr_version, session)
    _update_gbd_covariate_parameters(params, stgpr_version_id, session)
    _update_model_iterations(params, stgpr_version_id, session)
    return {k: v for k, v in sorted(params.items()) if k in parameters.PARAMETER_SKELETON}


def _update_standard_stgpr_version_parameters(
    params: Dict[str, Any],
    stgpr_version: Dict[str, Any],
) -> None:
    """Adds standard (i.e. no special action needed) parameters to the dictionary.

    If the parameter is a "scalar" parameter (i.e. it only stores a single value), then the
    value in the database is used directly.

    If the parameter is a "list" parameter (i.e. it is stored in the database as a
    comma-separated list), then the value in the database is split and added as a list.
    """
    for scalar_column in db_columns.SCALAR_COLUMNS:
        param_value = stgpr_version[scalar_column]
        if param_value is not None:
            params[scalar_column] = param_value

    for list_column in db_columns.LIST_COLUMNS:
        param_value = stgpr_version[list_column]
        if param_value is not None:
            param_type = dtype.CONFIG_LIST[list_column]
            param_values = []
            for item in str(param_value).split(","):
                if item != "":
                    param_values.append(param_type(item))
            params[list_column] = param_values


def _update_special_stgpr_version_parameters(
    params: Dict[str, Any],
    stgpr_version: Dict[str, Any],
    session: orm.Session,
) -> None:
    """Adds special ST-GPR version parameters to the parameters dictionary.

    Some parameters are stored differently in the database than they are passed in by
    modelers, and we need to return them in the format modelers pass them in:
        - Location set version ID to location set ID.
        - Years to year start and year end.
        - Transform type ID to data transform name.
        - Model type ID to model type name.
        - Decomp step ID to decomp step name.
        - GPR amp method id to GPR amp method name.
        - ST version type id to ST version.
    """
    params[parameters.LOCATION_SET_ID] = db_stgpr.get_location_set_from_version(
        params[parameters.PREDICTION_LOCATION_SET_VERSION_ID], session
    )
    years = stgpr_version[db_columns.PREDICTION_YEAR_IDS].split(",")
    params[parameters.YEAR_START] = int(years[0])
    params[parameters.YEAR_END] = int(years[-1])
    params[parameters.DATA_TRANSFORM] = TransformType(
        stgpr_version[db_columns.TRANSFORM_TYPE_ID]
    ).name
    params[parameters.MODEL_TYPE] = ModelType(stgpr_version[db_columns.MODEL_TYPE_ID]).name
    params[parameters.DECOMP_STEP] = gbd_decomp_step.decomp_step_from_decomp_step_id(
        stgpr_version[db_columns.DECOMP_STEP_ID]
    )
    params[parameters.GPR_AMP_METHOD] = AmplitudeMethod(
        stgpr_version[db_columns.GPR_AMP_METHOD_ID]
    ).name
    params[parameters.ST_VERSION] = Version(stgpr_version[db_columns.ST_VERSION_TYPE_ID]).name


def _update_gbd_covariate_parameters(
    params: Dict[str, Any],
    stgpr_version_id: int,
    session: orm.Session,
) -> None:
    """Pulls GBD covariate parameters and adds them to the parameters dictionary."""
    gbd_covariates = db_stgpr.get_gbd_covariates(stgpr_version_id, session)
    covariate_name_shorts = (
        db_stgpr.get_covariate_names(
            [row[db_columns.COVARIATE_ID] for row in gbd_covariates], session
        )
        if gbd_covariates
        else []
    )
    params[parameters.GBD_COVARIATE_IDS] = gbd_covariates
    params[parameters.GBD_COVARIATES] = ",".join(covariate_name_shorts)


def _update_model_iterations(
    params: Dict[str, Any],
    stgpr_version_id: int,
    session: orm.Session,
) -> None:
    """Pulls hyperparameters/density cutoffs and adds them to the parameters dictionary."""
    model_iteration_ids = db_stgpr.get_model_iterations(stgpr_version_id, session)
    for model_iteration_id in model_iteration_ids:
        density_cutoffs = db_stgpr.get_density_cutoffs(model_iteration_id, session)
        for cutoff in density_cutoffs:
            # Hyperparameters.
            hyperparameter_set = db_stgpr.get_hyperparameter_set(
                cutoff[db_columns.HYPERPARAMETER_SET_ID], session
            )
            _update_hyperparameters(params, hyperparameter_set, params[parameters.MODEL_TYPE])

            # Density cutoffs.
            if cutoff[db_columns.DATA_DENSITY_END]:
                params[parameters.DENSITY_CUTOFFS].append(
                    int(cutoff[db_columns.DATA_DENSITY_END])
                )


def _update_hyperparameters(
    params: Dict[str, Any], hyperparameter_set: Dict[str, float], model_type: str
) -> None:
    """Adds a hyperparameter set to the parameters dictionary.

    If this is a density cutoff model, then allow duplicate hyperparameters.
    """
    allow_duplicates = model_type == ModelType.dd.name
    for hyperparameter_column in db_columns.HYPERPARAMETER_COLUMNS:
        param_value = hyperparameter_set[hyperparameter_column]
        if param_value is not None and (
            param_value not in params[hyperparameter_column] or allow_duplicates
        ):
            params[hyperparameter_column].append(param_value)
