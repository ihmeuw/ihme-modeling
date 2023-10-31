"""Helpers for reading ST-GPR configs."""
import logging
import os
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

from stgpr_helpers.lib import general_utils
from stgpr_helpers.lib.constants import dtype
from stgpr_helpers.lib.constants import exceptions as exc
from stgpr_helpers.lib.constants import parameters


def get_parameters_from_config(
    path_to_config: str,
    model_index_id: Optional[int],
) -> Dict[str, Any]:
    """Reads ST-GPR model parameters from a config CSV.

    Uses Python engine rather than C engine in order to handle bad characters out of the box.
    The performance hit from using the Python engine is negligible since config files are
    tiny.

    Reading a config file involves the following steps:
    1. Select single row of config
    2. Ensure required parameters are present
    3. Drop unneeded columns
    4. Cast parameters to appropriate types
    5. Convert certain string parameters into lists
    6. Assign default parameter values
    7. Convert NaNs to None
    8. Strip leading 0 from density cutoffs, if present

    Args:
        path_to_config: path to the config file containing model parameters.
        model_index_id: index (i.e. corrsponds to a `model_index_id` column in the config) of
            the config row to read.

    Returns:
        Model parameters, read into a dictionary.

    Raises:
        ValueError: if path to config is invalid
    """
    _check_config_path(path_to_config)

    params = (
        pd.read_csv(path_to_config, engine="python", encoding="utf-8-sig")
        .pipe(lambda df: _get_parameter_set_at_index(df, model_index_id))
        .pipe(_validate_required_parameters_exist)
        .pipe(_drop_extra_columns)
        .pipe(_cast_parameters)
        .pipe(_assign_default_parameter_values)
        .pipe(general_utils.nan_to_none)
        .to_dict("records")[0]
    )
    params[parameters.DENSITY_CUTOFFS] = _update_density_cutoffs(
        params[parameters.DENSITY_CUTOFFS]
    )
    return params


def _check_config_path(path_to_config: str) -> None:
    config_exists = os.path.isfile(path_to_config)
    config_is_csv = path_to_config.lower().endswith(".csv")
    if not config_exists or not config_is_csv:
        raise exc.InvalidPathToConfig(f"{path_to_config} is not a valid path to a config CSV")


def _drop_extra_columns(config_df: pd.DataFrame) -> pd.DataFrame:
    """Drops unneeded columns."""
    return config_df.drop(
        columns=[
            col
            for col in config_df.columns
            if col not in parameters.REQUIRED_PARAMETERS
            and col not in parameters.DEFAULT_PARAMETERS
        ]
    )


def _get_parameter_set_at_index(
    config: pd.DataFrame, model_index_id: Optional[int]
) -> pd.DataFrame:
    """Returns the parameter set at a given row of the config."""
    if len(config) == 1:
        return config
    elif config.empty:
        raise exc.EmptyConfig("Config is empty")
    elif not model_index_id:
        raise exc.InvalidModelIndex(
            f"Config has {len(config)} rows, but argument model_index_id " "was not specified"
        )
    elif parameters.MODEL_INDEX_ID not in config:
        raise exc.MissingModelIndexColumn(
            f"Config has {len(config)} rows, but config does not include "
            f"a {parameters.MODEL_INDEX_ID} parameter"
        )

    param_df = config[config[parameters.MODEL_INDEX_ID] == model_index_id]
    if param_df.empty:
        raise exc.InvalidModelIndex(
            f"model_index_id {model_index_id} does not match any config entries"
        )
    elif len(param_df) > 1:
        raise exc.InvalidModelIndex(
            f"Found {len(param_df)} config entries with model_index_id " f"{model_index_id}"
        )

    return param_df.reset_index()


def _cast_parameters(config_df: pd.DataFrame) -> pd.DataFrame:
    """Casts parameters to appropriate types.

    This is done after reading in the CSV in order to provide better error messages for
    failed casts.

    Also splits string parameters into lists, casting each item in the list to the appropriate
    type.
    """

    def _cast_parameters_helper(
        config_df: pd.DataFrame, dtypes: Dict[str, Any], cast_lists: bool
    ) -> None:
        """Helper function that casts parameters with or without splitting lists."""
        for param, cast_type in dtypes.items():
            if param not in config_df or config_df[param].isna().any():
                continue
            try:
                if not cast_lists:
                    config_df[param] = config_df[param].astype(cast_type)
                else:
                    vals = []
                    for val in str(config_df[param].iat[0]).split(","):
                        vals.append(cast_type(val.strip()))
                    config_df[param] = [vals]
            except (ValueError, TypeError):
                raise exc.InvalidParameterType(
                    f"Invalid value for {param}: {config_df[param].iat[0]}"
                )

    _cast_parameters_helper(config_df, dtype.CONFIG_SCALAR, False)
    _cast_parameters_helper(config_df, dtype.CONFIG_LIST, True)
    return config_df


def _assign_default_parameter_values(config: pd.DataFrame) -> pd.DataFrame:
    """Assigns default values to optional parameters."""
    for param_name, default_value in parameters.DEFAULT_PARAMETERS.items():
        if param_name not in config or config[param_name].isna().any():
            flattened_value = (
                np.ravel(default_value).tolist()
                if isinstance(default_value, list)
                else default_value
            )
            logging.info(
                f"{param_name} not found in config. Using default value " f"{flattened_value}"
            )
            config[param_name] = default_value
    return config


def _update_density_cutoffs(density_cutoffs: List[int]) -> List[int]:
    """Sorts density cutoffs and ensures they don't start with 0."""
    density_cutoffs = sorted(density_cutoffs)
    if density_cutoffs and density_cutoffs[0] == 0:
        density_cutoffs.pop(0)
    return density_cutoffs


def _validate_required_parameters_exist(config: pd.DataFrame) -> pd.DataFrame:
    """Validates that config contains required parameters."""
    non_null_params = set(col for col in config.columns if not config[col].isna().any())
    if not parameters.REQUIRED_PARAMETERS.issubset(non_null_params):
        missing_params = parameters.REQUIRED_PARAMETERS - non_null_params
        raise exc.ConfigMissingRequiredParameters(
            f"Config is missing required parameters: " f"{sorted(missing_params)}"
        )
    return config
