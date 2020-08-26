import logging
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

from orm_stgpr.lib.util import helpers
from orm_stgpr.lib.constants import dtype, parameters


def get_parameters_from_config(
        path_to_config: str,
        model_index_id: Optional[int]
) -> Dict[str, Any]:
    """
    Extracts ST-GPR model parameters from a config CSV into a dictionary.
    Selects single row of config, ensures required parameters are present,
    drops extra columns, casts parameter types, converts certain strings
    into lists, assigns default parameter values, converts NaN to None, and
    removes leading 0 from density cutoffs if present.
    Uses Python engine rather than C engine in order to handle bad characters
    out of the box. The performance hit from using the Python engine is
    negligible since config files are tiny.
    """
    logging.info('Reading config')
    params = pd\
        .read_csv(path_to_config, engine='python')\
        .pipe(lambda df: _get_parameter_set_at_index(df, model_index_id))\
        .pipe(_validate_required_parameters_exist)\
        .pipe(_drop_extra_parameter_columns)\
        .pipe(_cast_parameters)\
        .pipe(_assign_default_parameter_values)\
        .pipe(helpers.nan_to_none)\
        .to_dict('records')[0]
    params[parameters.DENSITY_CUTOFFS] = _update_density_cutoffs(
        params[parameters.DENSITY_CUTOFFS]
    )
    return params


def _drop_extra_parameter_columns(config_df: pd.DataFrame) -> pd.DataFrame:
    return config_df.drop(columns=[
        col for col in config_df.columns
        if col not in parameters.REQUIRED_PARAMETERS
        and col not in parameters.DEFAULT_PARAMETERS
    ])


def _get_parameter_set_at_index(
        config: pd.DataFrame,
        model_index_id: Optional[int]
) -> pd.DataFrame:
    """Returns the parameter set at a given row of the config"""
    if len(config) == 1:
        return config
    elif config.empty:
        raise ValueError('Config is empty')
    elif not model_index_id:
        raise ValueError(
            f'Config has {len(config)} rows, but argument model_index_id '
            'was not specified'
        )
    elif parameters.MODEL_INDEX_ID not in config:
        raise ValueError(
            f'Config has {len(config)} rows, but config does not include '
            f'a {parameters.MODEL_INDEX_ID} parameter'
        )

    param_df = config[config[parameters.MODEL_INDEX_ID] == model_index_id]
    if param_df.empty:
        raise ValueError(
            f'model_index_id {model_index_id} does not match any config '
            'entries'
        )
    elif len(param_df) > 1:
        raise ValueError(
            f'Found {len(param_df)} config entries with model_index_id '
            f'{model_index_id}'
        )

    return param_df.reset_index()


def _cast_parameters(config_df: pd.DataFrame) -> pd.DataFrame:
    """
    Casts parameters to their required types.

    This is done after reading in the CSV in order to provide better error
    messages for failed casts.
    This function also splits string parameters into lists, casting each item
    in the list to the appropriate type.
    """
    def _cast_parameters_helper(config_df, dtypes, cast_lists):
        for param, cast_type in dtypes.items():
            # Column must exist and value must not be null.
            if param in config_df and not config_df[param].isna().any():
                try:
                    if not cast_lists:
                        config_df[param] = config_df[param].astype(cast_type)
                    else:
                        vals = []
                        for val in str(config_df[param].iat[0]).split(','):
                            vals.append(cast_type(val.strip()))
                        config_df[param] = [vals]
                except (ValueError, TypeError):
                    raise TypeError(
                        f'Invalid value for {param}: {config_df[param].iat[0]}'
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
                f'{param_name} not found in config. Using default value '
                f'{flattened_value}'
            )
            config[param_name] = default_value
    return config


def _update_density_cutoffs(density_cutoffs: List[int]) -> List[int]:
    """Sorts density cutoffs and ensures they don't start with 0"""
    density_cutoffs = sorted(density_cutoffs)
    if density_cutoffs and density_cutoffs[0] == 0:
        density_cutoffs.pop(0)
    return density_cutoffs


def _validate_required_parameters_exist(config: pd.DataFrame) -> pd.DataFrame:
    """Validates that config contains required parameters"""
    non_null_params = set(
        col for col in config.columns if not config[col].isna().any()
    )
    if not parameters.REQUIRED_PARAMETERS.issubset(non_null_params):
        missing_params = parameters.REQUIRED_PARAMETERS - non_null_params
        raise ValueError(
            f'Config is missing required parameters: '
            f'{sorted(missing_params)}'
        )
    return config
