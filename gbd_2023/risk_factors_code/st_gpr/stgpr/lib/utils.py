"""Utility functions for ST-GPR.

This module is expected to be a transitional step towards moving legacy code into the
new tooling system as well as adding new, utility functions. It's name, function, etc.
should be re-visited in the future.
"""

import math
from typing import Any, Dict, List

import numpy as np
import pandas as pd

import stgpr_helpers
from stgpr_helpers import columns, parameters
from stgpr_schema import ModelType

from stgpr.lib import constants


def get_best_parameter_set(stgpr_version_id: int) -> int:
    """Gets the best parameter set from the ST-GPR version."""
    file_utility = stgpr_helpers.StgprFileUtility(stgpr_version_id)
    model_type = file_utility.read_parameters()[parameters.MODEL_TYPE]

    if model_type not in ModelType.__members__:
        raise ValueError(f"ST-GPR model type '{model_type}' does not exist.")

    model_type_enum = ModelType[model_type]

    # Retrieve best parameter set for in sample and out of sample models.
    # Otherwise, there's only one
    if model_type_enum in [ModelType.in_sample_selection, ModelType.oos_selection]:
        fit_stats = file_utility.read_all_fit_statistics()
        return fit_stats.loc[fit_stats["best"] == 1, "parameter_set"].iat[0]
    else:
        return 0


def get_value_cols(data: pd.DataFrame, params: Dict[str, Any]) -> List[str]:
    """Get value cols for a step of STGPR.

    Arguments:
        data: Dataframe for a specific step of an STGPR versions workflow
        params: Parameters for an STGPR version

    Stage 1 and 2 use 'val' while GPR and raking steps use 'gpr_mean',
    ['gpr_mean', 'gpr_lower', 'gpr_upper'] if quantiles have been calculated, or draws.
    """
    return (
        [f"draw_{i}" for i in range(params[parameters.GPR_DRAWS])]
        if "draw_0" in data.columns
        # return lower, upper if lower is found within columns
        else (
            ["gpr_mean", "gpr_lower", "gpr_upper"]
            if "gpr_lower" in data.columns
            # if lower is missing just return mean
            else ["gpr_mean"] if "gpr_mean" in data.columns else [columns.VAL]
        )
    )


def get_parameter_groups(
    run_type: str, n_parameter_sets: int, holdouts: int
) -> List[np.typing.NDArray]:
    """Sets up parameter groups based on model_type.

    Arguments:
        run_type: type of model run determining logic for building parameter groups
        n_parameter_sets: number of parameter sets being used in model
        holdouts: number of holdouts being used in model

    We group parameter sets into groups to self-throttle jobs. Each group could have
    1 - n parameter sets within it which run in a loop in the appropriate steps,
    ex: spacetime. No parameter group can ever be empty.
    """
    max_submissions = constants.workflow.MAX_SUBMISSIONS

    # for in-sample selection, there are no holdouts, so we can run each parameter set
    # separately until we hit the MAX_SUBMISSIONS limit when we then start to
    # double/triple/etc up
    if run_type == "in_sample_selection":
        num_param_groups = min(max_submissions, n_parameter_sets)

    # for out-of-sample selection, each holdout needs to run each parameter group, but
    # we can't exceed the MAX_SUBMISSIONS limit. Their relationship is multiplicative:
    # holdouts * # param groups = MAX_SUBMISSIONS
    # => # param groups = MAX_SUBMISSIONS / # holdouts, rounding down
    elif run_type == "oos_selection":
        num_param_groups = min(math.floor(max_submissions / holdouts), n_parameter_sets)

    # for models with a single set of parameters
    else:
        num_param_groups = 1
    return np.array_split(range(n_parameter_sets), num_param_groups)
