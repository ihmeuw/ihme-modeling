"""Model type helper functions."""

from typing import List, Optional

from sqlalchemy import orm

import db_stgpr
import stgpr_schema

from stgpr_helpers.lib import file_utils
from stgpr_helpers.lib.constants import exceptions as exc
from stgpr_helpers.lib.constants import parameters


def determine_model_type(
    density_cutoffs: List[int],
    holdouts: int,
    st_lambda: List[float],
    st_omega: List[float],
    st_zeta: List[float],
    gpr_scale: List[float],
) -> stgpr_schema.ModelType:
    """Determines which type of ST-GPR model is being registered."""
    max_num_params = max(len(st_lambda), len(st_omega), len(st_zeta), len(gpr_scale))
    has_cutoffs = density_cutoffs and density_cutoffs != [0]
    if not has_cutoffs and not holdouts and max_num_params == 1:
        return stgpr_schema.ModelType.base
    elif not has_cutoffs and not holdouts and max_num_params > 1:
        return stgpr_schema.ModelType.in_sample_selection
    elif not has_cutoffs and holdouts and max_num_params == 1:
        return stgpr_schema.ModelType.oos_evaluation
    elif not has_cutoffs and holdouts and max_num_params > 1:
        return stgpr_schema.ModelType.oos_selection
    elif has_cutoffs and not holdouts and max_num_params > 1:
        return stgpr_schema.ModelType.dd
    raise exc.CantDetermineModelType("Could not determine model type")


def is_selection_model(
    stgpr_version_id: int,
    model_is_running: bool = False,
    session: Optional[orm.Session] = None,
) -> bool:
    """Checks whether a model is an in-sample- or out-of-sample selection model.

    Args:
        stgpr_version_id: ID of the ST-GPR version to check.
        model_is_running: Whether the model is currently running. If true, then look up model
            type from cached parameters rather than from the database.
        session: Session with the epi database.

    Returns:
        Whether a model is a selection model.
    """
    if not model_is_running and not session:
        raise ValueError(
            "Must pass either model_is_running or session when looking up model type"
        )

    selection_types = {
        stgpr_schema.ModelType.in_sample_selection.name,
        stgpr_schema.ModelType.oos_selection.name,
    }
    if model_is_running:
        file_utility = file_utils.StgprFileUtility(stgpr_version_id)
        params = file_utility.read_parameters()
        return params[parameters.MODEL_TYPE] in selection_types

    return db_stgpr.is_selection_model(stgpr_version_id, session)
