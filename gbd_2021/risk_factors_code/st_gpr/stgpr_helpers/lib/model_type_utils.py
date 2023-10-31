"""Model type helper functions."""
from typing import List, Optional

from sqlalchemy import orm

import db_stgpr
from db_stgpr.api.enums import ModelType

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
) -> ModelType:
    """Determines which type of ST-GPR model is being registered."""
    max_num_params = max(len(st_lambda), len(st_omega), len(st_zeta), len(gpr_scale))
    has_cutoffs = density_cutoffs and density_cutoffs != [0]
    if not has_cutoffs and not holdouts and max_num_params == 1:
        return ModelType.base
    elif not has_cutoffs and not holdouts and max_num_params > 1:
        return ModelType.in_sample_selection
    elif not has_cutoffs and holdouts and max_num_params == 1:
        return ModelType.oos_evaluation
    elif not has_cutoffs and holdouts and max_num_params > 1:
        return ModelType.oos_selection
    elif has_cutoffs and not holdouts and max_num_params > 1:
        return ModelType.dd
    raise exc.CantDetermineModelType("Could not determine model type")


def is_selection_model(
    stgpr_version_id: int,
    session: Optional[orm.Session] = None,
    output_path: Optional[str] = None,
) -> bool:
    """Checks whether a model is an in-sample- or out-of-sample selection model.

    Args:
        stgpr_version_id: ID of the ST-GPR version to check.
        session: Session with the epi database.
        output_path: If passed, used to look up model type from cached parameters rather
            than from the database. This only works while a model is running.

    Returns:
        Whether a model is a selection model.
    """
    if not session and not output_path:
        raise ValueError("Must pass either session or output_path when looking up model type")

    selection_types = {
        ModelType.in_sample_selection.name,
        ModelType.oos_selection.name,
    }
    if output_path:
        file_utility = file_utils.StgprFileUtility(output_path)
        params = file_utility.read_parameters()
        return params[parameters.MODEL_TYPE] in selection_types

    return db_stgpr.is_selection_model(stgpr_version_id, session)
