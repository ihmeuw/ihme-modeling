"""This module contains utilities related smoothing in the latent trend model."""

from typing import List, Union

import pandas as pd
from fhs_lib_database_interface.lib.constants import CauseConstants, DimensionConstants
from fhs_lib_database_interface.lib.strategy_set import query
from sqlalchemy.orm import Session
from tiny_structured_logger.lib.fhs_logging import get_logger

logger = get_logger()


def get_smoothing_dims(acause: str, gbd_round_id: int, session: Session) -> List[str]:
    """Get the dims to smooth over during the ARIMA or Random Walk modeling.

    Args:
        acause: The cause for which to get smoothing dimensions.
        gbd_round_id (int): The numeric ID for the GBD round.
        session: session with the database, used to get the cause hierarchy.

    Returns:
        List[str]: The dimensions to smooth over.
    """
    cause_hierarchy_version_id = query.get_hierarchy_version_id(
        session=session,
        entity_type=DimensionConstants.CAUSE,
        entity_set_id=CauseConstants.FHS_CAUSE_SET_ID,
        gbd_round_id=gbd_round_id,
    )

    cause_hierarchy = query.get_hierarchy(
        session=session,
        entity_type=DimensionConstants.CAUSE,
        hierarchy_version_id=cause_hierarchy_version_id,
    )
    return _smoothing_dims_from_hierarchy(acause, cause_hierarchy)


def _smoothing_dims_from_hierarchy(acause: str, cause_hierarchy: pd.DataFrame) -> List[str]:
    """The dimensions to smooth over during ARIMA modeling, based on the level of acause.

    Args:
        acause (str): The cause for which to get smoothing dimensions.
        cause_hierarchy (pd.Dataframe): The cause hierarchy used to determine the level of the
            given cause. Must have ``acause`` and ``level`` as columns.

    Returns:
        List[str]: The dimensions to smooth over.
    """
    LOCATION_SEX_AGE_DIMS = [
        DimensionConstants.LOCATION_ID,
        DimensionConstants.SEX_ID,
        DimensionConstants.AGE_GROUP_ID,
    ]
    REGION_SEX_AGE_DIMS = [
        DimensionConstants.REGION_ID,
        DimensionConstants.SEX_ID,
        DimensionConstants.AGE_GROUP_ID,
    ]
    SUPERREGION_SEX_AGE_DIMS = [
        DimensionConstants.SUPER_REGION_ID,
        DimensionConstants.SEX_ID,
        DimensionConstants.AGE_GROUP_ID,
    ]
    SMOOTHING_LOOKUP = {
        0: LOCATION_SEX_AGE_DIMS,
        1: LOCATION_SEX_AGE_DIMS,
        2: REGION_SEX_AGE_DIMS,
        3: SUPERREGION_SEX_AGE_DIMS,
        "modeled": SUPERREGION_SEX_AGE_DIMS,
    }

    cause_level = cause_hierarchy.query(f"{DimensionConstants.ACAUSE} == @acause")[
        DimensionConstants.LEVEL_COL
    ].unique()[0]

    logger.debug(
        "Pulling smoothing dims",
        bindings=dict(acause=acause, cause_level=cause_level),
    )

    level_option: Union[int, str]
    if cause_level > 2:
        level_option = "modeled"
    else:
        level_option = cause_level

    smoothing_dims = SMOOTHING_LOOKUP[level_option]

    return smoothing_dims
