from sqlalchemy import orm
import pandas as pd

from orm_stgpr.db import models
from orm_stgpr.lib.constants import columns
from orm_stgpr.lib.util import helpers


def get_custom_stage_1_estimates(
        session: orm.Session,
        stgpr_version_id: int
) -> pd.DataFrame:
    """
    Pulls custom stage 1 data associated with an ST-GPR version ID.

    Args:
        stgpr_version_id: the ID with which to pull custom stage 1 estimates

    Returns:
        Dataframe of custom stage 1 estimates for an ST-GPR run or None if
        there are no custom stage 1 estimates for the given ST-GPR version ID
    """
    if not _has_custom_stage_1(stgpr_version_id, session):
        return None

    # Custom stage 1 runs with holdouts have duplicate estimate sets for
    # each holdout, so only use the estimate set associated with the first
    # model iteration.
    model_iteration = _get_model_iteration(stgpr_version_id, session)
    stage_1_estimates = session\
        .query(models.Stage1Estimate)\
        .filter_by(model_iteration_id=model_iteration.model_iteration_id)
    return pd\
        .read_sql(stage_1_estimates.statement, session.bind)\
        .drop(columns=columns.MODEL_ITERATION_ID)\
        .pipe(helpers.sort_columns)


def _has_custom_stage_1(stgpr_version_id: int, session: orm.Session) -> bool:
    stgpr_version = session\
        .query(models.StgprVersion)\
        .filter_by(stgpr_version_id=stgpr_version_id)\
        .one()
    return stgpr_version.custom_stage_1 == 1


def _get_model_iteration(
        stgpr_version_id: int,
        session: orm.Session
) -> models.ModelIteration:
    model_iteration = session\
        .query(models.ModelIteration)\
        .filter_by(stgpr_version_id=stgpr_version_id)\
        .first()
    if not model_iteration:
        raise RuntimeError(
            'No model iterations found for ST-GPR version ID '
            f'{stgpr_version_id}. This is only possible if there was an '
            'error during registration, so try re-registering this model, '
            'or file a ticket'
        )
    return model_iteration
