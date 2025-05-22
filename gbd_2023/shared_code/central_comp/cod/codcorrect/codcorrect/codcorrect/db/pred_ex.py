from typing import List, Optional

import pandas as pd
from sqlalchemy import orm

import db_tools_core

from codcorrect.legacy.utils.constants import Columns, ConnectionDefinitions
from codcorrect.lib.db.queries import PredEx


def get_best_mortality_process_run_id(
    process_id: int, release_id: int, session: Optional[orm.Session] = None
) -> int:
    """Gets the best run id for the mortality process id passed
    for the given release.

    Args:
        process_id: the mortality process id to get the best run id of
        release_id: the release id to get the best run id of
        session: session with the mortality database.

    Raises:
        RuntimeError if there is 0 or more than 1 best run ids for the given
            process id
    """
    with db_tools_core.session_scope(
        ConnectionDefinitions.MORTALITY, session=session
    ) as scoped_session:
        run_id = db_tools_core.query_2_df(
            PredEx.GET_BEST_MORTALITY_PROCESS_ID,
            session=scoped_session,
            parameters={"process_id": process_id, "release_id": release_id},
        ).run_id

    if len(run_id) != 1:
        raise RuntimeError(
            f"Expected exactly 1 best run id for process id {process_id}, "
            f"and release id {release_id}. Received {len(run_id)}:"
            f" {run_id.tolist()}"
        )

    return run_id.iat[0]


def get_pred_ex(
    life_table_run_id: int,
    tmrlt_run_id: int,
    expected_age_group_ids: Optional[List[int]] = None,
    session: Optional[orm.Session] = None,
) -> pd.DataFrame:
    """Fetches predicted life expectancy.

    Arguments:
        life_table_run_id: run id for life tables with shock,
            mortality process id 29
        tmrlt_run_id: run id for TMRLT, mortality process id 30
        expected_age_group_ids: all the age group ids that we are
            expecting to have pred ex values for.
        session: session with the mortality database.

    Returns:
        DataFrame containing age_group_id (int), location_id (int),
        pred_ex (float), year_id (int), sex_id (int)

    Raises:
        RuntimeError: when any rows of predicted life expectancy are missing;
            or any expected age groups are missing
    """
    with db_tools_core.session_scope(
        ConnectionDefinitions.MORTALITY, session=session
    ) as scoped_session:
        tmrlt = db_tools_core.query_2_df(
            PredEx.GET_TMRLT, session=scoped_session, parameters={"run_id": tmrlt_run_id}
        )

        lt_u90 = db_tools_core.query_2_df(
            PredEx.GET_LIFE_TABLE_UNDER_90,
            session=scoped_session,
            parameters={"run_id": life_table_run_id},
        )

        lt_95plus = db_tools_core.query_2_df(
            PredEx.GET_LIFE_TABLE_OVER_95,
            session=scoped_session,
            parameters={"run_id": life_table_run_id},
        )

    # Append together lt_u90 and lt_95plus. ax=ex for terminal age group
    life_tables = pd.concat([lt_u90, lt_95plus], sort=True)

    # Merge on tmrlt
    pred_ex = pd.merge(
        life_tables, tmrlt, how="left", left_on="age_at_death", right_on="precise_age"
    )

    # Check that we don't have any NULL (None) values.
    missing_values = pred_ex[pred_ex.pred_ex.isna()]
    if not missing_values.empty:
        raise RuntimeError(
            f"There are {len(missing_values)} rows where predicted life "
            f"expectancy is None for with-shock life tables "
            f"v{life_table_run_id} and TMRLT v{tmrlt_run_id}.\n"
            f"Sample row: {missing_values.iloc[0].to_dict()}"
        )

    # Check that there aren't missing age groups
    if expected_age_group_ids is not None:
        missing_age_groups = set(expected_age_group_ids) - set(pred_ex.age_group_id)
        if missing_age_groups:
            raise RuntimeError(
                "The following age group ids are missing predicted life "
                f"expectancy for with-shock life tables v{life_table_run_id} "
                f"and TMRLT v{tmrlt_run_id}: {missing_age_groups}."
            )

    return pred_ex[Columns.PRED_EX_COLS]
