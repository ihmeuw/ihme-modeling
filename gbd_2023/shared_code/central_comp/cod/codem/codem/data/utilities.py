import logging
from typing import Optional

import pandas as pd
from sqlalchemy import orm

import db_tools_core
from gbd import conn_defs

logger = logging.getLogger(__name__)


def list_check(f):
    return f if type(f) is list else [f]


def age_id_to_name(
    age_id: int, age_df: pd.DataFrame = None, session: Optional[orm.Session] = None
) -> str:
    """
    returns the age_group_name_short corresponding to the input age_id

    :param age_id: int
        the age_group_id to convert
    :param age_df: pandas dataframe
        a dataframe specifying the relationship between age_group_id and
        age_group_name_short; if 'None', it will be queried from
        sql
    :param session: orm.Session
        database session with shared clone

    :return: string
        the age_group_name_short corresponding to the input age_id
    """
    # get age_df from shared.cause if it wasn't passed in as an input
    if age_df is None:
        with db_tools_core.session_scope(
            conn_defs.SHARED_VIP, session=session
        ) as scoped_session:
            age_df = db_tools_core.query_2_df(
                """
                SELECT
                    age_group_id, age_group_name_short
                FROM
                    shared.age_group
                """,
                session=scoped_session,
            )
    else:
        pass

    return age_df[age_df.age_group_id == age_id].age_group_name_short.iloc[0]
