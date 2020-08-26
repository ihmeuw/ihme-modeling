from sqlalchemy import orm
import pandas as pd

from orm_stgpr.db import models
from orm_stgpr.lib.constants import columns
from orm_stgpr.lib.util import helpers


def get_custom_covariates(
        session: orm.Session,
        stgpr_version_id: int
) -> pd.DataFrame:
    """
    Pulls custom covariate data associated with an ST-GPR version ID.

    Args:
        stgpr_version_id: the ID with which to pull custom covariate data

    Returns:
        Dataframe of custom covariates for an ST-GPR run or None if there are
        no custom covariates for the given ST-GPR version ID
    """
    covariate_df = _get_covariate_data(session, stgpr_version_id)
    if covariate_df.empty:
        return None

    covariate_df = covariate_df.pipe(
        lambda df: _map_covariate_lookup(covariate_df, session)
    )
    return pd\
        .pivot_table(
            covariate_df,
            values=columns.VAL,
            index=columns.DEMOGRAPHICS,
            columns=columns.COVARIATE_LOOKUP_ID)\
        .reset_index()\
        .pipe(_reset_column_names)\
        .pipe(helpers.sort_columns)


def _get_covariate_data(
        session: orm.Session,
        stgpr_version_id: int
) -> pd.DataFrame:
    covariate_query = session\
        .query(models.T3CovariateSTGPR)\
        .filter_by(stgpr_version_id=stgpr_version_id)
    return pd\
        .read_sql(covariate_query.statement, session.bind)\
        .drop(columns=columns.STGPR_VERSION_ID)


def _map_covariate_lookup(
        df: pd.DataFrame,
        session: orm.Session
) -> pd.DataFrame:
    lookup_ids = df[columns.COVARIATE_LOOKUP_ID].unique().tolist()
    covariate_lookups = session\
        .query(models.CovariateLookup)\
        .filter(models.CovariateLookup.covariate_lookup_id.in_(lookup_ids))\
        .all()
    lookup_mapper = {
        lookup.covariate_lookup_id:
            f'{columns.CV_PREFIX}{lookup.covariate_lookup}'
        for lookup in covariate_lookups
    }
    return df.assign(**{
        columns.COVARIATE_LOOKUP_ID:
        df[columns.COVARIATE_LOOKUP_ID].map(lookup_mapper)
    })


def _reset_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """Resets dataframe column names after a pivot operation"""
    df.columns.rename(None, inplace=True)
    return df
