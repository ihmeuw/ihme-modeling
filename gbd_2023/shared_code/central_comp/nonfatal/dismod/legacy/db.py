from sqlalchemy.orm import sessionmaker

import db_tools_core
from db_tools import config

from cascade_ode.legacy.constants import ConnectionDefinitions
from cascade_ode.legacy.settings import load as load_settings

settings = load_settings()


def get_engine(conn_def, env):
    """Retrieve a SQLAlchemy engine.

    Args:
        conn_def (str): One of 'epi', 'cod', 'gbd'. If epi, will return an
        engine with elevated permissions for inserting/infiling/updating. Cod
        and gbd are read-only
        env (str): One of 'prod' or 'dev'. Determines if test epi database is
            used. Cod and gbd db is always production since it's read-only
    """
    _remove_unexpected_conn_defs()

    conn_def = conn_def.lower().strip()
    if conn_def not in ConnectionDefinitions.VALID_CONN_DEFS:
        raise ValueError(
            f"Expected one of {ConnectionDefinitions.VALID_CONN_DEFS}, got " f"{conn_def}"
        )

    env = env.lower().strip()
    if env not in ["prod", "dev"]:
        raise ValueError("Expected prod or dev, got {}".format(env))

    # always read prod data from read-only databases
    if conn_def in ConnectionDefinitions.READ_ONLY:
        true_conn_def = conn_def
    else:
        true_conn_def = "cascade-{}".format(env)

    eng = db_tools_core.get_engine(true_conn_def)
    return eng.engine


def execute_select(query, conn_def="epi", params=None):
    """Run a sql query, return a dataframe. Uses ENVIRONMENT_NAME environment
    variable to determine prod/dev database server.

    Args:
        query(str): sql statement to execute
        conn_def(str, 'epi'): one of 'epi' or 'cod'
        params: Dictionary for parametrized queries.

    Returns:
        Pandas Dataframe
    """
    engine = get_engine(conn_def=conn_def, env=settings["env_variables"]["ENVIRONMENT_NAME"])
    engine.dispose()
    Session = sessionmaker(bind=engine)
    sesh = Session()
    try:
        df = db_tools_core.query_2_df(query, session=sesh, parameters=params)
    finally:
        sesh.close()
    return df


def _remove_unexpected_conn_defs() -> None:
    """We want all database queries to be called via service account user. To that end,
    we want to raise an error if we every try to access a conn def not in 
    (since it would default to a different user defined in central odbc).

    So before we create an engine, we want to ensure a runtime failure if we try
    to use a conn_def we don't know about.
    """
    unknown_conn_defs = [
        k for k in config.DBConfig.conn_defs if k not in ConnectionDefinitions.LOCAL_CONN_DEFS
    ]
    for key in unknown_conn_defs:
        config.DBConfig.conn_defs.pop(key)


def enable_cleartext_plugin() -> None:
    """This is necessary for LDAP. If we import certain db_tools modules
    prior to creating a session with db_tools_core, we hit a cleartext
    plugin error."""
    with db_tools_core.session_scope("epi"):
        pass


enable_cleartext_plugin()
