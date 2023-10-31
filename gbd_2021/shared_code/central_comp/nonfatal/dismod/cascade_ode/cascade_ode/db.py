from sqlalchemy.orm import sessionmaker
from db_tools import mysqlapis, config, query_tools as qt

from cascade_ode.constants import ConnectionDefinitions
from cascade_ode.settings import load as load_settings

settings = load_settings()


def get_engine(conn_def, env, **kwargs):
    '''Retrieve a SQLAlchemy engine.

    Args:
        conn_def (str): One of 'epi', 'cod', 'gbd'. If epi, will return an
        engine with elevated permissions for inserting/infiling/updating. Cod
        and gbd are read-only
        env (str): One of 'prod' or 'dev'. Determines if test epi database is
            used. Cod and gbd db is always production since it's read-only
    '''
    conn_def = conn_def.lower().strip()
    if conn_def not in ConnectionDefinitions.VALID_CONN_DEFS:
        raise ValueError(
            f"Expected one of {ConnectionDefinitions.VALID_CONN_DEFS}, got "
            f"{conn_def}"
        )

    env = env.lower().strip()
    if env not in ['prod', 'dev']:
        raise ValueError("Expected prod or dev, got {}".format(env))

    # always read prod data from read-only databases
    if conn_def in ConnectionDefinitions.READ_ONLY:
        true_conn_def = conn_def
    else:
        true_conn_def = "cascade-{}".format(env)

    conn_def_kwargs = config.DBConfig.conn_defs[true_conn_def]
    kwargs.update(conn_def_kwargs)
    eng = mysqlapis.MySQLEngine(**kwargs)
    return eng.engine


def execute_select(query, conn_def='epi', params=None):
    '''Run a sql query, return a dataframe. Uses ENVIRONMENT_NAME environment
    variable to determine prod/dev database server.

    Args:
        query(str): sql statement to execute
        conn_def(str, 'epi'): one of 'epi' or 'cod'

    Returns:
        Pandas Dataframe
    '''
    engine = get_engine(conn_def=conn_def,
                        env=settings['env_variables']['ENVIRONMENT_NAME'])
    engine.dispose()
    Session = sessionmaker(bind=engine)
    sesh = Session()
    try:
        df = qt.query_2_df(query, session=sesh, parameters=params)
    finally:
        sesh.close()
    return df
