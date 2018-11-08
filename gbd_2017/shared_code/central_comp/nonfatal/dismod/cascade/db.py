from db_tools import ezfuncs


def get_engine(conn_def, env):
    '''Retrieve a SQLAlchemy engine.
    '''
    conn_def = conn_def.lower().strip()
    if conn_def not in ['epi', 'cod']:
        raise ValueError("Expected epi or cod, got {}".format(conn_def))

    env = env.lower().strip()
    if env not in ['prod', 'dev']:
        raise ValueError("Expected prod or dev, got {}".format(env))

    # always read prod cod data
    if conn_def == 'cod':
        true_conn_def = 'cod'
    else:
        true_conn_def = "cascade-{}".format(env)
    eng = ezfuncs.get_engine(conn_def=true_conn_def)
    return eng
