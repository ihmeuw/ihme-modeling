"""
Update the versioning tables in the clinical db for each run
"""
import pandas as pd
import sqlalchemy
import getpass
import sys

user = getpass.getuser()
prep_path = "FILEPATH".format(user)
sys.path.append(prep_path)
import clinical_funcs



def cf_version(run_id, cf_version_id, description, engine):
    """
    Insert the cf version into the cf version table and then update the clinical run table
    Note: the order is important here. Must cf v table must have cf v id before it can be
    inserted into the run table, and then run_id must already exist in the run table
    """
    if len(description) > 10:
        print("This is going to break because the description is too long")
    assert isinstance(run_id, int) & isinstance(cf_version_id, int) &\
           isinstance(description, str), "Data types are wrong"
    insert_cfv = """SQL""".format(cf=cf_version_id, d=description)

    update_cfv = """SQL""".format(cf=cf_version_id, r=run_id)

    q = ""DB QUERY"
    vt = pd.read_sql("DB QUERY")
    if vt.shape[0] == 0:
        engine.execute(insert_cfv)
    else:
        print("It looks like this cf version id is already present in the cf version table")

    q = ""DB QUERY"
    rt = pd.read_sql("DB QUERY")
    if rt.shape[0] == 0:
        engine.execute(update_cfv)
    else:
        print("It looks like this cf version id is already present in the run table")

    return


def env_version(run_id, env_stgpr_id, env_me_id, description, engine):
    """
    Note, a run can only use 1 envelope, so -1 will be the value for "unused"
    """
    if len(description) > 10:
        print("This is going to break because the description is too long")
    assert isinstance(run_id, int) & isinstance(env_stgpr_id, int) &\
           isinstance(env_me_id, int) & \
           isinstance(description, str), "Data types are wrong"
    assert (env_stgpr_id != -1 and env_me_id == -1) | \
           (env_stgpr_id == -1 and env_me_id != -1),\
           "You can't use two envelopes in one run"
    if env_stgpr_id != -1:
        insert_env = """SQL""".format(e=env_stgpr_id, d=description)
        q = "SQL".format(env_stgpr_id)
        et = pd.read_sql("DB QUERY")
        q2 = "SQL".format(env_stgpr_id)
        rt = pd.read_sql("DB QUERY")

    elif env_me_id != -1:
        insert_env = """SQL""".format(e=env_me_id, d=description)
        q = "SQL".format(env_me_id)
        et = pd.read_sql("DB QUERY")
        q2 = "SQL".format(env_me_id)
        rt = pd.read_sql("DB QUERY")
    update_env = """SQL""".format(st=env_stgpr_id, me=env_me_id, r=run_id)


    if et.shape[0] == 0:
        engine.execute(insert_env)
    else:
        print("It looks like this envelope version id is already present in the envelope table")

    if rt.shape[0] == 0:
        engine.execute(update_env)
    else:
        print("It looks like this envelope version id is already present in the run table")

    return

def map_version(run_id, map_version_int, engine):
    """
    Update the run_id table to include the map version used on a run.

    Note- This field doesn't currently exist in our run table (2/22/2019)
    """

    q = "SQL".format(run_id)
    rt = pd.read_sql("DB QUERY")

    if rt['map_version'] != None:

        run_map_v = """SQL""".format(map=map_version_int, r=run_id)
        engine.execute(run_map_v)
    else:
        print("This map version is already present in the run table")
    return


def update_tables(user, run_id, env_stgpr_id, env_me_id, description, cf_version_id):
    engine = clinical_funcs.get_engine(odbc_filepath="FILEPATH".format(user),
                                      db_name='clinical')

    cf_version(run_id=run_id, cf_version_id=cf_version_id, description=description, engine=engine)
    env_version(run_id=run_id, env_stgpr_id=env_stgpr_id, env_me_id=env_me_id, description=description, engine=engine)

    return
