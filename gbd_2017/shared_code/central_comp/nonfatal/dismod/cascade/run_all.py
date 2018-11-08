import sys
import os
from functools32 import lru_cache
import pandas as pd

from cascade_ode.settings import load as load_settings
from cascade_ode import db
from cascade_ode.setup_logger import setup_logger
from cascade_ode import sge

# Set default file mask to readable-for all users
os.umask(0o0002)

# Load settings from file
settings = load_settings(check_for_custom_conda=True)


@lru_cache()
def prepare_directories(mvid):
    """ Define and creat directories for cascade outputs and logs """
    logdir = '%s/%s' % (settings['log_dir'], mvid)
    try:
        os.makedirs(logdir)
    except:
        pass
    root_dir = '%s/%s' % (settings['cascade_ode_out_dir'], mvid)
    try:
        os.makedirs(root_dir)
    except:
        pass
    try:
        os.chmod(logdir, 0o775)
    except:
        pass
    try:
        os.chmod(root_dir, 0o775)
    except:
        pass
    return {'logdir': logdir, 'root_dir': root_dir}


def submit_global(mvid, project, dirs):
    """ Submit the global dismod_ode job. This job will attempt to run
    the entire cascade."""
    logdir = dirs['logdir']
    gfile = os.path.join(settings['code_dir'], "run_global.py")
    jobname = 'dm_%s_boot' % mvid
    jid = sge.qsub_w_retry(
        gfile,
        jobname,
        jobtype='python',
        project=project,
        slots=15,
        memory=30,
        parameters=[mvid],
        conda_env=settings['conda_env'],
        environment_variables=settings['env_variables'],
        prepend_to_path=os.path.join(settings['conda_root'], 'bin'),
        stderr='%s/%s.error' % (logdir, jobname))
    return jid


def get_meid(mvid):
    ''' Given a model_version_id, return its modelable_entity_id. Uses
    ENVIRONMENT_NAME environment variable to determine which database to read
    from'''
    sett = load_settings()
    eng = db.get_engine(conn_def='epi',
                        env=sett['env_variables']['ENVIRONMENT_NAME'])
    meid = pd.read_sql("""
        SELECT modelable_entity_id FROM epi.model_version
        WHERE model_version_id = %s""" % mvid, eng)
    return meid.values[0][0]


def main():
    ''' Main entry point to launching a dismod model via Epi-Viz. Reads
    model_version_id from command line arguments, creates directories, and
    qsubs cascade job'''
    setup_logger()
    mvid = int(sys.argv[1])
    meid = get_meid(mvid)
    if meid in [9422, 7695, 1175, 10352, 9309]:
        project = "proj_tb"
    else:
        project = "proj_dismod"
    dirs = prepare_directories(mvid)
    submit_global(mvid, project, dirs)
