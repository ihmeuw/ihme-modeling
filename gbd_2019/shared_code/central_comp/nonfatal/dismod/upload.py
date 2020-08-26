"""
All code that uploads results to databases is in this file.
Every function that uploads writes a logging message with
the word "Uploading", so that we can be sure test runs do
not upload results.
"""
from datetime import datetime
from glob import glob
import os
import logging
from contextlib import closing

import sqlalchemy
from sqlalchemy.sql import text

from cascade_ode.settings import load as load_settings
from cascade_ode.demographics import Demographics
from cascade_ode import db

# Set default file mask to readable-for all users
os.umask(0o0002)

# Path to this file
this_path = os.path.dirname(os.path.abspath(__file__))

# Get configuration options
settings = load_settings()

RUNNING = -1
FAILED = 7


def skip_if_uploaded(func):
    def decorated(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except sqlalchemy.exc.IntegrityError:
            log = logging.getLogger(__name__)
            log.debug(
                'Skipping file since already uploaded: {}'.format(args))
    return decorated


@skip_if_uploaded
def upload_file(mvid, filepath, table, cols, conn=None):
    '''Infile a csv'''
    log = logging.getLogger(__name__)
    log.debug(f'Uploading {filepath} to mvid {mvid}')
    load_data_query = f"""
        LOAD DATA INFILE '{filepath}'
        INTO TABLE {table}
        FIELDS
            TERMINATED BY ","
            OPTIONALLY ENCLOSED BY '"'
        LINES
            TERMINATED BY "\\n"
        IGNORE 1 LINES
        ({','.join(cols)})
        SET model_version_id={mvid}"""

    if not conn:
        eng = db.get_engine(conn_def="epi",
                            env=settings['env_variables']['ENVIRONMENT_NAME'])
        with closing(eng.connect()) as conn:
            res = conn.execute(load_data_query)
    else:
        res = conn.execute(load_data_query)

    return res


def set_commit_hash(mvid, ch):
    '''Update epi.model_version table with git commit hash'''
    update_stmt = text(
        """
        UPDATE epi.model_version
        SET code_version=:ch
        WHERE model_version_id=:mvid
        """
    )
    params = {'ch': ch, 'mvid': mvid}
    eng = db.get_engine(conn_def="epi",
                        env=settings['env_variables']['ENVIRONMENT_NAME'])
    with closing(eng.connect()) as conn:
        res = conn.execute(update_stmt, **params)
    return res


def upload_model(mvid):
    '''Infile csvs to epi.model_estimate_fit, epi.model_prior,
    epi.model_data_adj, and epi.model_effect '''
    log = logging.getLogger(__name__)

    matches = []
    prior_matches = []
    root_dir = settings['cascade_ode_out_dir']
    searchdir = f'{root_dir}/{mvid}/full/locations'
    locdirs = glob(f'{searchdir}/*')
    demo = Demographics(mvid)
    for ld in locdirs:
        for sex in ['male', 'female', 'both']:
            for year in demo.year_ids:
                outfile = f'{ld}/outputs/{sex}/{year}/model_estimate_fit.csv'
                if os.path.isfile(outfile):
                    matches.append(outfile)
                priorfile = f'{ld}/inputs/{sex}/{year}/model_prior.csv'
                if os.path.isfile(priorfile):
                    prior_matches.append(priorfile)
    log.info(f'Uploading model to mvid {mvid} with {len(matches)} '
             f'files and {len(prior_matches)} prior files.')

    eng = db.get_engine(conn_def="epi",
                        env=settings['env_variables']['ENVIRONMENT_NAME'])

    with closing(eng.connect()) as conn:
        for i, m in enumerate(matches):
            log.debug(f'Fit file #:{i}')
            upload_file(
                mvid,
                m,
                'epi.model_estimate_fit',
                ['location_id', 'year_id', 'sex_id', 'age_group_id',
                 'measure_id', 'mean', 'lower', 'upper'],
                conn)

        for i, m in enumerate(prior_matches):
            log.debug(f'Prior file #:{i}')
            upload_file(mvid,
                        m,
                        'epi.model_prior',
                        ['year_id', 'location_id', 'sex_id', 'age_group_id',
                         'age', 'measure_id', 'mean', 'lower', 'upper'],
                        conn)

        log.info('Uploading adjusted data')
        adj_data_file = (
            f'{root_dir}/{mvid}/full/locations/1/outputs/both/2000/'
            'model_data_adj.csv')
        upload_file(mvid, adj_data_file, 'epi.model_data_adj',
                    ['model_version_dismod_id', 'sex_id', 'year_id', 'mean',
                     'lower', 'upper'],
                    conn)

        log.info('Uploading effects')
        me_file = (
            f'{root_dir}/{mvid}/full/locations/1/outputs/both/2000/'
            'model_effect.csv')
        # if we've uploaded this before, we won't get a Integrity error, so
        # lets check via counting rows
        query_str = ('select count(*) from epi.model_effect where '
                     f'model_version_id = {mvid} group by model_version_id')
        result = conn.execute(query_str).fetchone()
        row_count = 0 if not result else result[0]

        if row_count == 0:
            upload_file(mvid, me_file, 'epi.model_effect',
                        ['measure_id', 'parameter_type_id', 'cascade_level_id',
                         'study_covariate_id', 'country_covariate_id',
                         'asdr_cause', 'location_id', 'mean_effect',
                         'lower_effect', 'upper_effect'],
                        conn)
        else:
            log.info('Skipping effects file since already uploaded')


def upload_fit_stat(mvid):
    '''Infile model_version_fit_stat.csv to epi.model_version_fit_stat'''
    mvfs_file = (f'{settings["cascade_ode_out_dir"]}/{mvid}/full/'
                 'model_version_fit_stat.csv')

    upload_file(mvid, mvfs_file,
                'epi.model_version_fit_stat',
                ['measure_id', 'fit_stat_id', 'fit_stat_value'])


def update_model_status(mvid, status):
    '''Update epi.model_version_status for given model version id with
    model_version_status_id provided'''
    log = logging.getLogger(__name__)
    log.info(f"Uploading model status for {mvid} to {status}.")
    eng = db.get_engine(conn_def="epi",
                        env=settings['env_variables']['ENVIRONMENT_NAME'])
    update_statement = text(
        """
        UPDATE epi.model_version
        SET model_version_status_id=:status
        WHERE model_version_id=:mvid
        """
    )
    with closing(eng.connect()) as conn:
        conn.execute(update_statement, status=status, mvid=mvid)


def update_run_time(mvid):
    '''Given model version id, updates model_version_run_start to time of
    function call. Uses ENVIRONMENT_NAME to determine which db to update'''
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log = logging.getLogger(__name__)
    log.info(f"Uploading run time {now} for {mvid}.")
    query = """
        UPDATE epi.model_version
        SET model_version_run_start='%s'
        WHERE model_version_id=%s""" % (now, mvid)
    eng = db.get_engine(conn_def='epi',
                        env=settings['env_variables']['ENVIRONMENT_NAME'])
    conn = eng.connect()
    conn.execute(query)
    conn.close()
