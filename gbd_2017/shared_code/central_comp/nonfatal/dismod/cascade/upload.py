from glob import glob
import os
import logging

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


def upload_file(mvid, filepath, table, cols):
    '''Infile a csv'''
    log = logging.getLogger(__name__)
    log.info('Uploading %s to mvid %s' % (filepath, mvid))
    load_data_query = """
        LOAD DATA INFILE '{fp}'
        INTO TABLE {table}
        FIELDS
            TERMINATED BY ","
            OPTIONALLY ENCLOSED BY '"'
        LINES
            TERMINATED BY "\\n"
        IGNORE 1 LINES
        ({cols})
        SET model_version_id={mvid}""".format(
        fp=filepath, table=table, cols=",".join(cols), mvid=mvid)
    eng = db.get_engine(conn_def="epi",
                        env=settings['env_variables']['ENVIRONMENT_NAME'])
    conn = eng.connect()
    res = conn.execute(load_data_query)
    conn.close()
    return res


def set_commit_hash(mvid, ch):
    '''Update epi.model_version table with git commit hash'''
    update_stmt = """
        UPDATE epi.model_version
        SET code_version='{ch}'
        WHERE model_version_id={mvid}""".format(
        ch=ch, mvid=mvid)
    eng = db.get_engine(conn_def="epi",
                        env=settings['env_variables']['ENVIRONMENT_NAME'])
    conn = eng.connect()
    res = conn.execute(update_stmt)
    conn.close()
    return res


def upload_final(mvid):
    '''Upload all csvs in summaries folder of model version directory to
    epi.model_estimate_final'''
    searchdir = '%s/%s/full/summaries' % (
        settings['cascade_ode_out_dir'], mvid)
    summ_files = glob('%s/*.csv' % searchdir)
    log = logging.getLogger(__name__)

    for i, sf in enumerate(summ_files):
        log.info('Final file #:%s' % i)
        upload_file(mvid, sf,
                    'epi.model_estimate_final',
                    ['location_id', 'year_id', 'age_group_id', 'sex_id',
                     'measure_id', 'mean', '@dummy', 'lower', 'upper'])


def upload_model(mvid):
    '''Infile csvs to epi.model_estimate_fit, epi.model_prior,
    epi.model_data_adj, and epi.model_effect '''
    log = logging.getLogger(__name__)

    matches = []
    prior_matches = []
    searchdir = 'FILEPATH' % (
        settings['cascade_ode_out_dir'], mvid)
    locdirs = glob('%s/*' % searchdir)
    demo = Demographics()
    for ld in locdirs:
        for sex in ['male', 'female', 'both']:
            for year in demo.year_ids:
                outfile = 'FILEPATH' % (
                    ld, sex, year)
                if os.path.isfile(outfile):
                    matches.append(outfile)
                priorfile = 'FILEPATH' % (ld, sex, year)
                if os.path.isfile(priorfile):
                    prior_matches.append(priorfile)

    for i, m in enumerate(matches):
        log.info('Fit file #:%s' % i)
        try:
            upload_file(mvid,
                        m,
                        'epi.model_estimate_fit',
                        ['location_id', 'year_id', 'sex_id', 'age_group_id',
                         'measure_id', 'mean', 'lower', 'upper'])
        except Exception as e:
            log.exception(e)

    for i, m in enumerate(prior_matches):
        log.info('Prior file #:%s' % i)
        try:
            upload_file(mvid,
                        m,
                        'epi.model_prior',
                        ['year_id', 'location_id', 'sex_id', 'age_group_id',
                         'age', 'measure_id', 'mean', 'lower', 'upper'])
        except Exception as e:
            log.exception(e)

    try:
        log.info('Uploading adjusted data')
        adj_data_file = ('FILEPATH'
                         'model_data_adj.csv' % (
                             settings['cascade_ode_out_dir'], mvid))
        upload_file(mvid, adj_data_file, 'epi.model_data_adj',
                    ['model_version_dismod_id', 'sex_id', 'year_id', 'mean',
                     'lower', 'upper'])
    except Exception as e:
        log.exception(e)

    try:
        log.info('Uploading effects')
        me_file = ('FILEPATH'
                   'model_effect.csv' % (
                       settings['cascade_ode_out_dir'], mvid))
        upload_file(mvid, me_file, 'epi.model_effect',
                    ['measure_id', 'parameter_type_id', 'cascade_level_id',
                     'study_covariate_id', 'country_covariate_id',
                     'asdr_cause', 'location_id', 'mean_effect',
                     'lower_effect', 'upper_effect'])
    except Exception as e:
        log.exception(e)


def upload_fit_stat(mvid):
    '''Infile model_version_fit_stat.csv to epi.model_version_fit_stat'''
    mvfs_file = 'FILEPATH' % (
        settings['cascade_ode_out_dir'], mvid)

    upload_file(mvid, mvfs_file,
                'epi.model_version_fit_stat',
                ['measure_id', 'fit_stat_id', 'fit_stat_value'])


def update_model_status(mvid, status):
    '''Update epi.model_version_status for given model version id with
    model_version_status_id provided'''
    eng = db.get_engine(conn_def="epi",
                        env=settings['env_variables']['ENVIRONMENT_NAME'])
    conn = eng.connect()
    conn.execute("""
        UPDATE epi.model_version SET model_version_status_id=%s
        WHERE model_version_id=%s """ % (status, mvid))
    conn.close()
