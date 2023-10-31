import glob
import json
import logging
import os
import warnings
from datetime import datetime

import pandas as pd
import requests
from tqdm import tqdm

from db_tools import ezfuncs

logger = logging.getLogger(__name__)

QPID_API = 'URL'
QPID_FILE = 'FILEPATH/codem/qpid_cache/qpid_{d}.csv'
QPID_MAX_JOBS = 50000
# Default parameters to use for new cluster, if we don't have any previous
# runs saved for this set of demographics.
DEFAULT_PARAMS = {'cores_requested': [50], 'ram_gb': [100],
                  'ram_gb_requested': [100], 'runtime_min': [60*60*5]}
PROJECTS = ['proj_codem', 'proj_codem_wave1', 'proj_codem_wave2',
            'proj_codem_wave3', 'proj_codem_wave4']
START_DATE = datetime(2019, 7, 9)


def query_jobs_in_date_range(start_date, limit, project):
    """
    Get the jobs from the QPID API run in a given range.
    :param start_date: (datetime)
    :param limit: (int) number of jobs to pull from QPID
    :param project: (str) the cluster project
    :return: pd.DataFrame with all of the job information
    """
    logger.info(f"Checking starting at {start_date}, max of {limit} jobs, project {project}")
    jobs = requests.get(QPID_API.format(cluster='fair'), params={
        'limit': limit,
        'project': project,
        'ran_after': start_date,
        'job_prefix': 'cod_'
    })
    jobs.raise_for_status()
    try:
        return pd.DataFrame(jobs.json())
    except json.decoder.JSONDecodeError:
        return pd.DataFrame()


def query_jobs(start_date=START_DATE, limit=QPID_MAX_JOBS, project=PROJECTS):
    # Qpid API can pull a max of 50000 jobs. This means to obtain a full set
    # of jobs for several months, we need to make multiple API requests.
    qpid_data = []
    for p in project:
        max_date = start_date
        logger.info(f"Pulling jobs from QPID API for cluster project {p}.")
        while max_date < datetime.today():
            df = query_jobs_in_date_range(
                start_date=max_date, limit=limit, project=p)
            if df.empty:
                break
            qpid_data.append(df)
            min_date = datetime.strptime(
                df.submission_time.min().split("T")[0],"%Y-%m-%d")
            max_date = datetime.strptime(
                df.submission_time.max().split("T")[0],"%Y-%m-%d")
            if max_date == min_date:
                break
    df = pd.concat(qpid_data).drop_duplicates()
    df = df.loc[df.job_name.str.startswith('cod_')]
    df = df.loc[(df.exit_status == 0) & (df.failed == 0) & (df.ram_gb >= 0)]
    return df


def get_jobs(start_date=START_DATE, end_date=datetime.now(), hybridizer=False):
    """
    Get the codem jobs either by reading in an existing csv or pulling from the QPID
    API.

    :param start_date: a datetime object
    :param end_date: a datetime object
    :param hybridizer: whether or not to pull hybrid jobs instead of codem jobs
    :return: pd.DataFrame of jobs
    """
    date_string = "{}_{}_{}-{}_{}_{}".format(start_date.year, start_date.month, start_date.day,
                                             end_date.year, end_date.month, end_date.day)
    qpid_file = QPID_FILE.format(d=date_string)
    if os.path.exists(qpid_file):
        logger.info(f"Reading existing qpid file {qpid_file}.")
        jobs = pd.read_csv(qpid_file)
    else:
        last_qpid_file = max(glob.glob(QPID_FILE.format(d="*")), key=os.path.getctime)
        logger.info(f"Reading last qpid file {last_qpid_file} and pulling in new jobs.")
        jobs = pd.read_csv(last_qpid_file)
        try:
            max_date = datetime.strptime(
                jobs.submission_time.max().split("T")[0],"%Y-%m-%d")
            jobs = jobs.append(query_jobs(start_date=max_date)).drop_duplicates()
            logger.info("Saving new qpid file to csv.")
            jobs.to_csv(qpid_file, index=False)
        except requests.exceptions.HTTPError:
            logger.info("Qpid error, unable to pull new jobs, using last file only.")
    if hybridizer:
        jobs = jobs.loc[jobs.job_name.str.contains('_hybrid')]
    else:
        jobs = jobs.loc[~jobs.job_name.str.contains('_hybrid')]
    jobs['model_version_id'] = jobs.job_name.apply(lambda x: int(x.split("_")[1]))
    return jobs


def get_model_versions(cause_id, age_start, age_end, model_version_type_id):
    """
    Get model versions for a given set of demographic params.

    :param cause_id: (int)
    :param age_start: (int)
    :param age_end: (int)
    :param model_version_type_id: (int)
    :return: list of model versions
    """
    model_versions = ezfuncs.query(
        """
        SELECT model_version_id
        FROM cod.model_version
        WHERE cause_id = :cause_id
        AND age_start = :age_start
        AND age_end = :age_end
        AND model_version_type_id = :model_version_type_id
        AND gbd_round_id > 5
        AND status = 1
        """,
        parameters={
            "cause_id": cause_id, "age_start": age_start, "age_end": age_end,
            "model_version_type_id": model_version_type_id},
        conn_def='codem')['model_version_id'].tolist()
    return model_versions


def check_covariate_selection(mvid, conn_def='codem'):
    select = ezfuncs.query(
        """
        SELECT run_covariate_selection
        FROM cod.model_version
        WHERE model_version_id = :model_verison_id
        """,
        parameters={"model_version_id": mvid},
        conn_def=conn_def)['run_covariate_selection'][0]
    multiple = ezfuncs.query(
        """
        SELECT COUNT(*) AS count
        FROM cod.model_version_log
        WHERE model_version_id = :model_version_id
        AND model_version_log_entry = 'Running covariate selection started.'
        """,
        parameters={"model_version_id": mvid},
        conn_def=conn_def)['count'][0] > 1
    return bool(select) and not multiple


def get_parameters(cause_id, age_start, age_end, model_version_type_id,
                   hybridizer=False, start_date=START_DATE, end_date=datetime.now(),
                   jobs=None):
    """
    Pull the run parameters for a given cause ID, age range, and model version type.
    Add a little bit of padding for runtime and memory.

    :param cause_id: (int)
    :param age_start: (int)
    :param age_end: (int)
    :param model_version_type_id: (int) global or data rich
    :param hybridizer: (bool) hybrid jobs
    :param start_date: (datetime) starting date to look for jobs
    :param end_date: (datetime) end date to look for jobs
    :param jobs: pd.DataFrame of jobs already read in -- will use this to not read in the csv
                 more often than necessary
    :return: dictionary with run parameters for cores, mem, and runtime
    """
    if model_version_type_id == 3 and not hybridizer:
        raise ValueError(
            "Cannot pull non-hybridizer model version type IDs when asked for hybridizer jobs.")
    if hybridizer:
        logger.info("Getting hybrid parameters for cause_id {c}, age_start {s}, age_end {e}".format(
            c=cause_id, s=age_start, e=age_end)
        )
        model_versions = get_model_versions(cause_id, age_start, age_end, model_version_type_id)
        if jobs is None:
            jobs = get_jobs(start_date=start_date, end_date=end_date, hybridizer=hybridizer)
        jobs = jobs.loc[jobs.model_version_id.isin(model_versions)]
        jobs = jobs.loc[(jobs.exit_status == 0) & (jobs.failed == 0)]
        if jobs.empty:
            jobs = pd.DataFrame.from_dict(DEFAULT_PARAMS, orient='columns')
            warnings.warn("QPID did not capture any run information for these model versions {}.".format(
                ', '.join([str(x) for x in model_versions])
            ), RuntimeWarning)
        jobs.loc[jobs.ram_gb < 0, 'ram_gb'] = jobs.loc[jobs.ram_gb < 0]['ram_gb_requested']
        parameters = jobs[['cores_requested', 'ram_gb', 'ram_gb_requested', 'runtime_min']].mean().to_dict()
    else:
        logger.info("Setting non-hybrid parameters for cause_id {c}, age_start {s}, age_end {e}".format(
            c=cause_id, s=age_start, e=age_end)
        )
        parameters = {
            'cores_requested': 3,
            'ram_gb': 1,
            'ram_gb_requested': 1,
            'runtime_min': int(60*24*12)
        }
    logger.info(f"parameters: {parameters}")
    return parameters

def get_demographic_dictionary(cause_id, hybridizer=False):
    """
    Get the demographic/model type IDs associated with a given cause.

    :param cause_id: int
    :param hybridizer: bool
    :return: dictionary with nested demographics
    """
    model_version_type_ids = [3] if hybridizer else [1, 2, 11]
    df = ezfuncs.query("""
           SELECT DISTINCT model_version_type_id, age_start, age_end
           FROM cod.model_version
           WHERE cause_id = :cause_id
           AND gbd_round_id > 5
           AND status = 1
           AND model_version_type_id IN :model_version_type_ids
           """,
           conn_def='codem',
           parameters={"cause_id": cause_id,
                       "model_version_type_ids": model_version_type_ids}
    )
    if df.empty:
        raise RuntimeError(f"There are no previous model versions for cause ID {cause_id}")
    keys = {}
    for m in df.model_version_type_id.unique().tolist():
        keys[m] = {}
        for a_start in df.query('model_version_type_id == {}'.format(m))['age_start'].unique().tolist():
            keys[m][a_start] = {}
            for a_end in df.query('model_version_type_id == {} & '
                                  'age_start == {}'.format(m, a_start))['age_end'].unique().tolist():
                keys[m][a_start][a_end] = {}
    return keys


def get_all_parameters(cause_ids, hybridizer=False, start_date=START_DATE,
                       end_date=datetime.now()):
    """
    Pull all of the run parameters for a given cause ID and *ALL* associated combinations of model version
    type ID and age_start, age_end.
    :param cause_ids: (list of int) cause_ids
    :param hybridizer: bool
    :param start_date: (datetime) starting date to look for jobs
    :param end_date: (datetime) end date to look for jobs
    :return: multi-level dictionary to be keyed by cause-id, model_version_type_id, age_start, age_end
    """
    cause_ids = cause_ids if type(cause_ids) is list else [cause_ids]
    jobs = get_jobs(start_date, end_date, hybridizer)
    dictionary = {}
    for c in cause_ids:
        dictionary[c] = get_demographic_dictionary(c, hybridizer)
        for m in dictionary[c]:
            for a in dictionary[c][m]:
                for e in dictionary[c][m][a]:
                    dictionary[c][m][a][e] = get_parameters(
                        cause_id=c, age_start=a, age_end=e,
                        model_version_type_id=m, hybridizer=hybridizer,
                        jobs=jobs)
    return dictionary
