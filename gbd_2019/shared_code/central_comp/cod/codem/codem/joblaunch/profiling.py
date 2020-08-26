import requests
import os
import logging
import warnings
from datetime import datetime, timedelta
import pandas as pd
from tqdm import tqdm

from db_tools.ezfuncs import query

logger = logging.getLogger(__name__)

QPID_API = 'ADDRESS'
QPID_FILE = 'FILEPATH'
# Default parameters to use for new cluster, if we don't have any previous
# runs saved for this set of demographics.
DEFAULT_PARAMS = {'cores_requested': [50], 'ram_gb': [100],
                  'ram_gb_requested': [100], 'runtime_min': [60*60*5]}


def list_check(f):
    return f if type(f) is list else [f]


def pull_jobs(start_date, end_date=datetime.now(), limit=50000,
              project=['proj_codem']):
    """
    Get the jobs from the QPID API run in a given range.
    :param start_date: (datetime)
    :param end_date: (datetime)
    :param limit: (int) number of jobs to pull from QPID
    :param project: (str) the cluster project
    :return: pd.DataFrame with all of the job information
    """
    logger.info("Pulling jobs from QPID API.")
    logger.info(f"Checking {start_date} to {end_date}")
    dfs = []
    for p in project:
        logger.info(f"Checking project {p}")
        jobs = requests.get(QPID_API.format(cluster='fair'), params={
            'limit': limit,
            'project': p,
            'ran_after': start_date,
            'job_prefix': 'cod_'
        }).json()
        df = pd.DataFrame(jobs)
        dfs.append(df)
    df2 = pd.concat(dfs)
    return df2


def get_jobs(start_date=datetime(2019, 1, 11), end_date=datetime.now(),
             hybridizer=False):
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
    logger.info("Pulling in new qpid file.")
    jobs = pull_jobs(start_date=start_date, end_date=end_date)
    jobs = jobs.loc[jobs.job_name.str.contains('cod_')]
    logger.info("Saving new qpid file to csv.")
    jobs.to_csv(qpid_file.format(d=date_string), index=False)
    if hybridizer:
        jobs = jobs.loc[jobs.job_name.str.contains('_hybrid_')]
    else:
        jobs = jobs.loc[~jobs.job_name.str.contains('_hybrid_')]
    jobs['model_version_id'] = jobs.job_name.apply(lambda x: int(x.split("_")[1]))
    jobs.drop_duplicates(inplace=True)
    return jobs


def model_version_type_ids(hybridizer):
    """
    Get the model version type IDs.

    :param hybridizer:
    :return:
    """
    if hybridizer:
        mts = [3]
    else:
        mts = [1, 2]
    return ', '.join([str(x) for x in mts])


def get_model_versions(cause_id, age_start, age_end, model_version_type_id):
    """
    Get model versions for a given set of demographic params.

    :param cause_id: (int)
    :param age_start: (int)
    :param age_end: (int)
    :param model_version_type_id: (int)
    :return: list of model versions
    """
    call = """
           SELECT model_version_id FROM cod.model_version
           WHERE cause_id = {c}
           AND age_start = {a_start} AND age_end = {a_end}
           AND model_version_type_id = {mvt}
           AND gbd_round_id > 5 AND status = 1
           """.format(c=cause_id, a_start=age_start, a_end=age_end,
                      mvt=model_version_type_id)
    model_versions = query(call, conn_def='codem')['model_version_id'].tolist()
    return model_versions


def get_demographic_dictionary(cause_id, hybridizer=False):
    """
    Get the demographic/model type IDs associated with a given cause.

    :param cause_id: int
    :param hybridizer: bool
    :return: dictionary with nested demographics
    """
    keys = {}
    call = """
           SELECT model_version_type_id, age_start, age_end FROM cod.model_version
           WHERE cause_id = {c} AND gbd_round_id > 5 AND status = 1
           AND model_version_type_id IN ({mvt})
           """.format(c=cause_id,
                      mvt=model_version_type_ids(hybridizer))
    df = query(call, conn_def='codem').drop_duplicates()
    if df.empty:
        raise RuntimeError("There are no previous model versions for cause {}.".format(cause_id))
    for m in df.model_version_type_id.unique().tolist():
        keys[m] = {}
        for a_start in df.query('model_version_type_id == {}'.format(m))['age_start'].unique().tolist():
            keys[m][a_start] = {}
            for a_end in df.query('model_version_type_id == {} & '
                                  'age_start == {}'.format(m, a_start))['age_end'].unique().tolist():
                keys[m][a_start][a_end] = {}
    return keys


def check_covariate_selection(mvid, conn_def='codem'):
    call = f'''SELECT run_covariate_selection FROM cod.model_version WHERE model_version_id = {mvid}'''
    select = query(call, conn_def=conn_def)['run_covariate_selection'][0]
    call = f"SELECT COUNT(*) AS count FROM cod.model_version_log WHERE model_version_id = {mvid} " \
        f"AND model_version_log_entry = 'Running covariate selection started.'"
    multiple = query(call, conn_def=conn_def)['count'][0] > 1
    return bool(select) and not multiple


def get_parameters(cause_id, age_start, age_end, model_version_type_id,
                   hybridizer=False,
                   start_date=datetime(2019, 1, 11), end_date=datetime.now(),
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
        raise ValueError("Cannot pull non-hybridizer model version type IDs when you've asked for hybridizer jobs.")
    if hybridizer:
        logger.info("Getting parameters for cause_id {c}, age_start {s}, age_end {e}, and model_version_type_id {m}".format(
            c=cause_id, s=age_start, e=age_end, m=model_version_type_id
        ))
        model_versions = get_model_versions(cause_id, age_start, age_end, model_version_type_id)
        if jobs is None:
            jobs = get_jobs(start_date=start_date, end_date=end_date, hybridizer=hybridizer)
        jobs = jobs.loc[jobs.model_version_id.isin(model_versions)].copy()
        jobs['ran_covariate_selection'] = jobs['model_version_id'].apply(lambda x:
                                                                         check_covariate_selection(x, conn_def='codem'))
        logger.info(f"{jobs['ran_covariate_selection'].mean()*100} % of jobs ran covariate selection.")
        jobs.loc[~jobs.ran_covariate_selection, 'runtime_min'] = \
            jobs.loc[~jobs.ran_covariate_selection, 'runtime_min'] + 60*24*2
        jobs = jobs.loc[(jobs.exit_status == 0) & (jobs.failed == 0)]
        if jobs.empty:
            jobs = pd.DataFrame.from_dict(DEFAULT_PARAMS, orient='columns')
            warnings.warn("QPID did not capture any run information for these model versions {}.".format(
                ', '.join([str(x) for x in model_versions])
            ), RuntimeWarning)
        bad_jobs = jobs.loc[jobs.ram_gb == -1]
        if not bad_jobs.empty:
            warnings.warn("Cannot have -1 for ram GB. Defaulting to have the ram GB requested instead.", RuntimeWarning)
            for index, row in bad_jobs.iterrows():
                job_number = row['job_number']
                job_name = row['job_name']
                start_time = row['start_time']
                file_name = f"FILEPATH"
                if not os.path.exists(file_name):
                    f = open(file_name, 'w')
                    f.close()
        jobs.loc[jobs.ram_gb < 0, 'ram_gb'] = jobs.loc[jobs.ram_gb < 0]['ram_gb_requested']
        parameters = jobs[['cores_requested', 'ram_gb', 'ram_gb_requested', 'runtime_min']].mean().to_dict()
    else:
        parameters = {
            'cores_requested': 3,
            'ram_gb': 1,
            'ram_gb_requested': 1,
            'runtime_min': int(60*24*5)
        }
        logger.info(f"parameters: {parameters}")
    return parameters


def get_all_parameters(cause_ids, hybridizer=False,
                       start_date=datetime(2019, 1, 11), end_date=datetime.now()):
    """
    Pull all of the run parameters for a given cause ID and *ALL* associated combinations of model version
    type ID and age_start, age_end.
    :param cause_ids: (list of int) cause_ids
    :param hybridizer: bool
    :param start_date: (datetime) starting date to look for jobs
    :param end_date: (datetime) end date to look for jobs
    :return: multi-level dictionary to be keyed by cause-id, model_version_type_id, age_start, age_end
    """
    cause_ids = list_check(cause_ids)
    jobs = get_jobs(start_date, end_date, hybridizer)
    dictionary = {}
    for c in tqdm(cause_ids):
        dictionary[c] = get_demographic_dictionary(c, hybridizer)
        for m in dictionary[c]:
            for a in dictionary[c][m]:
                for e in dictionary[c][m][a]:
                    dictionary[c][m][a][e] = get_parameters(cause_id=c, age_start=a, age_end=e,
                                                            model_version_type_id=m,
                                                            hybridizer=hybridizer,
                                                            jobs=jobs)
    return dictionary


def param_df(df, gb_padding=50, min_padding=60*12):
    df2 = df.copy()
    params = get_all_parameters(df2.cause_id.unique().tolist())
    for index, row in tqdm(df2.iterrows()):
        these_params = params[row['cause_id']][row['model_version_type_id']][row['age_start']][row['age_end']]
        if (((these_params['runtime_min'] + min_padding) * 60) / (60 * 60 * 24)) > 3:
            logger.info("Long.q")
            queue = 'long.q'
        else:
            logger.info("All.q")
            queue = 'all.q'
        df2.set_value(index, 'cores', these_params['cores_requested'])
        df2.set_value(index, 'gigabytes', these_params['ram_gb'] + gb_padding)
        df2.set_value(index, 'minutes', these_params['runtime_min'])
        df2.set_value(index, 'queue', queue)

    return df2

