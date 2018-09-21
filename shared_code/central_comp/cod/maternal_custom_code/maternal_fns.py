import os
import re
import time
from datetime import datetime
import pandas as pd

from db_tools.ezfuncs import query
from db_queries import get_location_metadata
from db_queries import get_best_model_versions
from cluster_utils import submitter


def wait(pattern, seconds):
    '''
    Description: Pause the master script until certain sub-jobs are finished.

    Args:
        1. pattern: the pattern of the jobname that you want to wait for
        2. seconds: number of seconds you want to wait

    Output:
        None, just pauses the script
    '''
    seconds = int(seconds)
    while True:
        qstat = submitter.qstat()
        if qstat['name'].str.contains(pattern).any():
            print time.localtime()
            time.sleep(seconds)
            print time.localtime()
        else:
            break


def filter_cols():
    '''
    Description: Returns a list of the only columns needed for doing math
    on data frames within the maternal custom code. This is used to subset
    dataframes to only keep those columns.

    Args: None

    Output: (list) columns names: age_group_id and draws_0 - draw_999
    '''
    usecols = ['measure_id', 'location_id', 'year_id', 'age_group_id',
               'sex_id']
    for i in range(0, 1000, 1):
        usecols.append("draw_%d" % i)
    return usecols


# get date and time info
def get_time():
    '''
    Description: get timestamp in a format you can put in filepaths

    Args: None

    Output: (string) date_str: string of format '{year}_{month}_{day}_{hour}'
    '''
    date_regex = re.compile('\W')
    date_unformatted = str(datetime.now())[0:13]
    date_str = date_regex.sub('_', date_unformatted)
    return date_str


def get_locations():
    '''
    Description: get list of locations to iterate through for every part of the
    maternal custom process, down to one level of subnationals

    Args: None

    Output: (list) location_ids
    '''
    locations_df = get_location_metadata(location_set_id=35)
    return (locations_df[locations_df['most_detailed'] == 1][
            'location_id'].tolist())


def check_dir(filepath):
    '''
    Description: Checks if a file path exists. If not, creates the file path.

    Args: (str) a file path

    Output: (str) the file path that already existed or was created if it
    didn't already exist
    '''

    if not os.path.exists(filepath):
        os.makedirs(filepath)
    else:
        pass
    return filepath


def get_model_vers(entity, model_id=None, step=None):
    '''
    Description: Queries the database for the best model_version for the given
    model_id. Can do this for Dismod, Codem, or Codcorrect outputs.

    Args:
        1. (str) entity ('cause', 'modelable_entity', or 'codcorrect')
        2. id (modelable_entity_id for dismod, cause_id for codem, or
        none for codcorrect)

    Output: (int) best model_version
    '''
    if model_id is None and (entity == 'cause' or
                             entity == 'modelable_entity'):
        raise ValueError('Must specify a me_id')

    if entity != 'codcorrect':
        model_id = int(model_id)
        model_vers_df = get_best_model_versions(entity, ids=[model_id])
        if step == 2 and entity == 'cause':
            for i in model_vers_df.index:
                if "Hybrid" in model_vers_df.ix[i, 'description']:
                    model_version = model_vers_df.ix[i, 'model_version_id']
                else:
                    model_version = None
            if model_version:
                return model_version
            else:
                raise ValueError("No hybrid model marked best for 366")
        else:
            return model_vers_df.ix[0, 'model_version_id']
    else:
        q = '''SELECT output_version_id FROM cod.output_version
               WHERE status=1 and is_best = 1 and best_end IS NULL'''
        return query(q, conn_def='cod').ix[0, 'output_version_id']


def get_best_date(entity, model_id=None, step=None):
    '''
    Description: Queries the database for the best_start date of the most
    recent model version for the given cause_id/modelable_entity_id/process.
    Also pulls the most recent date of timestamped files.
    See dependency_map.csv for context.

    Args:
        enginer: connection to the db
        step: step of the process we're on
        dep_type: "cause_id" or "modelable_entity_id"
        dep_id: the modelable_entity_id, cod_correct id, or codem_id for
        which you want to get the best_start date.
        NOTE: dep_id REQUIRED for dismod process

    Output: (datetime) best start date
    '''
    if entity != 'codcorrect':
        model_id = int(model_id)
        best_df = get_best_model_versions(entity, ids=[model_id])
    else:
        q = '''
            SELECT best_start FROM cod.output_version WHERE
            env_version = (SELECT MAX(env_version) FROM cod.output_version
            where is_best=1 and status=1 and best_end IS NULL)
            and is_best = 1'''
        best_df = query(q, conn_def='cod')
    if len(best_df) == 0:
        return datetime(1800, 01, 01, 00, 00, 00)
    else:
        return best_df.ix[0, 'best_start'].to_datetime()


def check_dependencies(step, dep_map_type):
    '''
    Description: Checks dependencies of the step given, using the dependency
    map.

    Args: specify which step for which you want to check dependencies
        Options: 1, 2, 3, or '4'

    Output: True or False, which turns the step specified to 'On' or 'Off'
    '''
    dep_map = pd.read_csv("dependency_map_%s.csv" % dep_map_type,
                          header=0).dropna(axis='columns', how='all')
    step_df = dep_map.ix[dep_map.step == step]

    if len(step_df) != 0:
        bool_list = []
        for idx in step_df.index:
            src_date = get_best_date(step_df.ix[idx, "source_type"],
                                     step_df.ix[idx, "source_id"], step)
            trg_date = get_best_date(step_df.ix[idx, "target_type"],
                                     step_df.ix[idx, "target_id"], step)
            boolean = src_date > trg_date
            bool_list.append(boolean)

        if any(bool_list):
            return True
        else:
            return False
    else:
        raise ValueError("Must specify 1, 2, 3, or 4")
