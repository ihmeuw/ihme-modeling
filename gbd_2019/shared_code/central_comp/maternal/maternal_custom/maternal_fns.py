"""
maternal_fns.py is a selection of helper scripts for 00_master_maternal.py
as well as its sub-modules 01_, 02_, 03_, 04.
"""

import os
import re
import time
import logging
from datetime import datetime

import pandas as pd

from db_tools.ezfuncs import query
from db_queries import get_demographics, get_best_model_versions
from gbd import decomp_step as decomp
from qstat import qstat_cluster

GBD_ROUND_ID = 6

logger = logging.getLogger("maternal_custom.maternal_fns")

def wait(pattern, seconds):
    '''
    Description: Pause the master script until certain sub-jobs are finished.

    Args:
        1. pattern: the pattern of the jobname that you want to wait for
        2. seconds: number of seconds you want to wait

    Output:
        None
    '''
    seconds = int(seconds)
    while True:
        qstat = qstat_cluster()
        if qstat['name'].str.contains(pattern).any():
            print(time.localtime())
            time.sleep(seconds)
            print(time.localtime())
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
    logger.info("Getting locations")
    return get_demographics(
        gbd_team="cod", gbd_round_id=GBD_ROUND_ID)["location_id"]


def check_dir(filepath):
    '''
    Description: Checks if a file path exists. If not, creates the file path.

    Args: (str) a file path

    Output: (str) the file path that already existed or was created if it
    didn't already exist
    '''

    if not os.path.exists(filepath):
        os.makedirs(filepath)
    return filepath


def get_model_vers(entity, model_id=None, step=None, decomp_step_id=4):
    '''
    Description: Queries the database for the best model_version for the given
    model_id. Can do this for Dismod, Codem, or Codcorrect outputs.

    Args:
        entity (str) 'cause', 'modelable_entity', or 'codcorrect'
        model_id (int) modelable_entity_id for dismod, cause_id for codem, or
            None for codcorrect
        step (int) 'step' of 00_master_maternal.py
        decomp_step_id (int) decomp_step_id. Not required if
            entity=='codcorrect'

    Output: (int) best model_version
    '''
    if model_id is None and (entity == 'cause' or
                             entity == 'modelable_entity'):
        raise ValueError('Must specify a me_id')

    if entity != 'codcorrect':
        model_id = int(model_id)
        model_version_df = get_best_model_versions(
            entity,
            ids=[model_id],
            decomp_step=decomp.decomp_step_from_decomp_step_id(decomp_step_id))

        if len(model_version_df) > 1:
            raise ValueError(
                "get_best_model_versions more than one model_version_id for "
                "entity_id %s decomp_step_id %s" % (
                    model_id, decomp_step_id
                )
            )

        if step == 2 and entity == 'cause':
            for idx, _row in model_version_df.iterrows():
                if "HYBRID" in str.upper(model_version_df.at[idx, 'description']):
                    return model_version_df.at[idx, 'model_version_id']
            raise ValueError("No hybrid model marked best for 366")
        else:
            return model_version_df.at[0, 'model_version_id']
    else:
        q = '''SELECT MAX(CAST(output_version_id AS UNSIGNED)) as id FROM cod.output_version
               WHERE status=1 and is_best = 1 and best_end IS NULL'''
        return query(q, conn_def='cod').loc[0, 'id']


def get_best_date(entity, model_id=None, step=None, decomp_step_id=4):
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

    Output: (datetime) best start date
    '''
    
    decomp_step = decomp.decomp_step_from_decomp_step_id(decomp_step_id)
    if entity != 'codcorrect':
        model_id = int(model_id)
        best_df = get_best_model_versions(
            entity, ids=[model_id], decomp_step=decomp_step)
    else:
        q = '''
            SELECT best_start FROM cod.output_version WHERE
            env_version = (SELECT MAX(env_version) FROM cod.output_version
            where is_best=1 and status=1 and best_end IS NULL)
            and is_best = 1'''
        best_df = query(q, conn_def='cod')
    if len(best_df) == 0:
        return datetime(1800, 1, 1, 0, 0, 0)
    else:
        return best_df.loc[0, 'best_start'].to_pydatetime()


def check_dependencies(step, dep_map_type, decomp_step_id=1):
    '''
    Description: Checks dependencies of the step given, using the dependency
    map.

    Args: specify which step for which you want to check dependencies
        Options: 1, 2, 3, or '4'

    Output: True or False, which turns the step specified to 'On' or 'Off'
    '''

    dep_map = pd.read_csv("dependency_map_%s.csv" % dep_map_type,
                          header=0).dropna(axis='columns', how='all')
    step_df = dep_map.loc[dep_map.step == step]

    if len(step_df) != 0:
        bool_list = []
        for idx in step_df.index:

            [source_type, source_id, target_type, target_id] = step_df.loc[
                idx,
                ["source_type", "source_id", "target_type", "target_id"]
            ].tolist()

            src_date = get_best_date(source_type, source_id, step)
            trg_date = get_best_date(target_type, target_id, step)
            boolean = src_date > trg_date
            bool_list.append(boolean)

        if any(bool_list):
            return True
        else:
            return False
    else:
        raise ValueError("Must specify 1, 2, 3, or 4")
