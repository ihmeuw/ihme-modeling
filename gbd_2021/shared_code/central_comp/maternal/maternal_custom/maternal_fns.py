"""
maternal_fns.py is a selection of helper scripts for 00_main_maternal.py
as well as its sub-modules 01_, 02_, 03_, 04.
"""

from datetime import datetime
from typing import List
import os
import re
import time
import logging

import pandas as pd

from db_tools.ezfuncs import query
from db_queries import get_demographics, get_best_model_versions
from gbd import decomp_step as decomp
from gbd.estimation_years import estimation_years_from_gbd_round_id
from qstat import qstat_cluster

logger = logging.getLogger("maternal_custom.maternal_fns")

MATERNAL_AGE_GROUP_IDS = list(range(7, 16))
HARDCODED_EPI_DECOMP_STEP_ID = 15


def get_all_years(gbd_round_id) -> List[int]:
    """ Get a list of all relevant estimation years."""
    return sorted(list(
        range(1980, estimation_years_from_gbd_round_id(gbd_round_id)[-1] + 1)
    ))


def wait(pattern, seconds):
    '''
    Pause the main script until certain sub-jobs are finished.

    Args:
        1. pattern: the pattern of the jobname that you want to wait for
        2. seconds: number of seconds you want to wait
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
    Returns a list of the only columns needed for doing math
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


def get_time():
    '''
    get timestamp in a format you can put in filepaths

    Args: None

    Output: (string) date_str: string of format '{year}_{month}_{day}_{hour}'
    '''
    date_regex = re.compile('\W')
    date_unformatted = str(datetime.now())[0:13]
    date_str = date_regex.sub('_', date_unformatted)
    return date_str


def get_locations(decomp_step_id):
    '''
    get list of locations to iterate through for every part of the
    maternal custom process, down to one level of subnationals

    Args: None

    Output: (list) location_ids
    '''
    logger.info("Getting locations")
    gbd_round_id = decomp.gbd_round_id_from_decomp_step_id(decomp_step_id)
    return get_demographics(
        gbd_team="cod", gbd_round_id=gbd_round_id)["location_id"]


def check_dir(filepath):
    '''
    Checks if a file path exists. If not, creates the file path.

    Args: (str) a file path

    Output: (str) the file path that already existed or was created if it
    didn't already exist
    '''

    if not os.path.exists(filepath):
        os.makedirs(filepath)
    return filepath


def get_model_vers(entity, decomp_step_id, model_id=None, step=None) -> int:
    '''
    Queries the database for the best model_version for the given
    model_id. Can do this for Dismod, Codem, or Codcorrect outputs.

    Args:
        entity (str) 'cause', 'modelable_entity', or 'codcorrect'
        decomp_step_id (int) decomp_step_id.
        model_id (int) modelable_entity_id for dismod, cause_id for codem, or
            None for codcorrect
        step (int) 'step' of 00_main_maternal.py

    Output: (int) best model_version
    '''
    if (
        model_id is None and
        entity in ['cause', 'modelable_entity']
    ):
        raise ValueError(f'Must specify a me_id for entity={entity}')

    def get_latest_best_codcorrect():
        q = '''SELECT MAX(CAST(output_version_id AS UNSIGNED)) as id FROM cod.output_version
               WHERE status=1 and is_best = 1 and best_end IS NULL'''
        return query(q, conn_def='cod').loc[0, 'id']

    if entity == 'codcorrect':
        return get_latest_best_codcorrect()

    decomp_step = decomp.decomp_step_from_decomp_step_id(decomp_step_id)
    model_version_df = get_best_model_versions(
        entity,
        ids=model_id,
        decomp_step=decomp_step
    )
    if len(model_version_df) != 1:
        raise ValueError(
            f"get_best_model_versions returned more/less than 1 row with args:\n"
            f"entity:{entity}\nids={model_id}\ndecomp_step={decomp_step}"
        )   

    if step == 2 and entity == 'cause':
        for idx, _row in model_version_df.iterrows():
            if "HYBRID" in str.upper(model_version_df.at[idx, 'description']):
                return model_version_df.at[idx, 'model_version_id']
        raise ValueError("No hybrid model marked best for 366")
    else:
        return model_version_df.at[0, 'model_version_id']


def get_best_date(entity, model_id, step, decomp_step_id):
    '''
    Queries the database for the best_start date of the most
    recent model version for the given cause_id/modelable_entity_id/process.
    Also pulls the most recent date of timestamped files.
    See dependency_map.csv for context.

    Args:
        step: step of the process we're on

    Output: (datetime) best start date
    '''
    valid_types = ['modelable_entity', 'cause', 'codcorrect']
    if entity not in valid_types:
        raise RuntimeError(f'entity {entity} invalid!')

    if entity == 'modelable_entity':
        decomp_step_id = get_epi_decomp_step(decomp_step_id)
    decomp_step = decomp.decomp_step_from_decomp_step_id(decomp_step_id)

    if entity != 'codcorrect':
        model_id = int(model_id)
        best_df = get_best_model_versions(
            entity, ids=[model_id], decomp_step=decomp_step)
    else:
        qry = '''
            SELECT best_start FROM cod.output_version WHERE
            env_version = (
                SELECT MAX(env_version) FROM cod.output_version
                where is_best=1 and status=1 and best_end IS NULL
            )
            AND is_best = 1
        '''
        best_df = query(qry, conn_def='cod')

    # return really early datetime if it doesn't exist, otherwise return best date.
    if len(best_df) == 0 or best_df.loc[0, 'best_start'] is None:
        return datetime(1800, 1, 1, 0, 0, 0)
    else:
        return best_df.loc[0, 'best_start'].to_pydatetime()


def check_dependencies(step: str, dep_map_type: str, decomp_step_id: int):
    '''Confirm target ids marked best before source ids, implying targets not from source.

    Args:
        step: Which maternal-split step for which you want to check dependencies
        dep_map_type: Which dependency map to query. 
            'mmr' or 'props', probably mmr if it's after gbd2017
        decomp_step_id: decomp_step_id used to check

    Output: True or False, which turns the step specified to 'On' or 'Off'
    '''
    valid_steps = [1, 2, 3, 4]
    if step not in valid_steps:
        raise RuntimeError(f'supplied step {step} invalid, valid steps: {valid_steps}')

    dep_map = pd.read_csv("dependency_map_%s.csv" % dep_map_type, header=0)\
        .dropna(axis='columns', how='all')
    step_df = dep_map.loc[dep_map['step'] == step]
    if step_df.empty:
        raise RuntimeError(f'supplied step: {step}, dep_map_type: {dep_map_type} invalid')

    bool_list = []
    for idx in step_df.index:
        [source_type, source_id, target_type, target_id] = step_df.loc[
            idx,
            ["source_type", "source_id", "target_type", "target_id"]
        ].tolist()

        src_date = get_best_date(source_type, source_id, step, decomp_step_id)
        trg_date = get_best_date(target_type, target_id, step, decomp_step_id)
        bool_list.append(src_date > trg_date)

    # return all(bool_list)
    return True


def get_epi_decomp_step(decomp_step_id):
    gbd_round_id = decomp.gbd_round_id_from_decomp_step_id(decomp_step_id)
    if gbd_round_id == 7:
        return HARDCODED_EPI_DECOMP_STEP_ID
    return decomp_step_id
