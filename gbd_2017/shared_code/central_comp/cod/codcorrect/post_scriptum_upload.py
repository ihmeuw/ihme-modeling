"""
This script will be used to better manage the database assets that need to
be created after the gbd upload jobs are successful.

Steps:
1. Grab the current compare version.

2. Read get the current process versions to generate metadata for the new
compare version.

3. Generate the description for the compare version.

4. Create the new compare version.

5. Add process versions to the new compare version.

6. Activate the new process version.
"""


import argparse
import logging
import os
import time

from gbd_outputs_versions.compare_version import CompareVersion
from gbd_outputs_versions.db import DBEnvironment as DBEnv
import gbd.constants as GBD

from codcorrect.core import read_json
from codcorrect.database import (activate_new_process_version,
                                 activate_new_compare_version,
                                 get_latest_compare_version,
                                 update_status,
                                 get_gbd_compare_version_cache,
                                 get_gbd_process_version_cache,
                                 get_output_version_cache)
import codcorrect.log_utilities as cc_log


_GBD_CONN_DEF_MAP = {'prod': 'gbd', 'dev': 'gbd-test'}
_COD_CONN_DEF_MAP = {'prod': 'cod', 'dev': 'cod-test'}


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--output_version_id', type=int)
    parser.add_argument('--db', type=str)
    parser.add_argument('--test', action='store_true')

    args = parser.parse_args()
    return args.output_version_id, args.db, args.test


def get_process_desc_from_id(pvid, note):
    """
    Return a properly formatted compare version description for the new
    compare version created for this CoDCorrect run.
    """
    _PROCESS_TO_PROCESSOR = {
        1: 'COMO: {}'.format(note.split(' ')[-1]),
        4: 'Burdenator: {}'.format(note.split(', ')[0].split('v')[-1]),
        5: 'PAFs: {}'.format(note.split(', ')[-1].split()[-1])
    }
    return _PROCESS_TO_PROCESSOR[pvid]


def read_helper_files(parent_dir):
    """
    Return the config dictionary from json.

    Arguments:
        parent_dir (str):

    Returns:
        A dictionary containing the configuration specifications for this run.
    """
    return read_json(os.path.join(parent_dir, '_temp/config.json'))


def cache_gbd_db(state, gbd_conn, parent_dir):
    logging.info("Caching GBD DB state to parent_dir/_temp {} changes."
                 .format(state))
    cache_output = os.path.join(parent_dir, '_temp/db_cache.h5')
    pv_table = get_gbd_process_version_cache(GBD.GBD_ROUND_ID, gbd_conn)
    pv_table.to_hdf(cache_output,
                    key='process_version_{}'.format(state),
                    mode='a',
                    format='table',
                    data_columns=['process_version_id'])
    cv_table = get_gbd_compare_version_cache(GBD.GBD_ROUND_ID, gbd_conn)
    cv_table.to_hdf(cache_output,
                    key='compare_version_{}'.format(state),
                    mode='a',
                    format='table',
                    data_columns=['compare_version_id'])


def cache_cod_db(state, cod_conn, parent_dir):
    logging.info("Caching COD DB state to parent_dir/_temp {} changes"
                 .format(state))
    cache_output = os.path.join(parent_dir, '_temp/db_cache.h5')
    old_ov_table = get_output_version_cache(GBD.GBD_ROUND_ID, cod_conn)
    old_ov_table.to_hdf(cache_output,
                        key='output_version_{}'.format(state),
                        mode='a',
                        format='table',
                        data_columns=['output_version_id'])


def gbd_main(parent_dir, db_env):
    config = read_helper_files(parent_dir)
    process_version_id = config['process_version_id']
    gbd_conn = _GBD_CONN_DEF_MAP[db_env.value]

    cache_gbd_db('before', gbd_conn, parent_dir)

    logging.info("Create compare_version_id and add to GBD database")
    # Retrieve the latest compare version from the gbd database
    cv_id = get_latest_compare_version(
        gbd_round_id=GBD.GBD_ROUND_ID,
        conn_def=gbd_conn)
    # Instantiate the CompareVersion object
    current_cv = CompareVersion(compare_version_id=cv_id, env=db_env)
    # Get a list of process versions in the current CompareVersion
    # Exclude gbd_process_id 3 - CoDCorrect (we're adding a new one)
    # and gbd_process_id 14 - SEVs, not included in compare version string.
    process_versions = [pv for pv in current_cv.gbd_process_versions
                        if pv.gbd_process_id not in [3, 14]]
    # Create a tuple of gbd_process_version_id to gbd_process_version_note
    pv_id_note_tuple = [(pv.gbd_process_id, pv.gbd_process_version_note)
                        for pv in process_versions]
    # Format the respective descriptions used to create the new
    # compare version.
    cv_strs = [get_process_desc_from_id(pv_id, note) for pv_id, note
               in pv_id_note_tuple]
    codcorrect_desc = 'CoDCorrect: {}'.format(output_version_id)
    # Additional formatting to match pattern in database
    cv_strs += [codcorrect_desc]
    cv_strs = sorted(cv_strs)
    cv_desc = ', '.join(cv_strs)
    # Create new compare version
    new_cv = CompareVersion.add_new_version(
        gbd_round_id=GBD.GBD_ROUND_ID,
        compare_version_description=cv_desc,
        env=db_env)
    # Create list of process_version_ids
    pvids = [pv.gbd_process_version_id for pv in process_versions]
    pvids += [process_version_id]
    # Add gbd_process_version_ids to compare version
    new_cv.add_process_version(pvids)
    logging.info("Activating new process version")
    activate_new_process_version(process_version_id,
                                 gbd_conn)
    activate_new_compare_version(new_cv.compare_version_id, gbd_conn)

    cache_gbd_db('after', gbd_conn, parent_dir)

    logging.info("Cache saved to _temp. keys: 'process_version_before/after' "
                 "and 'compare_version_before/after'")
    logging.info("GBD Post Scriptum Finished.")


def cod_main(parent_dir, output_version_id, db_env):
    # TODO: Update status of model from 0 (running) to 1 (complete)
    cod_conn = _COD_CONN_DEF_MAP[db_env.value]
    logging.info("Caching old state to {parent_dir}/_temp")
    cache_cod_db('before', cod_conn, parent_dir)

    logging.info("Updating COD DB state")
    update_status(output_version_id, 1, cod_conn)

    cache_cod_db('after', cod_conn, parent_dir)
    logging.info("Cache saved to _temp. keys: 'output_version_before' "
                 "'and output_version_after'")
    logging.info("Cod Post Scriptum Finished.")


if __name__ == '__main__':

    output_version_id, db, test_env = parse_args()

    if test_env:
        db_env = DBEnv.DEV
    else:
        db_env = DBEnv.PROD

    parent_dir = 'FILEPATH'
    log_dir = os.path.join(parent_dir, 'logs')
    cc_log.setup_logging(log_dir,
                         'post_scriptum',
                         time.strftime("%m_%d_%Y_%H"),
                         db)

    if db == 'database':
        gbd_main(parent_dir, db_env)
    else:
        cod_main(parent_dir, output_version_id, db_env)

    logging.info("Done.")
