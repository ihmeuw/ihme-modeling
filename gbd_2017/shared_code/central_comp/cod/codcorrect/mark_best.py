"""
mark_best.py -- This script is meant to separate the process of marking a
CoDCorrect best from the CoDCorrect dag.

Often we do not know whether a run will be marked best until after the dag has
completed and post_scriptum_upload.py script has finished running.


"""

import argparse
import logging
import os
import time

from gbd_outputs_versions.db import DBEnvironment as DBEnv
import gbd.constants as GBD

from codcorrect.core import read_json
from codcorrect.database import (unmark_cod_best, mark_cod_best,
                                 unmark_gbd_best, mark_gbd_best,
                                 get_output_version_cache,
                                 get_gbd_compare_version_cache,
                                 get_gbd_process_version_cache)
import codcorrect.log_utilities as cc_log


_GBD_CONN_DEF_MAP = {'prod': 'gbd', 'dev': 'gbd-test'}
_COD_CONN_DEF_MAP = {'prod': 'cod', 'dev': 'cod-test'}


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--output_version_id', type=int)
    parser.add_argument('--test', action='store_true')

    args = parser.parse_args()

    return args.output_version_id, args.test


def cache_gbd_db(state, gbd_conn, parent_dir):
    logging.info("Caching GBD DB state to parent_dir/_temp {} changes."
                 .format(state))
    cache_output = os.path.join(parent_dir, '_temp/db_cache.h5')
    pv_table = get_gbd_process_version_cache(GBD.GBD_ROUND_ID, gbd_conn)
    pv_table.to_hdf(cache_output,
                    key='best_process_version_{}'.format(state),
                    mode='a',
                    format='table',
                    data_columns=['process_version_id'])
    cv_table = get_gbd_compare_version_cache(GBD.GBD_ROUND_ID, gbd_conn)
    cv_table.to_hdf(cache_output,
                    key='best_compare_version_{}'.format(state),
                    mode='a',
                    format='table',
                    data_columns=['compare_version_id'])


def cache_cod_db(state, cod_conn, parent_dir):
    logging.info("Caching COD DB state to parent_dir/_temp {} changes"
                 .format(state))
    cache_output = os.path.join(parent_dir, '_temp/db_cache.h5')
    old_ov_table = get_output_version_cache(GBD.GBD_ROUND_ID, cod_conn)
    old_ov_table.to_hdf(cache_output,
                        key='best_output_version_{}'.format(state),
                        mode='a',
                        format='table',
                        data_columns=['output_version_id'])


def read_helper_files(parent_dir):
    return read_json(os.path.join(parent_dir, '_temp/config.json'))


if __name__ == '__main__':
    output_version_id, test = parse_args()

    parent_dir = 'FILEPATH'
    log_dir = os.path.join(parent_dir, 'logs')
    cc_log.setup_logging(log_dir, 'mark_best', time.strftime("%m_%d_%Y_%H"))

    config = read_helper_files(parent_dir)

    process_version_id = config['process_version_id']

    if test:
        db_env = DBEnv.DEV
    else:
        db_env = DBEnv.PROD

    gbd_conn_def = _GBD_CONN_DEF_MAP[db_env.value]
    cod_conn_def = _COD_CONN_DEF_MAP[db_env.value]

    cache_cod_db('before', cod_conn_def, parent_dir)
    cache_gbd_db('before', gbd_conn_def, parent_dir)

    logging.info("Unmark current gbd process version best.")
    unmark_gbd_best(process_version_id, GBD.GBD_ROUND_ID, gbd_conn_def)
    logging.info("Mark new best process version.")
    mark_gbd_best(process_version_id, GBD.GBD_ROUND_ID, gbd_conn_def)

    logging.info("Unmarking old best cod model.")
    unmark_cod_best(output_version_id, GBD.GBD_ROUND_ID, cod_conn_def)
    logging.info("Marking new output_version_id as best model.")
    mark_cod_best(output_version_id, GBD.GBD_ROUND_ID, cod_conn_def)

    cache_cod_db('after', cod_conn_def, parent_dir)
    cache_gbd_db('after', gbd_conn_def, parent_dir)

    logging.info("Fin.")




