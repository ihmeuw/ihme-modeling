import os
import sys
import argparse
import logging
import glob
import pandas as pd

from codcorrect.database import new_diagnostic_version, wipe_diagnostics
from codcorrect.database import (upload_diagnostics, upload_cod_summaries,
                                 upload_gbd_summaries)
from codcorrect.database import create_new_output_version_row, update_status
from codcorrect.core import read_json
import codcorrect.log_utilities as l


def parse_args():
    '''
        Parse command line arguments

        Arguments are output_version_id, db, measure_id, conn_def, change

        Returns all 3 arguments as a tuple, in that order
    '''
    parser = argparse.ArgumentParser()
    parser.add_argument("--output_version_id", type=str)
    parser.add_argument("--db", type=str)
    parser.add_argument("--measure_id", type=str)
    parser.add_argument("--conn_def", type=str)
    parser.add_argument("--change", action="store_true")

    args = parser.parse_args()
    output_version_id = args.output_version_id
    db = args.db
    measure_id = args.measure_id
    conn_def = args.conn_def
    change = args.change

    valid_dbs = ['codcorrect', 'cod', 'gbd']
    assert db in valid_dbs, "Invalid db: {}".format(db)

    return output_version_id, db, measure_id, conn_def, change


def read_helper_files(parent_dir):
    ''' Read in and return helper DataFrames.

        Returns:
        config
        list of causes
    '''
    logger = logging.getLogger('correct.read_helper_files')

    # Config file
    logger.info('Reading config file')
    config = read_json(parent_dir + r'FILEPATH.json')
    causes = sorted(
        pd.read_csv(parent_dir + 'FILEPATH.csv').cause_id.unique(), key=int)

    return config, causes


def upload_cod_summary(output_version_id, envelope_version_id, causes, years,
                       cod_conn_def):
    logging.info("Preparing for upload to CoD")
    create_new_output_version_row(output_version_id,
                                  "New version of CodCorrect",
                                  envelope_version_id, cod_conn_def)
    directories = [os.path.join(parent_dir, 'FILEPATH')]
    upload_cod_summaries(directories, cod_conn_def)
    update_status(output_version_id, 1, cod_conn_def)
    logging.info("Finished upload to CoD")


def upload_diagnostic_summary(output_version_id):
    logging.info("Uploading diagnostics")
    wipe_diagnostics()
    new_diagnostic_version(output_version_id)
    upload_diagnostics(parent_dir)
    logging.info("Finished upload to Codcorrect")


def upload_gbd_summary(process_version, measure_id, change, gbd_conn_def):
    logging.info("Preparing for upload to GBD")
    if change:
        directories = sorted(glob.glob(parent_dir +
                                       r'FILEPATH/*'))
    else:
        directories = sorted(glob.glob(parent_dir +
                                       r'FILEPATH/*'))
    upload_gbd_summaries(process_version, gbd_conn_def, directories)
    logging.info("Finished upload to gbd")


if __name__ == '__main__':
    # parse args
    output_version_id, db, measure_id, conn_def, change = parse_args()

    # Set paths
    parent_dir = (r'FILEPATH'
                  .format(v=output_version_id))
    log_dir = parent_dir + r'/logs'

    # Start logging
    l.setup_logging(log_dir, 'upload', output_version_id, db, measure_id,
                    str(change))
    logging.info("conn_def is {}".format(conn_def))

    try:
        config, causes = read_helper_files(parent_dir)
        process_version = config['process_version_id']
        envelope_version_id = config['envelope_version_id']
        years = config['eligible_year_ids']
        if db == 'cod':
            upload_cod_summary(output_version_id, envelope_version_id,
                               causes, years, conn_def)
        elif db == 'gbd':
            upload_gbd_summary(process_version, measure_id, change, conn_def)
        else:
            upload_diagnostic_summary(output_version_id)
        logging.info('All done!')
    except:
        logging.exception('uncaught exception in upload.py: {}'
                          .format(sys.exc_info()[0]))

