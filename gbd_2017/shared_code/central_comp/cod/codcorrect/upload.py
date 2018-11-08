import os
import sys
import argparse
import logging
import glob
import pandas as pd

from codcorrect.database import (new_diagnostic_version, wipe_diagnostics,
                                 upload_diagnostics, upload_cod_summaries,
                                 upload_gbd_summaries, update_status,
                                 create_new_output_version_row)
from codcorrect.core import read_json
import codcorrect.log_utilities as cc_log_utils


def parse_args():
    """Parse command line arguments.

    Arguments:
        output_version_id, db, measure_id, conn_def, change

    Returns:
        arguments
    """
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
    """Read in and return helper DataFrames.

    Returns:
        config
        list of causes
    """
    logger = logging.getLogger('correct.read_helper_files')

    # Config file
    logger.info('Reading config file')
    config = read_json(os.path.join(parent_dir, '_temp/config.json'))
    causes = sorted(pd.read_csv(
        os.path.join(parent_dir, '_temp/cause_aggregation_hierarchy.csv')
                    ).cause_id.unique(), key=int)

    return config, causes


def upload_cod_summary(output_version_id, envelope_version_id, causes, years,
                       cod_conn_def):
    """Execute all steps needed to upload to the CoD db, including creating
    a new output_version_id, uploading, and updating status."""
    logging.info("Preparing for upload to CoD")
    create_new_output_version_row(output_version_id,
                                  "New version of CodCorrect",
                                  envelope_version_id, cod_conn_def)
    directories = [os.path.join(parent_dir, 'summaries/cod')]
    upload_cod_summaries(directories, cod_conn_def)
    update_status(output_version_id, 1, cod_conn_def)
    logging.info("Finished upload to CoD")


def upload_diagnostic_summary(output_version_id):
    """Execute all steps needed to upload to the Codcorrect db, including
    unmarking old versions, creating a new diagnostic version, and uploading
    """
    logging.info("Uploading diagnostics")
    # change_permission(parent_dir)
    # change_permission(parent_dir + r'/_temp/', recursively=True)
    wipe_diagnostics()
    new_diagnostic_version(output_version_id)
    upload_diagnostics(parent_dir)
    logging.info("Finished upload to Codcorrect")


def upload_gbd_summary(process_version, measure_id, change, gbd_conn_def):
    """Execute all steps needed to upload to the gbd db, including creating
    a new output_version_id, uploading, and updating status"""
    logging.info("Preparing for upload to GBD")
    if change:
        directories = sorted(glob.glob(
            os.path.join(parent_dir,
                         'summaries/gbd/multi/{m}/*'.format(m=measure_id))))
    else:
        directories = sorted(glob.glob(
            os.path.join(parent_dir,
                         'summaries/gbd/single/{m}/*'.format(m=measure_id))))
    upload_gbd_summaries(process_version, gbd_conn_def, directories)
    logging.info("Finished upload to gbd")


if __name__ == '__main__':
    # parse args
    output_version_id, db, measure_id, conn_def, change = parse_args()

    # Set paths
    parent_dir = 'FILEPATH'
    log_dir = os.path.join(parent_dir, 'logs')

    # Start logging
    cc_log_utils.setup_logging(log_dir, 'upload', output_version_id, db,
                               measure_id, str(change))
    logging.info("conn_def is {}".format(conn_def))

    try:
        config, causes = read_helper_files(parent_dir)
        envelope_version_id = config['envelope_version_id']
        years = config['eligible_year_ids']
        if db == 'cod':
            upload_cod_summary(output_version_id, envelope_version_id,
                               causes, years, conn_def)
        elif db == 'gbd':
            process_version = config['process_version_id']
            upload_gbd_summary(process_version, measure_id, change, conn_def)
        else:
            upload_diagnostic_summary(output_version_id)
        logging.info('All done!')
    except Exception:
        logging.exception('uncaught exception in upload.py: {}'
                          .format(sys.exc_info()[0]))
        sys.exit(1)
