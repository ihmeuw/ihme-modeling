import logging
import time

import pandas as pd
import glob
from gbd_outputs_versions import GBDProcessVersion
from gbd_outputs_versions.db import DBEnvironment as DBEnv

import dalynator.get_input_args as get_input_args


# Other programs look for this string.
SUCCESS_LOG_MESSAGE = "DONE write DF"
logger = logging.getLogger(__name__)

def import_data_list_from_file_pattern(file_pattern):

    data = []
    logger.info("Search for files in pattern {}".format(file_pattern))
    for f in glob.glob(file_pattern):
        logger.info("Appending {}".format(f))
        data.append(pd.read_csv(f))
    return data


def concatentate_summary_data(summary_data, table_type):
    logger.info("Concatenating summary data, time = {}".format(time.time()))
    summary_data = pd.concat(summary_data)
    if table_type == "single_year":
        keep_columns = ['location_id', 'year_id', 'sex_id', 'age_group_id',
                        'cause_id', 'measure_id', 'metric_id', 'mean', 'lower',
                        'upper']
    elif table_type == "multi_year":
        keep_columns = ['location_id', 'year_start_id', 'year_end_id', 'sex_id',
                        'age_group_id', 'cause_id', 'measure_id', 'metric_id',
                        'mean', 'lower', 'upper']
    summary_data = summary_data[keep_columns].rename(columns={'mean': 'val'})
    logger.info("End concatenating summary data, time = {}".format(time.time()))
    return summary_data


def save_for_upload(process_version, out_dir, summary_data):
    logger.info("Save data for upload, time = {}".format(time.time()))
    process_version.save_for_upload(summary_data, "{}/upload".format(out_dir))
    logger.info("End saving data for upload, time = {}".format(time.time()))


def concatenate_and_upload(input_data, table_type, process_version, out_dir):
    summary_data = concatentate_summary_data(input_data, table_type)
    save_for_upload(process_version, out_dir, summary_data)


def run_dalynator_upload(out_dir, gbd_process_version_id, location_ids,
                         measure_id, table_type, upload_to_test):
    """
    Upload summary files to GBD Outputs summary tables

    Args:
        out_dir (str): the root directory for this dalynator run
        gbd_process_version_id (int): GBD Process Version to upload to
        location_ids (int): location ids used in this dalynator run
        measure_id (int): measure_id of the upload
        table_type (str): type of DB table to upload into (single or multi year)
        upload_to_test (bool): determines whether to upload to the test db
    """

    start_time = time.time()
    logger.info("START pipeline dalynator upload at {}".format(start_time))

    # Get process version
    if upload_to_test:
        upload_env = DBEnv.DEV
    else:
        upload_env = DBEnv.PROD
    pv = GBDProcessVersion(gbd_process_version_id, env=upload_env)

    # Read in all summary files and save
    summary_data = []
    total_rows = 0
    for location_id in location_ids:
        file_pattern = "FILEPATH.csv".format(
            o=out_dir, l=location_id, m=measure_id, tt=table_type)
        for d in import_data_list_from_file_pattern(file_pattern):
            total_rows += len(d)
            summary_data.append(d)
        if total_rows > 20000000:
            # Concatenate results and save for upload
            concatenate_and_upload(
                summary_data, table_type, pv, out_dir)
            # Reset summary data and row count
            summary_data = []
            total_rows = 0
    if total_rows > 0:
        # Concatenate results and save for upload
        concatenate_and_upload(
            summary_data, table_type, pv, out_dir)
        # Reset summary data and row count
        summary_data = []
        total_rows = 0

    # Upload data
    table_name = "output_summary_{}_v{}".format(
        table_type, pv.gbd_process_version_id)
    logger.info("Uploading data, time = {}".format(time.time()))
    pv.upload_to_table(table_name)
    logger.info("End uploading data, time = {}".format(time.time()))

    # End log
    end_time = time.time()
    elapsed = end_time - start_time
    logger.info("DONE cleanup pipeline at {}, elapsed seconds= {}".format(
        end_time, elapsed))
    logger.info("{}".format(SUCCESS_LOG_MESSAGE))


def main():
    parser = get_input_args.construct_parser_dalynator_upload()
    args = get_input_args.construct_args_dalynator_upload(parser)

    run_dalynator_upload(args.out_dir, args.gbd_process_version_id,
                         args.location_ids, args.measure_id, args.table_type,
                         args.upload_to_test)


if __name__ == "__main__":
    main()
