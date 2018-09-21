import sys

import pandas as pd
import logging
import argparse
from multiprocessing import Pool
from functools import partial

from codcorrect.core import read_json
from codcorrect.io import read_hdf_draws, change_permission
from codcorrect.error_check import save_diagnostics
import codcorrect.log_utilities as l


"""
    Appends all diagnostics together
"""


def parse_args():
    """ Parse command line arguments """
    parser = argparse.ArgumentParser()
    parser.add_argument("--output_version_id", type=str)

    args = parser.parse_args()
    output_version_id = args.output_version_id

    return output_version_id


def read_helper_files(parent_dir):
    """ Read in and return helper DataFrames.

        Returns:
        DataFrame containing cause hierarchy used for aggregation
    """

    # Config file
    config = read_json(parent_dir + r'FILEPATH.json')

    # Location hierarchy
    location_hierarchy = pd.read_csv(parent_dir +
                                     r'FILEPATH.csv')
    location_ids = location_hierarchy['location_id'].drop_duplicates().tolist()
    estimate_locations = location_hierarchy.ix[
        location_hierarchy['is_estimate'] == 1, 'location_id'].tolist()

    return config, location_ids, estimate_locations


def read_aggregated_unscaled(parent_dir, location_id, year_id):
    unscaled_filepath = (parent_dir + r'FILEPATH.h5'
                         .format(loc=location_id, yr=year_id))
    return read_hdf_draws(unscaled_filepath, location_id)


def read_aggregated_rescaled(parent_dir, location_id, diag_years):
    rescaled_w_shock_filepath = (parent_dir + r'FILEPATH.h5'
                                 .format(loc=location_id))
    return read_hdf_draws(rescaled_w_shock_filepath, location_id,
                          filter_years=diag_years)


def calculate_diagnostics(parent_dir, location_id, diag_years):
    logging.info("Calculating diagnostics for {}".format(location_id))
    pool = Pool(3)
    read_unscaled = partial(read_aggregated_unscaled, parent_dir, location_id)
    unscaled_list = pool.map(read_unscaled, diag_years)
    pool.close()
    pool.join()
    unscaled = pd.concat(unscaled_list)
    rescaled = read_aggregated_rescaled(parent_dir, location_id, diag_years)
    return save_diagnostics(unscaled, rescaled, index_columns,
                            data_columns, location_id, parent_dir, save=False)


if __name__ == '__main__':

    # Get command line arguments
    output_version_id = parse_args()

    # Set paths
    parent_dir = (r'FILEPATH')
    log_dir = parent_dir + r'FILEPATH'

    # Start logging
    l.setup_logging(log_dir, 'append_diagnostics', output_version_id)

    try:
        # Read in helper files
        logging.info("Reading in helper files")
        config, location_ids, est_locations = read_helper_files(parent_dir)

        # Read in config variables
        diag_years = config['diagnostic_year_ids']
        index_columns = config['index_columns']
        data_columns = config['data_columns']

        logging.info('Reading in diagnostic files and creating ones that '
                     'dont exist')
        data = []
        for location_id in location_ids:
            if location_id in est_locations:
                # all most detailed locations already have diagnostics created
                file_path = parent_dir + r'FILEPATH.csv'
                logging.info("Reading in {}".format(file_path))
                df = pd.read_csv(file_path)
                df = df.ix[df['year_id'].isin(diag_years)]
                data.append(df)
            # all aggregated locations need diagnostics created
            else:
                data.append(calculate_diagnostics(parent_dir, location_id,
                                                  diag_years))

        logging.info("Concatenating in diagnostic files")
        diag = pd.concat(data)

        # Format for upload
        diag['output_version_id'] = output_version_id
        diagnostic_table_fields = ['output_version_id', 'location_id',
                                   'year_id', 'sex_id', 'age_group_id',
                                   'cause_id', 'mean_before', 'mean_after']
        diag = diag[diagnostic_table_fields]

        # Save
        logging.info("Saving single diagnostic file")
        file_path = parent_dir + r'FILEPATH.csv'
        diag.to_csv(file_path, index=False)
        change_permission(file_path)

        logging.info('All done!')
    except:
        logging.exception('uncaught exception in append_diagnostics.py: {}'
                          .format(sys.exc_info()[0]))
