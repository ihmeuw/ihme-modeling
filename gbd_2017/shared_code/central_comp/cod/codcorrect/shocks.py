import sys
import os
import pandas as pd
import argparse
import logging

from codcorrect.core import read_json
from codcorrect.io import import_cod_model_draws
from codcorrect.error_check import check_data_format
import codcorrect.log_utilities as cc_log_utils


"""
    This script does the following:
      -Reads in best CoD shock draws
      -Saves file for Mortality team
      -Saves HDF file

"""


def parse_args():
    """Parse command line arguments

    Arguments are output_version_id, location_id, and sex_id

        Returns all 3 arguments as a tuple, in that order.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--output_version_id", type=str)
    parser.add_argument("--location_id", type=str)
    parser.add_argument("--sex_id", type=str)

    args = parser.parse_args()
    output_version_id = args.output_version_id
    location_id = args.location_id
    sex_id = args.sex_id

    return output_version_id, location_id, sex_id


def read_helper_files(parent_dir, location_id, sex_id):
    """Read in and return helper DataFrames.

        Returns:
        best_models: DataFrame containing all best model ids and relevant cause
            metadata for a given sex
        eligible_data: a DataFrame containing all demographics and their
            restriction status
    """
    logger = logging.getLogger('shocks.read_helper_files')

    # Config file
    logger.info('Reading config file')
    config = read_json(os.path.join(parent_dir, '_temp/config.json'))

    # List of best models for shocks/imported_cases/hiv
    logger.info('Reading best models')
    best_models = pd.read_csv(parent_dir + r'/_temp/best_models.csv')
    best_models = best_models.loc[(best_models['sex_id'] == int(sex_id)) &
                                  (best_models['model_version_type_id']
                                  .isin(list(range(5, 8))))]

    return config, best_models


def read_all_model_draws(best_models, required_columns, filter_years=None):
    """Reads in all shock/hiv/imported_cases models for a specific sex and
    location_id. Also logs which models it couldn't open

        Returns: a DataFrame with the draws
    """
    # Read in best models
    data = []
    for i in best_models.index:
        model_version_id = int(best_models.loc[i, 'model_version_id'])
        cause_id = best_models.loc[i, 'cause_id']
        temp_data = import_cod_model_draws(model_version_id, location_id,
                                           cause_id, sex_id,
                                           required_columns, filter_years)
        data.append(temp_data)

    data = pd.concat(data)

    # DataFrame shouldn't be empty
    logger = logging.getLogger('correct.read_all_model_draws')
    try:
        assert not data.empty, 'No best model data found'
    except AssertionError as e:
        logger.exception('No best model data found')
        sys.exit(1)

    return data


def save_draws(data, index_columns):
    """Saves draws wide in an h5 file.

       Returns: None
    """
    logger = logging.getLogger('shocks.save_draws')

    # Save draws
    data = data.sort_values(index_columns).reset_index(drop=True)
    data.to_hdf(
        parent_dir +
        r'FILEPATH.h5'.format(
            location_id=location_id,
            sex_id=sex_id),
        'draws', mode='w', format='table',
        data_columns=index_columns)


if __name__ == '__main__':

    # Get command line arguments
    output_version_id, location_id, sex_id = parse_args()

    # Set paths
    parent_dir = 'FILEPATH'
    log_dir = os.path.join(parent_dir, 'logs')

    # Start logging
    cc_log_utils.setup_logging(log_dir, 'shocks', output_version_id,
                               location_id, sex_id)

    try:
        # Read in helper files
        logging.info("Reading in helper files")
        config, best_models = read_helper_files(parent_dir, location_id,
                                                sex_id)

        # Read in config variables
        eligible_year_ids = config['eligible_year_ids']
        index_columns = config['index_columns']
        data_columns = config['data_columns']
        raw_data_columns = index_columns + data_columns

        # Read in draw files
        logging.info("Reading in best model draws")
        raw_data = read_all_model_draws(best_models, raw_data_columns,
                                        filter_years=eligible_year_ids)

        # Check formatting
        logging.info("Checking in best model draws")
        check_data_format(raw_data, raw_data_columns, fail=True)

        # Saving data
        logging.info("Saving data")
        save_draws(raw_data, index_columns)
        logging.info('All done!')
    except Exception:
        logging.exception('uncaught exception in shocks.py: {}'
                          .format(sys.exc_info()[0]))
        sys.exit(1)
