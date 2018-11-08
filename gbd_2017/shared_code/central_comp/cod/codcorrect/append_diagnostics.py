import sys
import os
import pandas as pd
import logging
import argparse

from draw_sources.draw_sources import DrawSource
from db_queries import get_location_metadata

from codcorrect.core import read_json
from codcorrect.io import read_hdf_draws, change_permission
from codcorrect.error_check import save_diagnostics
import codcorrect.log_utilities as cc_log_utils


"""
    Appends all diagnostics together that already exist from the most-detailed
    location level, and creates diagnostics for location aggregate level.
    All diagnostics only exist for deaths, not YLLs
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
    config = read_json(os.path.join(parent_dir, '_temp/config.json'))

    # Location hierarchy
    location_hierarchy = get_location_metadata(gbd_round_id=5,
                                               location_set_id=35)
    location_hierarchy = location_hierarchy[
        ['location_id', 'parent_id', 'level', 'is_estimate', 'most_detailed',
         'sort_order']]
    location_ids = location_hierarchy['location_id'].drop_duplicates().tolist()
    estimate_locations = location_hierarchy.loc[
        location_hierarchy['is_estimate'] == 1, 'location_id'].tolist()

    return config, location_ids, estimate_locations


def read_aggregated_unscaled(parent_dir, location_id, diag_years):
    """ Read in location aggregates of unscaled draws for deaths only"""
    unscaled_params = {
        'draw_dir': os.path.join(parent_dir, 'aggregated/unscaled'),
        'file_pattern': '{measure_id}_{location_id}_{year_id}.h5'
    }
    ds = DrawSource(unscaled_params)
    unscaled_draws = ds.content(filters={'location_id': location_id,
                                         'year_id': diag_years,
                                         'measure_id': 1})
    return unscaled_draws


def read_aggregated_rescaled(parent_dir, location_id, diag_years):
    """ Read in location aggregates of rescaled draws for deaths only"""
    rescaled_params = {
        'draw_dir': os.path.join(parent_dir, 'draws'),
        'file_pattern': '{measure_id}_{location_id}.h5'
    }
    ds = DrawSource(rescaled_params)
    rescaled_draws = ds.content(filters={'location_id': location_id,
                                         'year_id': diag_years,
                                         'measure_id': 1})
    return rescaled_draws


def calculate_diagnostics(parent_dir, location_id, diag_years):
    """ Calculates diagnostics, as the difference between scaled and unscaled
    and formats it for the codcorrect.diagnostics table schema"""
    logging.info("Calculating diagnostics for {}".format(location_id))
    unscaled = read_aggregated_unscaled(parent_dir, location_id, diag_years)
    rescaled = read_aggregated_rescaled(parent_dir, location_id, diag_years)
    return save_diagnostics(unscaled, rescaled, index_columns,
                            data_columns, location_id, parent_dir, save=False)


if __name__ == '__main__':

    # Get command line arguments
    output_version_id = parse_args()

    # Set paths
    parent_dir = 'FILEPATH'
    log_dir = os.path.join(parent_dir, 'logs')

    # Start logging
    cc_log_utils.setup_logging(log_dir, 'append_diagnostics',
                               output_version_id)

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
                file_path = (parent_dir + r'FILEPATH'
                             .format(loc=location_id))
                logging.info("Reading in {}".format(file_path))
                df = pd.read_csv(file_path)
                df = df.loc[df['year_id'].isin(diag_years)]
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
        file_path = parent_dir + r'FILEPATH'
        diag.to_csv(file_path, index=False)
        change_permission(file_path)

        logging.info('All done!')
    except Exception:
        logging.exception('uncaught exception in append_diagnostics.py: {}'
                          .format(sys.exc_info()[0]))
        sys.exit(1)
