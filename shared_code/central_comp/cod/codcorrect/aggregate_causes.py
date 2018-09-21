import sys

import argparse
import pandas as pd
import logging

from codcorrect.core import read_json
from codcorrect.io import read_hdf_draws
from codcorrect.io import save_hdf
from codcorrect.error_check import save_diagnostics
import codcorrect.log_utilities as l


"""
    This script aggregates up the cause hierarchy
"""


def parse_args():
    '''
        Parse command line arguments

        Arguments are output_version_id, location_id, and sex_id

        Returns all 3 arguments as a tuple, in that order
    '''
    parser = argparse.ArgumentParser()
    parser.add_argument("--output_version_id", type=str)
    parser.add_argument("--location_id", type=str)

    args = parser.parse_args()
    output_version_id = args.output_version_id
    location_id = args.location_id

    return output_version_id, location_id


def read_helper_files(parent_dir, location_id):
    """ Read in and return helper DataFrames.

        Returns:
        DataFrame containing cause hierarchy used for aggregation
    """

    # Config file
    config = read_json(parent_dir + r'FILEPATH.json')

    # Cause hierarchy
    cause_hierarchy = pd.read_csv(
        parent_dir + r'FILEPATH.csv')

    return config, cause_hierarchy


def read_rescaled_draw_files(parent_dir, location_id):
    """ Reads in rescaled draw files """
    data = []
    for sex_id in [1, 2]:
        draw_filepath = (
            parent_dir +
            r'FILEPATH.h5'
            .format(location_id=location_id, sex_id=sex_id))
        data.append(read_hdf_draws(draw_filepath, location_id))
    data = pd.concat(data).reset_index(drop=True)
    return data


def read_unscaled_draw_files(parent_dir, location_id, index_columns,
                             draw_columns):
    """ Reads in unscaled draw files """
    data = []
    for sex_id in [1, 2]:
        draw_filepath = (
            parent_dir +
            r'FILEPATH.h5'
            .format(location_id=location_id, sex_id=sex_id))
        data.append(read_hdf_draws(draw_filepath, location_id))
    data = pd.concat(data).reset_index(drop=True)
    data = data[index_columns + data_columns]
    data = data.sort(index_columns).reset_index(drop=True)
    return data


def read_shock_draw_files(parent_dir, location_id):
    """ Reads in shock draw files """
    data = []
    for sex_id in [1, 2]:
        draw_filepath = (
            parent_dir +
            r'FILEPATH.h5'
            .format(location_id=location_id, sex_id=sex_id))
        data.append(read_hdf_draws(draw_filepath, location_id))
    data = pd.concat(data).reset_index(drop=True)
    return data


def aggregate_causes(data, index_columns, data_columns, cause_hierarchy):
    """ Aggregate causes up the cause hierarchy """
    logger = logging.getLogger('aggregate_causes.aggregate_causes')
    try:

        # Merge on cause hierarchy
        cause_hierarchy['level'] = cause_hierarchy['level'].astype('int64')
        min_level = cause_hierarchy['level'].min()
        data = data[index_columns + data_columns]
        data = pd.merge(data,
                        cause_hierarchy[['cause_id',
                                         'level',
                                         'parent_id',
                                         'most_detailed']
                                        ],
                        on='cause_id',
                        how='left')
        # Filter down to the most detailed causes
        data = data.ix[data['most_detailed'] == 1]
        max_level = data['level'].max()
        # Loop through and aggregate
        data = data[index_columns + data_columns]
        for level in xrange(max_level, min_level, -1):
            logging.info("Agg causes level: {}".format(level))
            data = pd.merge(data,
                            cause_hierarchy[['cause_id',
                                             'level',
                                             'parent_id']
                                            ],
                            on='cause_id',
                            how='left')
            temp = data.ix[data['level'] == level].copy(deep=True)
            temp['cause_id'] = temp['parent_id']
            temp = temp[index_columns + data_columns]
            temp = temp.groupby(index_columns).sum().reset_index()
            data = pd.concat([data[index_columns + data_columns], temp]
                             ).reset_index(drop=True)

    except Exception as e:
        logger.exception('Failed to aggregate causes: {}'.format(e))
        sys.exit()

    return data


def aggregate_blanks(data, index_columns, data_columns, cause_hierarchy,
                     full_index_set):
    """ This function is to fill in gaps and to preserve existing data """
    logger = logging.getLogger('aggregate_causes.aggregate_blanks')
    # Merge on cause hierarchy
    data = pd.merge(data,
                    full_index_set,
                    on=index_columns,
                    how='outer')
    data = pd.merge(data,
                    cause_hierarchy[['cause_id',
                                     'level',
                                     'parent_id']],
                    on='cause_id',
                    how='left')

    # Get min and max level where we need to aggregate
    min_level = data.ix[data[data_columns[0]].isnull(), 'level'].min() - 1
    max_level = data.ix[data[data_columns[0]].isnull(), 'level'].max()

    # Loop through and aggregate things that are missing
    for level in xrange(max_level, min_level, -1):
        logging.info("Agg blanks level: {}".format(level))
        # Wipe then merge cause hierarchy onto data
        data = data[index_columns + data_columns]
        data = pd.merge(data,
                        cause_hierarchy[['cause_id', 'level', 'parent_id']],
                        on='cause_id')
        # Get data that needs to get aggregated
        temp = data.ix[(data[data_columns[0]].isnull()) &
                       (data['level'] == level),
                       index_columns].copy(deep=True)
        temp = temp.rename(columns={'cause_id': 'parent_id'})
        temp = pd.merge(temp,
                        data,
                        on=list(set(index_columns) - set(['cause_id'])) +
                        ['parent_id'])
        # Collapse to parent
        temp['cause_id'] = temp['parent_id']
        temp = temp.groupby(index_columns)[data_columns].sum().reset_index()
        # Merge back onto original data
        data = pd.concat([data.ix[((data[data_columns[0]].notnull()) &
                                   (data['level'] == level)) |
                                  (data['level'] != level)],
                          temp])

    data = data[index_columns + data_columns]
    return data


def save_all_draws(parent_dir, index_columns, rescaled_data, shock_data,
                   unscaled_data, location):
    # Save rescaled data
    draw_filepath = (parent_dir +
                     r'FILEPATH.h5'.format(loc=location))
    save_hdf(rescaled_data, draw_filepath, key='draws', mode='w',
             format='table', data_columns=index_columns)

    # Save unscaled data
    draw_filepath = (parent_dir +
                     r'FILEPATH.h5'.format(loc=location))
    save_hdf(unscaled_data, draw_filepath, key='draws', mode='w',
             format='table', data_columns=index_columns)

    # Save shocks
    draw_filepath = (parent_dir +
                     r'FILEPATH.h5'.format(loc=location))
    save_hdf(shock_data, draw_filepath, key='draws', mode='w', format='table',
             data_columns=index_columns)


if __name__ == '__main__':

    # Get command line arguments
    output_version_id, location = parse_args()

    # Set paths
    parent_dir = r'FILEPATH'
    log_dir = parent_dir + r'FILEPATH'

    # Start logging
    l.setup_logging(log_dir, 'agg_cause', output_version_id, location)

    try:
        # Read in helper files
        logging.info("Reading in helper files")
        config, cause_hierarchy = read_helper_files(
            parent_dir, location)

        # Read in config variables
        index_columns = config['index_columns']
        data_columns = config['data_columns']

        # Read in rescaled draw files
        logging.info("Reading in rescaled draw files")
        rescaled_data = read_rescaled_draw_files(parent_dir, location)

        # Read in unscaled draw files
        logging.info("Reading in unscaled draw files")
        unscaled_data = read_unscaled_draw_files(parent_dir, location,
                                                 index_columns, data_columns)

        # Read in shock draw files
        logging.info("Reading in shock draw files")
        shocks_data = read_shock_draw_files(parent_dir, location)
        hiv_data = shocks_data.ix[shocks_data['cause_id'].isin([299, 300])
                                  ].copy(deep=True)
        shocks_data = shocks_data.ix[~shocks_data['cause_id'].isin([299, 300])]

        # Aggregate causes
        logging.info("Aggregating causes - rescaled")
        rescaled_data = pd.concat([rescaled_data, hiv_data]).reset_index(
            drop=True)
        rescaled_data = (rescaled_data[index_columns + data_columns]
                         .groupby(index_columns).sum().reset_index())
        aggregated_rescaled_data = aggregate_causes(rescaled_data,
                                                    index_columns,
                                                    data_columns,
                                                    cause_hierarchy)

        logging.info("Aggregating causes - unscaled")
        full_index_set = aggregated_rescaled_data.ix[:, index_columns
                                                     ].copy(deep=True)
        full_index_set = full_index_set.drop_duplicates()
        aggregated_unscaled_data = aggregate_blanks(unscaled_data,
                                                    index_columns,
                                                    data_columns,
                                                    cause_hierarchy,
                                                    full_index_set)

        logging.info("Aggregating causes - shocks")
        aggregated_shocks = aggregate_causes(shocks_data, index_columns,
                                             data_columns, cause_hierarchy)

        # Save
        logging.info("Save draws")
        save_all_draws(parent_dir, index_columns, aggregated_rescaled_data,
                       aggregated_shocks, aggregated_unscaled_data, location)

        # Saving diagnostics
        logging.info("Saving diagnostics")
        aggregated_all_data = pd.concat([aggregated_rescaled_data,
                                         aggregated_shocks]
                                        ).reset_index(drop=True)
        aggregated_all_data = aggregated_all_data[
            index_columns + data_columns].groupby(
            index_columns).sum().reset_index()
        save_diagnostics(aggregated_unscaled_data, aggregated_all_data,
                         index_columns, data_columns, location, parent_dir)

        logging.info('All done!')
    except:
        logging.exception('uncaught exception in aggregate_causes.py: {}'
                          .format(sys.exc_info()[0]))
