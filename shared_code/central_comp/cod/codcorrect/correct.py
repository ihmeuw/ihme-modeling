import sys

import pandas as pd
import logging
import argparse

from codcorrect.core import Envelope, read_json
from codcorrect.io import (import_cod_model_draws, read_envelope_draws,
                           save_hdf)
from codcorrect.error_check import tag_zeros, check_data_format
from codcorrect.error_check import missing_check, exclusivity_check
from codcorrect.restrictions import expand_id_set
import codcorrect.log_utilities as l


"""
    This script does the following:
      -Reads in best model CoD draws
      -Converts to cause fraction space
      -Rescales so cause fractions add up to 1 for
       a given level-parent_id group
      -Adjusts cause fractions based on the
       parent_id
      -Multiply cause fractions by latest version
       of the envelope to get corrected death
       numbers
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
    parser.add_argument("--sex_id", type=str)

    args = parser.parse_args()
    output_version_id = args.output_version_id
    location_id = args.location_id
    sex_id = args.sex_id

    return output_version_id, location_id, sex_id


def read_helper_files(parent_dir, location_id, sex_id):
    ''' Read in and return helper DataFrames.

        Returns:
        best_models: DataFrame containing all best model ids
                     and relevant cause metadata for a given sex
        eligible_data: a DataFrame containing all demographics
                       and their restriction status
    '''
    logger = logging.getLogger('correct.read_helper_files')

    # Config file
    logger.info('Reading config file')
    config = read_json(parent_dir + r'FILEPATH.json')

    # List of best models (excluding shocks)
    logger.info('Reading best models')
    best_models = pd.read_csv(parent_dir + r'FILEPATH.csv')
    best_models = best_models.ix[(best_models['sex_id'] == int(sex_id)) &
                                 (best_models['model_version_type_id']
                                  .isin(range(0, 5) + [10]))]

    # List of eligible data
    logger.info('Reading eligible models')
    eligible_data = pd.read_csv(parent_dir + r'FILEPATH.csv')

    # Space-time restrictions
    spacetime_restriction_data = pd.read_csv(
        parent_dir + 'FILEPATH.csv')

    # Envelope
    logger.info('Reading envelope draws')
    envelope_data = read_envelope_draws(parent_dir + r'FILEPATH.h5',
                                        location_id)
    rename_columns = {}
    for x in xrange(1000):
        rename_columns['env_{}'.format(x)] = 'draw_{}'.format(x)
    envelope_data = envelope_data.rename(columns=rename_columns)

    return (config, best_models, eligible_data, spacetime_restriction_data,
            envelope_data)


def read_all_model_draws(best_models, required_columns, filter_years=None):
    """
        Reads in all CODEm models for a specific
        sex and location_id

        Also logs which models it couldn't open

        returns:
        a DataFrame with the CODEm draws
    """
    # Read in best models
    data = []
    for i in best_models.index:
        model_version_id = int(best_models.ix[i, 'model_version_id'])
        cause_id = best_models.ix[i, 'cause_id']
        temp_data = import_cod_model_draws(model_version_id, location_id,
                                           cause_id, sex_id,
                                           required_columns,
                                           filter_years=filter_years)
        data.append(temp_data)
    data = pd.concat(data)

    # DataFrame shouldn't be empty
    logger = logging.getLogger('correct.read_all_model_draws')
    try:
        assert not data.empty, 'No best model data found'
    except AssertionError as e:
        logger.exception('No best model data found')
        sys.exit()

    return data


def filter_zeros(data, data_columns):
    data = tag_zeros(data, data_columns, tag_column='zeros')
    nozeroes = data.ix[data['zeros'] == False].copy(deep=True)
    zeroes = data.ix[data['zeros'] == True].copy(deep=True)
    return nozeroes, zeroes


def restrict_and_check(data, eligible_data, index_columns, data_columns,
                       save_missing_filepath=None, save_overlap_filepath=None):
    """ Restricts data and the does checks of missing and overlapping data

    Restricts data down to eligible demographics, and then checks for missing
    data and checks for overlapping data

    Returns: Restricted DataFrame
    """
    logger = logging.getLogger('correct.restrict_and_check')
    try:
        for c in index_columns:
            eligible_data.ix[:, c] = eligible_data.ix[:, c].astype('int64')
            data.ix[:, c] = data.ix[:, c].astype('int64')

        # Merge with eligible data
        data = pd.merge(eligible_data[index_columns + ['restricted']],
                        data,
                        on=index_columns,
                        how='left')
        data.ix[:, data_columns] = data.ix[:, data_columns].fillna(0)
        # Restrict
        data = data.ix[data['restricted'] == False].reset_index(drop=True)
        # Check missingness
        missing_check(data, data_columns,
                      save_missing_filepath=save_missing_filepath)
        # Check exclusivity
        exclusivity_check(data, index_columns, fail=True,
                          save_overlap_filepath=save_overlap_filepath)
    except Exception as e:
        logger.exception('Failed to restrict and check data: {}'.format(e))
        sys.exit()
    return data


def format_for_rescale(data, eligible_data, index_columns, data_columns,
                       envelope_column, debug_folder, prefix):
    """ Runs all steps to take the raw data and prepare it to be rescaled """
    logger = logging.getLogger('correct.format_for_rescale')
    try:
        # Restrict
        data = restrict_and_check(data, eligible_data, index_columns,
                                  data_columns, save_overlap_filepath=(
                                      debug_folder + 'FILEPATH' +
                                      prefix + '.csv'))
        # Get a copy of the model data
        model_data = data.ix[:, index_columns + ['model_version_id']].copy(
            deep=True)
        # Keep just the variables we need
        data = data.ix[:, index_columns + [envelope_column] + data_columns]
        # Convert to cause fractions
        for data_column in data_columns:
            data.eval('{d} = {d}/{e}'.format(d=data_column, e=envelope_column),
                      inplace=True)
            data[data_column] = data[data_column].fillna(0)
        # Merge on hierarchy variables
        data = pd.merge(data,
                        eligible_data[index_columns + ['level', 'parent_id']],
                        on=index_columns, how='left')
        data = data.ix[:, index_columns + ['level', 'parent_id'] +
                       data_columns]
    except Exception as e:
        logger.exception('Failed to format data for rescale: {}'.format(e))
        sys.exit()

    return data, model_data


def rescale_group(data, groupby_columns, data_columns):
    """ Rescale column to 1

    Takes a set of columns and rescales to 1 in groups defined by the groupby
    colunns.
    NOTE: the intermediate total column CANNOT have a value of 0 or else
          this can cause problems aggregating up the hierarchy later.  If
          this happens, make the all values within that group equal and
          resume.
    """
    # Make totals
    temp = data[groupby_columns + data_columns].copy(deep=True)
    temp = temp.groupby(groupby_columns)[data_columns].sum().reset_index()
    rename_columns = {'{}'.format(d): '{}_total'.format(d)
                      for d in data_columns}
    temp = temp.rename(columns=rename_columns)
    # Attempt to rescale
    data = pd.merge(data, temp, on=groupby_columns)
    for data_column in data_columns:
        data.eval('{d} = {d}/{d}_total'.format(d=data_column), inplace=True)
    data = data.drop(['{}_total'.format(d) for d in data_columns], axis=1)
    # Fill in problem cells
    data[data_columns] = data[data_columns].fillna(1)
    # Remake totals
    temp = data[groupby_columns + data_columns].copy(deep=True)
    temp = temp.groupby(groupby_columns)[data_columns].sum().reset_index()
    temp = temp.rename(columns=rename_columns)
    # Rescale
    data = pd.merge(data, temp, on=groupby_columns)
    for data_column in data_columns:
        data.eval('{d} = {d}/{d}_total'.format(d=data_column), inplace=True)
    data = data.drop(['{}_total'.format(d) for d in data_columns], axis=1)
    return data


def rescale_to_parent(data, index_columns, data_columns, cause_column,
                      parent_cause_column, level_column, level):
    """ Rescales child data to be internally consistent with parent data

    This function is called once for every level of the hierarchy.

    It subsets out all data for that level, & merges on the parent cause data.
    Then it overwrites the child adjusted values with the product of the parent
    and child adjusted values so that the data is internally consistent.

    Data MUST BE IN CAUSE FRACTION space in order for this to work.

    Returns: Entire DataFrame with adjusted child data
    """
    parent_keep_columns = list(set(index_columns + [cause_column] +
                                   data_columns))
    merge_columns = list(set(list(set(index_columns) - set([cause_column])) +
                             [parent_cause_column]))
    temp_child = data.ix[data[level_column] == level].copy(deep=True)
    temp_parent = data.ix[data[cause_column].isin(
        temp_child[parent_cause_column]
        .drop_duplicates())].copy(deep=True)
    temp_parent = temp_parent.ix[:, parent_keep_columns]
    parent_rename_columns = {'cause_id': 'parent_id'}
    for data_column in data_columns:
        parent_rename_columns[data_column] = '{}_parent'.format(data_column)
    temp_parent = temp_parent.rename(columns=parent_rename_columns)
    temp_child = pd.merge(temp_child, temp_parent, on=merge_columns)
    for data_column in data_columns:
        temp_child[data_column] = (
            temp_child.eval('{data_column} * {data_column}_parent'
                            .format(data_column=data_column), inplace=True))
        temp_child = temp_child.drop('{}_parent'.format(data_column), axis=1)
    return pd.concat([data.ix[data['level'] != level], temp_child])


def rescale_data(data, index_columns, data_columns, cause_column='cause_id',
                 parent_cause_column='parent_id', level_column='level'):
    """ Rescales data to make it internally consistent within hierarchy

    First, takes DataFrame and rescale to 1 within each level of the index
    columns

    Then runs down cause hierarchy and rescales each level according to the
    parent

    Returns a scaled DataFrame
    """
    # Rescale
    data = rescale_group(data,
                         list(set(index_columns) - set([cause_column])) +
                         [parent_cause_column, level_column],
                         data_columns)
    # Propagate down levels
    for level in (xrange(data[level_column].min() +
                         1, data[level_column].max() + 1)):
        data = rescale_to_parent(data, index_columns, data_columns,
                                 cause_column, parent_cause_column,
                                 level_column, level)
    return data


def convert_to_deaths(data, data_columns, envelope):
    """ Multiplies death draws by envelope """
    logger = logging.getLogger('correct.convert_to_deaths')
    try:
        # Merge on envelope data
        envelope_data = envelope.data
        envelope_data = envelope_data[envelope.index_columns + data_columns]
        rename_columns = {d: '{}_env'.format(d) for d in data_columns}
        envelope_data = envelope_data.rename(columns=rename_columns)
        data = pd.merge(data, envelope_data,
                        on=envelope.index_columns,
                        how='left')
        # Convert to death space
        for data_column in data_columns:
            data.eval('{d} = {d} * {d}_env'.format(d=data_column),
                      inplace=True)
        # Drop old columns
        data = data.drop(['{}_env'.format(d) for d in data_columns], axis=1)

    except Exception as e:
        logger.exception('Failed to convert to death space: {}'.format(e))
        sys.exit()

    return data


def add_zeroes_back(data, eligible_data, zeroes, index_columns, data_columns):
    for c in index_columns:
        eligible_data.ix[:, c] = eligible_data.ix[:, c].astype('int64')
        data.ix[:, c] = data.ix[:, c].astype('int64')

    # Merge with eligible data
    data = pd.merge(eligible_data[index_columns + ['restricted']],
                    data,
                    on=index_columns,
                    how='inner')
    # Restrict
    data = data.ix[data['restricted'] == False].reset_index(drop=True)

    # Add true zeroes (post-restriction) back in
    df = pd.concat([data, zeroes]).reset_index(drop=True)
    df = df[index_columns + data_columns].groupby(
        index_columns).sum().reset_index()
    return df


def save_unscaled_draws(data, index_columns):
    """ Saves unscaled draws in an h5 file """

    logger = logging.getLogger('correct.save_unscaled_draws')

    # Save unscaled draws
    draw_filepath = (parent_dir + r'FILEPATH.h5')
    save_hdf(data, draw_filepath, key='draws', mode='w',
             format='table', data_columns=index_columns)


def save_models(data, index_columns):
    """ Saves model version id for each data point in an h5 file """

    logger = logging.getLogger('correct.save_models')

    # Save model data
    draw_filepath = (parent_dir + r'FILEPATH.h5'
                     .format(location_id=location_id, sex_id=sex_id))
    save_hdf(data, draw_filepath, key='draws', mode='w',
             format='table', data_columns=index_columns)


def save_rescaled_draws(data, index_columns):
    """ Saves rescaled draws in an h5 file """
    logger = logging.getLogger('correct.save_rescaled_draws')

    # Save rescaled draws
    draw_filepath = (parent_dir + r'FILEPATH.h5')
    save_hdf(data, draw_filepath, key='draws', mode='w',
             format='table', data_columns=index_columns)


if __name__ == '__main__':

    # Get command line arguments
    output_version_id, location_id, sex_id = parse_args()

    # Set paths
    parent_dir = (r'FILEPATH')
    log_dir = parent_dir + r'/logs'

    # Start logging
    l.setup_logging(log_dir, 'correct', output_version_id, location_id,
                    sex_id)

    try:
        # Read in helper files
        logging.info("Reading in helper files")
        (config, best_models, eligible_data, spacetime_restriction_data,
         envelope_data) = read_helper_files(parent_dir, location_id, sex_id)

        # Read in config variables
        eligible_year_ids = config['eligible_year_ids']
        index_columns = config['index_columns']
        data_columns = config['data_columns']
        envelope_index_columns = config['envelope_index_columns']
        envelope_column = config['envelope_column']
        raw_data_columns = (['model_version_id'] + [envelope_column] +
                            index_columns + data_columns)

        # Make eligible data for data
        logging.info("Make eligible data list")
        eligible_data = eligible_data.ix[
            eligible_data['sex_id'] == int(sex_id)]
        eligible_data = expand_id_set(eligible_data, eligible_year_ids,
                                      'year_id')
        eligible_data['location_id'] = int(location_id)

        # Merge on space-time restrictions
        spacetime_restriction_data['spacetime_restriction'] = True
        eligible_data = pd.merge(eligible_data,
                                 spacetime_restriction_data,
                                 on=['location_id', 'year_id', 'cause_id'],
                                 how='left')

        # Apply space-time restrictions
        eligible_data.ix[eligible_data['spacetime_restriction'] == True,
                                       'restricted'] = True
        eligible_data = eligible_data.ix[:, ['cause_id', 'age_group_id',
                                             'sex_id', 'restricted', 'level',
                                             'parent_id', 'year_id',
                                             'location_id']]

        # Make envelope object
        logging.info("Make envelope object")
        envelope = Envelope(envelope_data, envelope_index_columns,
                            data_columns)

        # Read in draw files
        logging.info("Reading in best model draws")
        raw_data = read_all_model_draws(best_models, raw_data_columns,
                                        filter_years=eligible_year_ids)

        # Check formatting
        logging.info("Checking in best model draws")
        check_data_format(raw_data, raw_data_columns, fail=True)

        # Filter out zeros
        logging.info("Filtering out zeroes")
        data, zeroes = filter_zeros(raw_data, data_columns)

        # Format data for rescale
        logging.info("Formatting data for rescale")
        formatted_data, model_data = format_for_rescale(
            data,
            eligible_data,
            index_columns,
            data_columns,
            envelope_column,
            parent_dir + '/FILEPATH',
            '{}_{}_{}'.format(output_version_id, location_id, sex_id))

        # Save model data
        logging.info("Saving model data")
        save_models(model_data, index_columns)

        # Save input data
        logging.info("Saving input data")
        formatted_data_deaths = convert_to_deaths(formatted_data, data_columns,
                                                  envelope)
        formatted_data_deaths = add_zeroes_back(formatted_data_deaths,
                                                eligible_data, zeroes,
                                                index_columns, data_columns)
        save_unscaled_draws(formatted_data_deaths, index_columns)

        # Rescale data
        logging.info("Rescaling data")
        scaled_data = rescale_data(formatted_data, index_columns, data_columns,
                                   cause_column='cause_id',
                                   parent_cause_column='parent_id',
                                   level_column='level')

        # Saving data
        logging.info("Saving data")
        scaled_data = convert_to_deaths(scaled_data, data_columns, envelope)
        scaled_data = add_zeroes_back(scaled_data, eligible_data, zeroes,
                                      index_columns, data_columns)
        save_rescaled_draws(scaled_data, index_columns)

        print 'All done!'
        logging.info('All done!')
    except:
        logging.exception('uncaught exception in correct.py: {}'
                          .format(sys.exc_info()[0]))
