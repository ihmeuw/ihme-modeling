import sys

import logging
import argparse
import datetime
import getpass
from functools import partial
from multiprocessing import Pool
import numpy as np
import os
import pandas as pd
import gc

from core_maths.summarize import pct_change
from draw_sources.draw_sources import DrawSource
import gbd.constants as GBD
from hierarchies.dbtrees import agetree, loctree, sextree

from codcorrect.io import read_hdf_draws, change_permission
from codcorrect.core import read_json, grouper
import codcorrect.log_utilities as cc_log_utils


"""
This script generates summary files for all final codcorrect draws, but does
so differently by GBD database, since each database has a different summary
table schema. This includes create age/sex aggregates and calculating
pct_change.
"""


def parse_args():
    """Parse command line arguments

    Arguments are output_version_id, location_id

        Returns all arguments as a tuple, in that order
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--output_version_id", type=str)
    parser.add_argument("--location_id", type=str)
    parser.add_argument("--db", type=str)

    args = parser.parse_args()
    output_version_id = args.output_version_id
    location_id = args.location_id
    db = args.db

    return output_version_id, location_id, db


def read_helper_files(parent_dir, location_id):
    """Read in and return helper DataFrames.

        Returns:
            Config
            DataFrame containing age_weights for age-standardized rate
                calculation
            Most detailed locations
    """

    # Config file
    config = read_json(os.path.join(parent_dir, '_temp/config.json'))

    # Age weights
    age_weights = pd.read_csv(
        os.path.join(parent_dir, '_temp/age_weights.csv'))

    # Most-detailed location
    location_hierarchy = pd.read_csv(
        os.path.join(parent_dir, '_temp/location_hierarchy.csv'))
    estimate_locations = location_hierarchy.loc[
        location_hierarchy['is_estimate'] == 1, 'location_id'].tolist()

    if int(location_id) in estimate_locations:
        most_detailed_location = True
    else:
        most_detailed_location = False

    return age_weights, most_detailed_location, config


def read_cod_draw_files(pool, parent_dir, location_id, years):
    """Pull in all data to be summarized for CoD, by location and filtering by
    years."""
    logger = logging.getLogger('summary.read_cod_draw_files')
    try:
        agg_rescaled_params = {
            'draw_dir': os.path.join(parent_dir, 'aggregated/rescaled'),
            'file_pattern': '{measure_id}_{location_id}_{year_id}.h5'
        }
        ds = DrawSource(agg_rescaled_params)
        rescaled_draws = ds.content(filters={'location_id': location_id,
                                             'year_id': years,
                                             'measure_id': 1})

        daly_draw_params = {
            'draw_dir': os.path.join(parent_dir, 'draws'),
            'file_pattern': '{measure_id}_{location_id}.h5',
        }
        ds = DrawSource(daly_draw_params)
        dalynator_draws = ds.content(filters={'location_id': location_id,
                                              'year_id': years,
                                              'measure_id': 1})
        return rescaled_draws, dalynator_draws
    except Exception as e:
        logger.exception('Failed to read location: {}'.format(e))


def read_gbd_draw_files(parent_dir, location_id, years, measure_id):
    """Pull in all data to be summarized for gbd, by location and measure, and
    filtering by years."""
    logger = logging.getLogger('summary.read_gbd_draw_files')
    try:
        params = {
            'draw_dir': os.path.join(parent_dir, 'draws'),
            'file_pattern': '{measure_id}_{location_id}.h5'
        }
        ds = DrawSource(params)
        return ds.content(filters={'measure_id': measure_id,
                                   'location_id': location_id,
                                   'year_id': years})
    except Exception as e:
        logger.exception('Failed to read location: {}'.format(e))


def get_model_numbers(location_id, index_columns):
    """Reads in model version ids, in order to tack these on for CoD db."""
    logger = logging.getLogger('summary.get_model_numbers')
    try:
        data = []
        for sex_id in [1, 2]:
            draw_filepath = (os.path.join(
                parent_dir,
                'models/models_{loc}_{sex}.h5'
                .format(loc=location_id, sex=sex_id)))
            data.append(read_hdf_draws(draw_filepath, location_id))
        data = pd.concat(data).reset_index(drop=True)

        data = data[index_columns + ['model_version_id']]
    except Exception as e:
        logger.exception('Failed to read model version data: {}'.format(e))
    return data


def generate_aggregated_ages(df, index_columns, database='gbd'):
    """Takes in a dataframe in count space, calculates all aggregated ages, and
    adds to the dataframe.

    Arguments:
        df (pd.DataFrame): dataframe containing indices and draws to create
            age aggregates with.
        index_columns (str[]): list of strings represnting the data indices to
            aggregate over.

    Returns:
        The original dataset with all-ages added.
    """
    compare_ages = GBD.GBD_COMPARE_AGES
    # remove 28 from our list of GBD compare ages. We don't compute under one.
    if 28 in compare_ages:
        compare_ages.remove(28)

    if 22 not in compare_ages:
        compare_ages.append(22)

    if (database == 'cod') and (21 in compare_ages):
        compare_ages.remove(21)

    df = df[~df['age_group_id'].isin(compare_ages)]
    # create age trees
    age_trees = []
    for age_group in compare_ages:
        tree = agetree(age_group_id=age_group, gbd_round_id=GBD.GBD_ROUND_ID)
        age_trees.append(tree)

    agg_ages = []
    for atree in age_trees:
        child_ids = list(map(lambda x: x.id, atree.root.children))
        temp = df[df['age_group_id'].isin(child_ids)].copy(deep=True)
        temp['age_group_id'] = atree.root.id
        temp = temp.groupby(index_columns).sum().reset_index()
        agg_ages.append(temp)
    agg_ages_df = pd.concat(agg_ages)
    df = pd.concat([df, agg_ages_df]).reset_index(drop=True)
    return df


def generate_both_sexes(df, index_columns):
    """
    Aggregate sex_id 1 and 2 into both sexes.

    Arguments:
        df (pd.DataFrame):
        index_columns (str[]):

    Returns:
        The original dataframe with both-sexes appended
    """
    temp = df.copy(deep=True)
    temp['sex_id'] = 3
    temp = temp.groupby(index_columns).sum().reset_index()
    df = pd.concat([df, temp])
    return df


def generate_asr(df, index_cols, pop_column, data_cols, age_weights, db):
    """
    Takes in a dataframe in count space, calculates age-std, and adds it to
    the dataframe.

    Arguments:
        df (pd.DataFrame):
        index_cols (str[]):
        pop_column (str):
        data_cols (str[]):
        age_weights (pd.DataFrame):

    Returns:
        The original dataframe with age-std rates appended
    """
    most_detailed_ages = list(range(2, 21)) + [30, 31, 32, 235]
    temp = df[df['age_group_id'].isin(most_detailed_ages)].copy(deep=True)

    temp = pd.merge(temp, age_weights, on=['age_group_id'])
    for c in data_cols:
        temp[c] = (temp[c] / temp[pop_column]) * temp['age_group_weight_value']

    temp['age_group_id'] = 27
    temp = temp.drop('age_group_weight_value', axis=1)
    temp = temp.groupby(index_cols).sum().reset_index()
    if 'gbd' in db:
        df['metric_id'] = 1
        temp['metric_id'] = 3
    df = pd.concat([df, temp])
    return df


def generate_cod_cause_fractions(data, index_columns, data_columns):
    """Takes in a dataframe, calculates cause fractions as specific
    cause/all-cause, summarizes this, and adds this new data to the dataframe.

        Returns: The original dataframe with cause fractions appended
    """
    orig = data.copy(deep=True)
    temp = data.loc[(data['cause_id'] == 294) &
                    (data['age_group_id'] != 27)].copy(deep=True)
    temp = temp[['location_id', 'year_id', 'sex_id', 'age_group_id'] +
                ['draw_{}'.format(x) for x in range(1000)]]
    rename_columns = {'draw_{}'.format(x): 'env_{}'.format(x)
                      for x in range(1000)}
    temp = temp.rename(columns=rename_columns)
    data = pd.merge(data, temp,
                    on=['location_id', 'year_id', 'sex_id', 'age_group_id'])
    for x in range(1000):
        data['env_{}'.format(x)].replace(0, 1e-9, inplace=True)
        data['draw_{}'.format(x)] = data['draw_{}'.format(x)
                                         ] / data['env_{}'.format(x)]
    rename_columns = {'{}_death'.format(x): '{}_cf'.format(x)
                      for x in ['mean', 'lower', 'upper']}
    rename_vals = list(rename_columns.values())
    data = generate_cod_summaries(data, index_columns,
                                  data_columns).rename(columns=rename_columns)
    orig = pd.merge(orig, data, on=index_columns, how='left')
    orig[rename_vals] = orig[rename_vals].fillna(0)
    return orig


def generate_gbd_cause_fractions(data):
    """Takes in a dataframe, calculates cause fractions as specific
    cause/all-cause, and adds this new data in with a metric_id of 2.

        Returns: The original dataframe with cause fractions appended
    """
    env = data.loc[(data['cause_id'] == 294) &
                   (data['metric_id'] == 1)].copy(deep=True)
    env = env[['location_id', 'year_id', 'sex_id', 'age_group_id'] +
              ['draw_{}'.format(x) for x in range(1000)]]
    for x in range(1000):
        env['draw_{}'.format(x)] = env['draw_{}'.format(x)].replace(0, 1e-9)
    rename_columns = {'draw_{}'.format(x): 'env_{}'.format(x)
                      for x in range(1000)}
    env = env.rename(columns=rename_columns)
    tempo = data.loc[data['metric_id'] == 1].copy(deep=True)
    tempo = pd.merge(tempo, env,
                     on=['location_id', 'year_id', 'sex_id', 'age_group_id'])
    for x in range(1000):
        tempo['draw_{}'.format(x)] = tempo['draw_{}'.format(x)
                                           ] / tempo['env_{}'.format(x)]
    tempo['metric_id'] = 2
    tempo.drop(['env_{}'.format(i) for i in range(1000)], axis=1,
               inplace=True)
    data = pd.concat([data, tempo])
    return data


def generate_gbd_rates(data, index_columns, data_columns, pop_column):
    """Takes in a dataframe, calculates rates as specific cause/pop, and adds
    this new data in with a metric_id of 3.

        Returns: The original dataframe with rates appended
    """
    temp = data.loc[data['metric_id'] == 1].copy(deep=True)
    for c in data_columns:
        temp[c] = (temp[c] / temp[pop_column])
    temp['metric_id'] = 3
    return pd.concat([data, temp])


def generate_cod_summaries(df, index_columns, data_columns):
    """
    Generate mean, lower, and upper in the schema required for the CoD db.

    Arguments:
        df (pd.DataFrame):
        index_columns (str[]):
        data_columns (str[]):

    Returns:
        Mutated input dataframe with just the index columns and mean_death,
        lower_death, and upper_death.
    """
    df['mean_death'] = np.mean(df[data_columns].values, axis=1)
    df['lower_death'] = np.percentile(df[data_columns].values, q=2.5, axis=1)
    df['upper_death'] = np.percentile(df[data_columns].values, q=97.5, axis=1)
    df = df[index_columns + ['mean_death', 'lower_death', 'upper_death']]
    return df


def generate_gbd_summaries(df, index_columns, data_columns, change=False):
    """
    Generate mean, lower, and upper in the schema required for the gbd db.

    Arguements:
        df (pd.DataFrame):
        index_columns (str[]):
        data_columns (str[]):
        change (bool):
    """
    if change:
        df.rename(columns={'pct_change_means': 'val'}, inplace=True)
    else:
        df['val'] = np.mean(
            df[data_columns].values, axis=1
        )
    df['lower'] = np.percentile(
        df[data_columns].values, q=2.5, axis=1
    )
    df['upper'] = np.percentile(
        df[data_columns].values, q=97.5, axis=1
    )
    df = df[index_columns + ['val', 'lower', 'upper']]
    return df


def format_df(df, db, measure_id=1):
    """Formats draws in the appropriate file structure for each db.

       Returns: Formatted df
    """
    if db == 'cod':
        df['output_version_id'] = output_version_id
        df['date_inserted'] = datetime.datetime.now()
        df['inserted_by'] = getpass.getuser()
        df['last_updated'] = datetime.datetime.now()
        df['last_updated_by'] = getpass.getuser()
        df['last_updated_action'] = 'INSERT'
    else:
        df['measure_id'] = measure_id
    return df


def save_cod_summaries(data, location_id, years):
    """Saves draws wide in an h5 file in the schema required for the cod db.

       Returns: None
    """

    # Save draws
    data = data[['output_version_id',
                 'cause_id',
                 'year_id',
                 'location_id',
                 'sex_id',
                 'age_group_id',
                 'model_version_id',
                 'mean_cf',
                 'upper_cf',
                 'lower_cf',
                 'mean_death',
                 'upper_death',
                 'lower_death',
                 'mean_cf_with_shocks',
                 'upper_cf_with_shocks',
                 'lower_cf_with_shocks',
                 'mean_death_with_shocks',
                 'upper_death_with_shocks',
                 'lower_death_with_shocks',
                 'date_inserted',
                 'inserted_by',
                 'last_updated',
                 'last_updated_by',
                 'last_updated_action']]
    data.sort_values(by=['cause_id', 'year_id', 'location_id', 'sex_id',
                     'age_group_id', 'model_version_id'], inplace=True)
    year_str = ''.join(str(yr) for yr in years)
    data.to_csv(
        parent_dir + r'/summaries/cod/1_{location_id}_{year_str}.csv'.format(
            location_id=location_id, year_str=year_str), index=False)


def save_gbd_summaries(data, location_id, years, measure_id, change=False):
    """Saves draws long in an h5 file in the schema required for the gbd db.

       Returns: None
    """
    keep_cols = ['measure_id',
                 'location_id',
                 'sex_id',
                 'age_group_id',
                 'cause_id',
                 'metric_id',
                 'val',
                 'upper',
                 'lower']
    if change is True:
        keep_cols.extend(['year_start_id', 'year_end_id'])
        data = data[keep_cols]
        data.sort_values(by=['measure_id', 'year_start_id', 'year_end_id',
                             'location_id', 'sex_id', 'age_group_id',
                             'cause_id', 'metric_id'], inplace=True)
        for year in data.year_start_id.unique():
            filepath = (parent_dir + r'/summaries/gbd/multi/{m}/{y}/{loc}.csv'
                        .format(m=int(measure_id), y=int(year),
                                loc=int(location_id)))
            data.loc[data.year_start_id == year].to_csv(filepath, index=False)
            change_permission(filepath)
    else:
        keep_cols.extend(['year_id'])
        data = data[keep_cols]
        data.sort_values(by=['measure_id', 'year_id', 'location_id', 'sex_id',
                             'age_group_id', 'cause_id', 'metric_id'],
                         inplace=True)
        for year in data.year_id.unique():
            filepath = (parent_dir + r'/summaries/gbd/single/{m}/{y}/{loc}.csv'
                        .format(m=int(measure_id), y=int(year),
                                loc=int(location_id)))
            data.loc[data.year_id == year].to_csv(filepath, index=False)
            change_permission(filepath)


def aggregate_pop(pop_data, index_columns=['location_id', 'year_id', 'sex_id',
                  'age_group_id']):
    """Adds age/sex aggregates on to the population dataframe."""
    logging.info("Generating both-sexes on population")
    pop_data = generate_both_sexes(pop_data, index_columns)

    logging.info("Generating aggregated-ages on population")
    pop_data = generate_aggregated_ages(pop_data, index_columns)
    return pop_data


def main_summarize_cod(pool, pop_data, index_columns, data_columns,
                       location_id, years, most_detailed_location):
    """Execute all the steps needed to summarize for the cod db. Do this once
    for "dalynator draws" which include shocks, and "rescaled_draws" which
    don't include shocks."""
    logger = logging.getLogger('summary.main_summarize_cod')
    try:
        if most_detailed_location:
            logging.info("Reading in model files")
            model_version_data = get_model_numbers(location_id, index_columns)

        logging.info("Reading with and without shocks draws for COD")
        rescaled_draws, dalynator_draws = read_cod_draw_files(
            pool, parent_dir, location_id, years)

        logging.info("Generating both-sexes on rescaled draws")
        rescaled_draws = generate_both_sexes(rescaled_draws, index_columns)

        logging.info("Generating aggregated-ages on rescaled draws")
        rescaled_draws = generate_aggregated_ages(rescaled_draws,
                                                  index_columns,
                                                  database='cod')

        rescaled_draws = pd.merge(rescaled_draws, pop_data,
                                  on=['location_id', 'year_id', 'sex_id',
                                      'age_group_id'],
                                  how='left')
        rescaled_draws['pop'] = rescaled_draws['pop'].fillna(0)

        logging.info("Generating age-standardized rates on rescaled draws")
        rescaled_draws = generate_asr(rescaled_draws, index_columns,
                                      'pop', data_columns, age_weights, 'cod')

        logging.info("Generating summaries on rescaled draws")
        rescaled_summaries = generate_cod_summaries(rescaled_draws,
                                                    index_columns,
                                                    data_columns)

        logging.info("Generating cause fractions on rescaled draws")
        rescaled_summaries = generate_cod_cause_fractions(rescaled_draws,
                                                          index_columns,
                                                          data_columns)
        del rescaled_draws
        gc.collect()

        logging.info("Generating both-sexes on dalynator draws")
        dalynator_draws = generate_both_sexes(dalynator_draws, index_columns)

        logging.info("Generating aggregated-ages on dalynator draws")
        dalynator_draws = generate_aggregated_ages(dalynator_draws,
                                                   index_columns,
                                                   database='cod')

        dalynator_draws = pd.merge(dalynator_draws, pop_data,
                                   on=['location_id', 'year_id', 'sex_id',
                                       'age_group_id'],
                                   how='left')
        dalynator_draws['pop'] = dalynator_draws['pop'].fillna(0)

        logging.info("Generating age-standardized rates on dalynator draws")
        dalynator_draws = generate_asr(dalynator_draws, index_columns,
                                       'pop', data_columns, age_weights, 'cod')

        logging.info("Generating summaries on dalynator draws")
        dalynator_summaries = generate_cod_summaries(dalynator_draws,
                                                     index_columns,
                                                     data_columns)

        logging.info("Generating cause fractions on dalynator draws")
        dalynator_summaries = generate_cod_cause_fractions(dalynator_draws,
                                                           index_columns,
                                                           data_columns)

        del dalynator_draws
        gc.collect()
        if most_detailed_location:
            logging.info("Merge on model_version_ids")
            rescaled_summaries = pd.merge(rescaled_summaries,
                                          model_version_data,
                                          on=index_columns, how='left')
            rescaled_summaries['model_version_id'] = rescaled_summaries[
                'model_version_id'].fillna(0)
        else:
            rescaled_summaries['model_version_id'] = 0

        logging.info("Merging shocks and non-shocks together")
        rename_columns = {}
        for t in ['cf', 'death']:
            for b in ['mean', 'lower', 'upper']:
                rename_columns['{}_{}'.format(
                    b, t)] = '{}_{}_with_shocks'.format(b, t)
        dalynator_summaries = dalynator_summaries.rename(
            columns=rename_columns)
        data_summaries = pd.merge(rescaled_summaries, dalynator_summaries,
                                  on=index_columns, how='outer')
        data_summaries[list(rename_columns.keys())] = (
            data_summaries[list(rename_columns.keys())].fillna(0))
        data_summaries['model_version_id'] = data_summaries[
            'model_version_id'].fillna(0)

        logging.info("Formatting final COD df")
        data_summaries = format_df(data_summaries, 'cod')

        logging.info("Saving COD summaries")
        save_cod_summaries(data_summaries, location_id, years)
    except Exception as e:
        logger.exception("Summarizing COD failed: {}".format(e))
        raise e


def main_summarize_gbd(pop_data, index_columns, data_columns, years,
                       location_id, measure_id):
    """Execute all the steps needed to summarize for the gbd db. This includes
    calculating pct-change."""
    logger = logging.getLogger('summary.main_summarize_gbd')
    try:
        if 'change' in measure_id:
            change = True
        else:
            change = False
        measure_id = int(measure_id.lstrip("change_"))
        logging.info("Reading in draw files for GBD, for measure {}"
                     .format(measure_id))
        draws = read_gbd_draw_files(parent_dir, location_id, years, measure_id)

        logging.info("Generating both-sexes")
        draws = generate_both_sexes(draws, index_columns)

        logging.info("Generating aggregated-ages")
        draws = generate_aggregated_ages(draws, index_columns, database='gbd')

        logging.info("Merging population on")
        draws = pd.merge(draws, pop_data, on=['location_id', 'year_id',
                                              'sex_id', 'age_group_id'],
                         how='left')
        draws['pop'] = draws['pop'].fillna(0)

        logging.info("Generating age-standardized rates")
        draws = generate_asr(draws, index_columns, 'pop', data_columns,
                             age_weights, 'gbd')

        logging.info("Generating rates")
        index_columns = index_columns + ['metric_id']
        draws = generate_gbd_rates(draws, index_columns, data_columns, 'pop')
        draws.drop('pop', axis=1, inplace=True)

        logging.info("Generating cause fractions")
        draws = generate_gbd_cause_fractions(draws)

        if change:
            logging.info("Generating pct-change")
            change_dict = {1990: [2007, 2017], 2007: [2017]}
            change_list = []
            for start in change_dict.keys():
                for end in change_dict[start]:
                    change_df = pct_change(draws, start, end, 'year_id',
                                           data_columns,
                                           change_type='pct_change')
                    change_df.dropna(inplace=True)
                    change_list.append(change_df)
            draws = pd.concat(change_list).reset_index(drop=True)
            index_columns = ['location_id', 'year_start_id', 'year_end_id',
                             'age_group_id', 'sex_id', 'cause_id', 'metric_id']

        logging.info("Generating summaries for GBD")
        data_summaries = generate_gbd_summaries(draws, index_columns,
                                                data_columns, change)

        del draws
        gc.collect()
        logging.info("Formatting final GBD df")
        data_summaries = format_df(data_summaries, 'gbd', measure_id)

        logging.info("Saving GBD summaries")
        save_gbd_summaries(data_summaries, location_id, years, measure_id,
                           change)
        rc = 1
    except Exception as e:
        logger.exception("Summarizing GBD failed: {}".format(e))
        rc = e
    return rc


if __name__ == '__main__':

    # Get command line arguments
    output_version_id, location_id, db = parse_args()

    # Set paths
    parent_dir = 'FILEPATH'
    log_dir = os.path.join(parent_dir, 'logs')

    # Start logging
    cc_log_utils.setup_logging(log_dir, 'summary', output_version_id,
                               location_id, db)

    try:
        # Read in helper files
        logging.info("Reading in helper files")
        age_weights, most_detailed_location, config = read_helper_files(
            parent_dir, location_id)

        # Read in config variables
        index_columns = config['index_columns']

        if 'measure_id' in index_columns:
            index_columns.remove('measure_id')

        data_columns = config['data_columns']
        change_years = config['change_years']
        years = [yr for yr in config['eligible_year_ids']
                 if yr not in change_years]
        # ensure that the change years get grouped together
        for i, y in enumerate(change_years):
            years.insert(i, y)

        # read in population for this location
        pop_data = pd.read_hdf(
            os.path.join(parent_dir, '_temp/pop.h5'),
            'summary',
            where=["'location_id'=={}".format(location_id)])
        pop_data['pop'].replace(0, 1e-9, inplace=True)
        pop_data = aggregate_pop(pop_data)

        # summarize in a loop, grouping by years, to reduce memory usage
        if db == 'cod':
            pool = Pool(6)
        else:
            pool = Pool(4)
        for yr_set in grouper(years, 6, None):
            yrs = [yr for yr in yr_set if yr is not None]
            if db == 'cod':
                logging.info("Db is COD; Running main_summarize_cod")
                main_summarize_cod(pool, pop_data, index_columns, data_columns,
                                   location_id, yrs, most_detailed_location)
            elif db == 'gbd':
                logging.info("Db is GBD; Running main_summarize_gbd parallel")
                measure_ids = ["1", "4", "change_1", "change_4"]
                summarize_gbd = partial(main_summarize_gbd, pop_data,
                                        index_columns, data_columns,
                                        yrs, location_id)
                rc = pool.map(summarize_gbd, measure_ids)
            else:
                raise ValueError("db must be 'DATABASE' or 'DATABASE'")
        pool.close()
        pool.join()
        if db == 'gbd':
            e = [e for e in rc if e != 1]
            if e:
                raise e[0]
        logging.info('All done!')
    except Exception:
        logging.exception('uncaught exception in summary.py: {}'
                          .format(sys.exc_info()[0]))
        sys.exit(1)
