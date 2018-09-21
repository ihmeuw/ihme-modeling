# -*- coding: utf-8 -*-
"""
Created on Thu Jan 19 16:34:36 2017

@author: USERNAME

Script that interpolates the hospital utilization envelope, and
aggregates the envelope for under one years old.  Only needs to be ran when
there is a new envelope.

RUN WITH MORE THAN 10 SLOTS!
"""

import platform
import pandas as pd
from db_queries import get_population
from db_tools.ezfuncs import query
from transmogrifier import draw_ops
import datetime

# Environment:
if platform.system() == "Linux":
    root = "FILEPATH"
else:
    root = "FILEPATH"

# INTERPOLATE
# doing both sexes and all years at once breaks. multiprocessing tries
# to send very large arrays between processes and it can't handle stuff
# this big. It'll just hang and never error-out

# takes about 18 minutes to run all the interpolations
def run_envelope_interp(write_both_envelopes=False):
    today = datetime.datetime.today().strftime("%Y_%m_%d")  # YYYY-MM-DD
    env_df_male_1 = draw_ops.interpolate(
       gbd_id_field='modelable_entity_id',
       gbd_id=10334,
       source='epi',
       measure_ids=[19],
       sex_ids=1,
       reporting_year_start=1990,
       reporting_year_end=2000)
    env_df_male_2 = draw_ops.interpolate(
       gbd_id_field='modelable_entity_id',
       gbd_id=10334,
       source='epi',
       measure_ids=[19],
       sex_ids=1,
       reporting_year_start=2000,
       reporting_year_end=2016)
    env_df_female_1 = draw_ops.interpolate(
        gbd_id_field='modelable_entity_id',
        gbd_id=10334,
        source='epi',
        measure_ids=[19],
        sex_ids=2,
        reporting_year_start=1990,
        reporting_year_end=2000)
    env_df_female_2 = draw_ops.interpolate(
        gbd_id_field='modelable_entity_id',
        gbd_id=10334,
        source='epi',
        measure_ids=[19],
        sex_ids=2,
        reporting_year_start=2000,
        reporting_year_end=2016)

    # drop duplicate of year 2000
    env_df_female_2 = env_df_female_2[env_df_female_2['year_id'] != 2000]
    env_df_male_2 = env_df_male_2[env_df_male_2['year_id'] != 2000]

    # put them together
    env_df = pd.concat([env_df_male_1, env_df_male_2, env_df_female_1, env_df_female_2]).reset_index(drop=True)

    # AGGREGATE ENVELOPE FOR AGE LESS THAN ONE YEAR OLD

    # Keep envelope where age is under one
    under_one_env = env_df[(env_df['age_group_id'] == 2) |
                           (env_df['age_group_id'] == 3) |
                           (env_df['age_group_id'] == 4)]

    # get years that are present in under one envelope
    years = list(under_one_env.year_id.unique())

    # get location_ids that are present in the envelope
    loc_list = list(under_one_env.location_id.unique())

    # get population data for under one years old age groups
    under_one_pop = get_population(age_group_id=[2, 3, 4], location_id=loc_list,
                                   sex_id=[1, 2], year_id=years)
    # under_one_pop = get_population(age_group_id=[2, 3, 4], location_id=-1,
    #                                sex_id=[1, 2], year_id=years)

    # Merge population data onto under one envelope
    under_one_env = under_one_env.merge(under_one_pop, how='left',
                                        on=['location_id', 'sex_id',
                                            'age_group_id', 'year_id'])

    # create list of draw columns to subset and run operations on
    draw_cols = [col for col in list(under_one_env) if col.startswith('draw_')]
    assert len(draw_cols) == 1000,\
    "There must be 1000 draws in the interpolated envelope"

    # Compute products that go in the numerator of the formula for every draw
    under_one_env[draw_cols] = under_one_env[draw_cols].\
            multiply(under_one_env['population'], axis=0)

    # Sum. This computes the numerator and denominator of the formula
    # (numerator = sum of the products env_pop['mean']*env_pop['population'],
    # denominator = sum of populations for each age group
    # we don't groupby ages, since we're trying to aggregate them into one group!
    # under_one_env = under_one_env.groupby(by=['location_id', 'sex_id', 'year_id'])\
    #    .agg({'numerator_mean': 'sum', 'population': 'sum'}).reset_index()

    under_one_env = under_one_env.groupby(by=['location_id', 'sex_id', 'year_id'])\
                                           .sum().reset_index()

    # Compute the aggregated under one years old envelope value
    under_one_env[draw_cols] = under_one_env[draw_cols].\
                div(under_one_env['population'], axis = 0)


    # Add age_group_id and measure_id and modelable_entitiy_id
    under_one_env['age_group_id'] = 28  # age group id for 0-1 years old,
    # i.e., 0-364 days old.

    # drop unneeded columns
    under_one_env.drop(['process_version_map_id',
                        'population', 'measure_id',
                        'modelable_entity_id'], axis=1, inplace=True)

    # drop age groups 2, 3, and 4 from envelope
    env_df = env_df[(env_df['age_group_id'] != 2)]
    env_df = env_df[(env_df['age_group_id'] != 3)]
    env_df = env_df[(env_df['age_group_id'] != 4)]

    # append aggregated under one years old envelope onto full envelope
    env_df = env_df.append(under_one_env)

    # Take the mean of all the draws
    env_df['mean'] = env_df[draw_cols].mean(axis=1)

    # Add uncertainty, find the 2.5th percentile and 97.5 percentile
    # calculate quantiles for every row, then transpose and col bind
    quant = env_df[draw_cols].quantile([0.025, 0.975], axis=1).transpose()
    # rename columns
    quant.columns = ['lower', 'upper']
    # col bind quant to env
    env_df = pd.concat([env_df, quant], axis=1)

    # MERGE AGES ONTO ENVELOPE
    # we want the envelope to have age_start and age_end instead of
    # age_group_id, so import table with age_groups
    age_group = query("QUERY")

    # database has age end in format of 10, 15, 20, but should be 9, 14, 19
    # age_group['age_group_years_end'].loc[age_group['age_group_years_end'] > 1]\
    #     = age_group['age_group_years_end'].\
    #     loc[age_group['age_group_years_end'] > 1] - 1
    age_group.loc[age_group['age_group_years_end'] > 1, 'age_group_years_end']\
        = age_group.\
        loc[age_group['age_group_years_end'] > 1, 'age_group_years_end'] - 1

    # merge age_start and age_end on
    env_df = env_df.merge(age_group[['age_group_id', 'age_group_years_start',
                                     'age_group_years_end']], how='left',
                          on='age_group_id')
    assert env_df['age_group_years_start'].isnull().sum() == 0,\
    "The merge did not properly attach age_start"
    assert env_df['age_group_years_end'].isnull().sum() == 0,\
    "The merge did not properly attach age_end"

    # rename envelope age start and age end columns
    env_df.rename(columns={"age_group_years_end": "age_end",
                           "age_group_years_start": "age_start"}, inplace=True)
    # make year_start and year_end
    env_df['year_start'] = env_df['year_id']
    env_df['year_end'] = env_df['year_id']
    env_df.drop('year_id', axis=1, inplace=True)

    # assert mean smaller than upper and larger than lower
    assert (env_df['upper'] > env_df['mean']).sum() == env_df.shape[0],\
    "Upper estimate is not always larger than mean"
    assert (env_df['mean'] > env_df['lower']).sum() == env_df.shape[0],\
    "Mean estimate is not always larger than lower"

    if write_both_envelopes:
        # NOTE, old key was "table"
        env_df.to_hdf("FILEPATH",
                      key='df', complib='blosc', complevel=5, mode='w')

        # keep only needed columns
        env_df = env_df[['location_id', 'age_group_id',
                         'age_start', 'age_end', 'sex_id',
                         'year_start', 'year_end',
                         'mean', 'upper', 'lower']]

        env_df.to_csv("FILEPATH",
                      index=False)
    return(env_df)
