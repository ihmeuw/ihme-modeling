# -*- coding: utf-8 -*-
"""
Created on Thu Jan 19 16:34:36 2017

Script that interpolates the hospital utilization envelope, and
aggregates the envelope for under one years old.  Only needs to be ran when
there is a new envelope.

RUN WITH MORE THAN 10 SLOTS
"""

import platform
import pandas as pd
from db_queries import get_population
from db_tools.ezfuncs import query
# from transmogrifier import draw_ops
import datetime
import glob
import warnings

root = "FILEPATH"

# INTERPOLATE
# doing both sexes and all years at once breaks. multiprocessing tries
# to send very large arrays between processes and it can't handle stuff
# this big. It'll just hang and never error-out

# takes about 30 minutes to run all the interpolations
#
def run_envelope_interp(write_both_envelopes, version_id):
    today = datetime.datetime.today().strftime("%Y_%m_%d")  # YYYY-MM-DD

    # version_id to int
    try:
        version_id = int(version_id)
    except:
        print("version id can not be converted to an integer, ending program")
        assert False

    print("Interpolating males 1990-2000...")
    env_df_male_1 = draw_ops.interpolate(
       gbd_id_field='modelable_entity_id',
       gbd_id=10334,
       source='epi',
       measure_ids=[19],
       sex_ids=1,
       reporting_year_start=1990,
       reporting_year_end=2000,
       version_id=version_id)

    print("Interpolating males 2000-2017...")
    env_df_male_2 = draw_ops.interpolate(
       gbd_id_field='modelable_entity_id',
       gbd_id=10334,
       source='epi',
       measure_ids=[19],
       sex_ids=1,
       reporting_year_start=2000,
       reporting_year_end=2017,
       version_id=version_id)

    print("Interpolating females 1990-2000...")
    env_df_female_1 = draw_ops.interpolate(
        gbd_id_field='modelable_entity_id',
        gbd_id=10334,
        source='epi',
        measure_ids=[19],
        sex_ids=2,
        reporting_year_start=1990,
        reporting_year_end=2000,
        version_id=version_id)

    print("Interpolating females 2000-2017...")
    env_df_female_2 = draw_ops.interpolate(
        gbd_id_field='modelable_entity_id',
        gbd_id=10334,
        source='epi',
        measure_ids=[19],
        sex_ids=2,
        reporting_year_start=2000,
        reporting_year_end=2017,
        version_id=version_id)

    # drop duplicate of year 2000
    env_df_female_2 = env_df_female_2[env_df_female_2['year_id'] != 2000]
    env_df_male_2 = env_df_male_2[env_df_male_2['year_id'] != 2000]

    # put them together
    print("Concatenating...")
    env_df = pd.concat([env_df_male_1, env_df_male_2, env_df_female_1, env_df_female_2]).reset_index(drop=True)

    # AGGREGATE ENVELOPE FOR AGE LESS THAN ONE YEAR OLD

    # aggregate under one year old according to this formula:
    # (envelope_{2}*population_{2} + envelope_{3}*population_{3}
    # + envelope_{4}*population_{4}) / pop_{2,3,4},
    # where indecies are age_group_ids

    # Keep envelope where age is under one
    print("Aggregating ages under 1 years old...")
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
    under_one_env.drop(['run_id',
                        'population', 'measure_id',
                        'modelable_entity_id'], axis=1, inplace=True)

    # drop age groups 2, 3, and 4 from envelope
    env_df = env_df[(env_df['age_group_id'] != 2)]
    env_df = env_df[(env_df['age_group_id'] != 3)]
    env_df = env_df[(env_df['age_group_id'] != 4)]

    # append aggregated under one years old envelope onto full envelope
    env_df = env_df.append(under_one_env)

    # Take the mean of all the draws as per Ryan 12/27/2016
    env_df['mean'] = env_df[draw_cols].mean(axis=1)

    # Add uncertainty, find the 2.5th percentile and 97.5 percentile
    # calculate quantiles for every row, then transpose and col bind
    print("Calculating percentiles...")
    quant = env_df[draw_cols].quantile([0.025, 0.975], axis=1).transpose()
    # rename columns
    quant.columns = ['lower', 'upper']
    # col bind quant to env
    env_df = pd.concat([env_df, quant], axis=1)

    # MERGE AGES ONTO ENVELOPE
    # we want the envelope to have age_start and age_end instead of
    # age_group_id, so import table with age_groups
    print("Merging ages onto Envelope...")
    age_group = query("select age_group_id,\
                      age_group_years_start, age_group_years_end\
                      from age_group", conn_def='shared')

    # database has age end in format of 10, 15, 20, but should be 9, 14, 19
    # age_group['age_group_years_end'].loc[age_group['age_group_years_end'] > 1]\
    #     = age_group['age_group_years_end'].\
    #     loc[age_group['age_group_years_end'] > 1] - 1
    age_group.loc[age_group['age_group_years_end'] > 1, 'age_group_years_end']\
        = age_group.\
        loc[age_group['age_group_years_end'] > 1, 'age_group_years_end'] - 1

    # merge age_start and age_end on
    shape_before = env_df.shape[0]
    env_df = env_df.merge(age_group[['age_group_id', 'age_group_years_start',
                                     'age_group_years_end']], how='left',
                          on='age_group_id')
    if shape_before != env_df.shape[0]:
        print("Rows changes while merging ages!!!!!!")
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
        env_df.to_hdf(FILEPATH.\
                      format(version_id, today),
                      key='df', complib='blosc', complevel=5, mode='w')

        # keep only needed columns
        env_df = env_df[['location_id', 'age_group_id',
                         'age_start', 'age_end', 'sex_id',
                         'year_start', 'year_end',
                         'mean', 'upper', 'lower']]

        env_df.to_csv(FILEPATH.\
                      format(version_id, today),
                      index=False)
    return(env_df)


def stgpr_formatter(fpath, write_both_envelopes):
    """
    Params
        fpath: (str) - a directory containing the STGPR outputs by location in CSV format
    """
    today = datetime.datetime.today().strftime("%Y_%m_%d")  # YYYY-MM-DD

    files = glob.glob(fpath + "*.csv")
    if not files:
        print("There aren't any CSVs in this dir, ending program")
        assert False

    # concat together
    df = pd.concat([pd.read_csv(f) for f in files])

    model_id = fpath.split("/")[6]

    # create list of draw columns to subset and run operations on
    draw_cols = df.filter(regex='draw_').columns.tolist()
    assert len(draw_cols) == 1000,\
    "There must be 1000 draws in the interpolated envelope"

    # Take the mean of all the draws as per Ryan 12/27/2016
    df['mean'] = df[draw_cols].mean(axis=1)

    # Add uncertainty, find the 2.5th percentile and 97.5 percentile
    # calculate quantiles for every row, then transpose and col bind
    print("Calculating percentiles...")
    quant = df[draw_cols].quantile([0.025, 0.975], axis=1).transpose()
    # rename columns
    quant.columns = ['lower', 'upper']
    # col bind quant to env
    df = pd.concat([df, quant], axis=1)

    # MERGE AGES ONTO ENVELOPE
    # we want the envelope to have age_start and age_end instead of
    # age_group_id, so import table with age_groups
    print("Merging ages onto Envelope...")
    age_group = query("select age_group_id,\
                      age_group_years_start, age_group_years_end\
                      from age_group", conn_def='shared')

    # database has age end in format of 10, 15, 20, but should be 9, 14, 19
    age_group.loc[age_group['age_group_years_end'] > 1, 'age_group_years_end']\
        = age_group.\
        loc[age_group['age_group_years_end'] > 1, 'age_group_years_end'] - 1

    # merge age_start and age_end on
    shape_before = df.shape[0]
    df = df.merge(age_group[['age_group_id', 'age_group_years_start',
                                     'age_group_years_end']], how='left',
                          on='age_group_id')
    if shape_before != df.shape[0]:
        print("Rows changes while merging ages!!!!!!")
    assert df['age_group_years_start'].isnull().sum() == 0,\
    "The merge did not properly attach age_start"
    assert df['age_group_years_end'].isnull().sum() == 0,\
    "The merge did not properly attach age_end"

    # rename envelope age start and age end columns
    df.rename(columns={"age_group_years_end": "age_end",
                           "age_group_years_start": "age_start"}, inplace=True)
    # make year_start and year_end
    df['year_start'] = df['year_id']
    df['year_end'] = df['year_id']
    df.drop('year_id', axis=1, inplace=True)

    # assert mean smaller than upper and larger than lower
    assert (df['upper'] > df['mean']).sum() == df.shape[0],\
    "Upper estimate is not always larger than mean"
    assert (df['mean'] > df['lower']).sum() == df.shape[0],\
    "Mean estimate is not always larger than lower"

    if write_both_envelopes:
        test_stgpr(df, test_level='draw')
        df.to_hdf(FILEPATH.format(model_id, today),
                  key='df', complib='blosc', complevel=5, mode='w')

        # keep only needed columns
        df = df[['location_id', 'age_group_id',
                 'age_start', 'age_end', 'sex_id',
                 'year_start', 'year_end',
                 'mean', 'upper', 'lower']]

        test_stgpr(df, test_level='agg')
        df.to_csv(FILEPATH.format(model_id, today),
                  index=False)

    return df


def test_stgpr(df, test_level):
    """
    compare the column names and values of the stgpr envelope to the dismod envelope
    Params
        df (pd DataFrame):
            the stgpr envelope concatted together from a dir of CSVs
        test_level (str):
            are we comparing draw level or aggregated envelopes
    """
    # the import part here is that the envelope demographic values match our hospital data
    # read in condensed demo info
    write_vers = pd.read_csv(root + r"FILEPATH)
    write_vers = int(write_vers['version'].max()) + 1
    hosp_demo = pd.read_csv(\
        FILEPATH.format(write_vers))
    if 'year_id' in hosp_demo.columns:
        hosp_demo['year_start'], hosp_demo['year_end'] = hosp_demo['year_id'], hosp_demo['year_id']
        hosp_demo.drop('year_id', axis=1, inplace=True)

    # use these two dataframes of the envelope to check our stgpr results against
    draw_df = FILEPATH
    draw_df = pd.read_hdf(draw_df, start=0, stop=10)
    # there are some cols we don't want, drop them
    draw_df.drop(['n_draws', 'modelable_entity_id', 'measure_id'], axis=1, inplace=True)

    test_df = FILEPATH
    test_df = pd.read_csv(test_df)

    # checking column names
    if test_level == "draw":
        chk_cols = draw_df.columns
    if test_level == "agg":
        chk_cols = test_df.columns
    col_diff = set(df.columns).symmetric_difference(set(chk_cols))
    assert not col_diff,\
        "Columns do not match, the diff is {}".format(col_diff)
    print("Column names are good")

    # checking locations, ages, sexes, years
    chk_cols = ['location_id', 'age_group_id', 'age_start', 'age_end', 'year_start', 'year_end', 'sex_id']
    for col in chk_cols:
        diff_vals = set(df[col].unique()).symmetric_difference(set(test_df[col].unique()))
        if col not in ['age_start', 'age_end']:
            hosp_col = hosp_demo[col].unique()
            diff_vals = [v for v in diff_vals if v in hosp_col]
        if diff_vals:
            warnings.warn("column {} has these differences {}".format(col, diff_vals))

        # just use print statements while developing
        # assert not diff_vals, "column {} has these differences {}".format(col, diff_vals)


if __name__ == '__main__':

    # this should be universal
    write_input = eval(input("Should the envelope be written to FILEPATH, yes or no?"))

    if write_input in ['yes', 'Yes', 'y', 'YES']:
        write_input = True
    else:
        write_input = False

    # ask to use st gpr or dismod
    model_type = eval(input("Do you want to create the envelope from stgpr or dismod? enter 'stgpr' or 'dismod'"))

    if model_type == 'dismod':
        v_input = eval(input("Enter the envelope DisMod version_id to interpolate"))
        df = run_envelope_interp(write_both_envelopes=write_input, version_id=v_input)

    elif model_type == 'stgpr':
        stgpr_path = eval(input("enter the directory for the stgpr model at the draw level with individual years"))
        df = stgpr_formatter(fpath=stgpr_path, write_both_envelopes=write_input)
