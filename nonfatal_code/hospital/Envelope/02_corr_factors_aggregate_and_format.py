"""
Script to apply the hospital envelope.
"""

import platform
import getpass
import pandas as pd
import numpy as np
import sys
import warnings
import datetime
import re
from db_tools.ezfuncs import query
from db_queries import get_population, get_cause_metadata

# load our functions
if getpass.getuser() == 'USERNAME':
    USERNAME_path = "FILEPATH"
    sys.path.append(USERNAME_path)

import hosp_prep

if platform.system() == "Linux":
    root = "FILEPATH"
else:
    root = "FILEPATH"


def prepare_correction_factors(df):
    """ applies the correction factors created with market scan data
    to the applied envelope (cause fraction * mean) values
    """

    # merge on bundle id
    maps = pd.read_csv("FILEPATH")
    maps = maps[['nonfatal_cause_name', 'bundle_id']].copy()
    maps = maps.drop_duplicates()

    # this merge duplicates data as we expect. b/c a single baby sequela goes
    # to multiple bundle IDs+
    df = df.merge(maps, how='left', on='nonfatal_cause_name')

    # Drop stuff without a bundle id
    warnings.warn("""
                  ==============================================================
                  ============= DROPPING ROWS WITH NULL BUNDLE IDs =============
                  ==============================================================
                  There are {} rows of data in total
                  There are {} rows that did not have a bundle_id
                  """.format(df.shape[0], df[df.bundle_id.isnull()].shape[0]))

    # drop
    df = df[df.bundle_id.notnull()]

    correction_factors = pd.read_csv("FILEPATH")

    correction_factors.drop("outpatient", axis=1, inplace=True)

    # rename columns to match df
    correction_factors.rename(columns={'sex': 'sex_id'}, inplace=True)

    # Read in clean maps to merge measure onto data
    # need this for ELMO reqs
    clean_maps = pd.read_csv("FILEPATH")
    clean_maps = clean_maps[['bundle_id', 'bid_measure']]
    clean_maps.drop_duplicates(inplace=True)
    clean_maps.rename(columns={'bid_measure': 'measure'}, inplace=True)

    # remove null bundle ids in map
    clean_maps = clean_maps[clean_maps.bundle_id.notnull()]

    # check to make sure rows aren't duplicated
    pre_shape = df.shape[0]
    # merge measure onto data
    df = df.merge(clean_maps, how='left', on='bundle_id')

    # now that we have measure, merge corr factors onto data
    # this is really just for diagnostic purposes.  We need to reattach later
    # after aggregating to 5 year dismod bands.
    df = df.merge(correction_factors, how='left', on=['age_start', 'sex_id',
                  'bundle_id'])
    assert pre_shape == df.shape[0] , ("You unexpectedly added rows while "
        "merging on the correction factors. Don't do that!")

    df.update(df[[u'indv_cf', 'incidence', 'prevalence', 'injury_cf']].fillna(1))

    # rename correction factors to match what we told people they would be
    # There is no 'correction_factor_0', because 'mean_0' is uncorrected data.
    # 'correction_factor_1' is what makes 'mean_1' and is applied to 'mean_0'
    # 'correction_factor_2' is what makes 'mean_2' and is applied to 'mean_0'
    # 'correction_factor_3' is what makes 'mean_3' and is applied to 'mean_0'
    # the same relationship holds for upper and lower
    df.rename(columns={'indv_cf': 'correction_factor_1',
                       'incidence': 'correction_factor_2',
                       'prevalence': 'correction_factor_3',
                       'injury_cf': 'correction_factor_inj'},
                       inplace=True)
    # rename mean, upper, and lower to match what we told people they would be
    df.rename(columns={'product':'mean_0',
                       'upper_product': 'upper_0',
                       'lower_product': 'lower_0'},
                       inplace=True)

    # make mean_1, lower_1, upper_1
    df['mean_1'] = df.correction_factor_1 * df.mean_0
    df['lower_1'] = df.correction_factor_1 * df.lower_0
    df['upper_1'] = df.correction_factor_1 * df.upper_0

    # make mean_2, lower_2, upper_2
    df['mean_2'] = df.correction_factor_2 * df.mean_0
    df['lower_2'] = df.correction_factor_2 * df.lower_0
    df['upper_2'] = df.correction_factor_2 * df.upper_0

    # make mean_3, lower_3, upper_3
    df['mean_3'] = df.correction_factor_3 * df.mean_0
    df['lower_3'] = df.correction_factor_3 * df.lower_0
    df['upper_3'] = df.correction_factor_3 * df.upper_0

    # make injury mean, lower, upper
    df['mean_inj'] = df.correction_factor_inj * df.mean_0
    df['lower_inj'] = df.correction_factor_inj * df.lower_0
    df['upper_inj'] = df.correction_factor_inj * df.upper_0

    def factor_applier(df, levels=["1", "2", "3", "inj"]):
        for level in levels:
            # initialize cols
            df['test_mean_' + level] = np.nan
            df['test_lower_' + level] = np.nan
            df['test_upper_' + level] = np.nan
            # apply corr factors
            df['test_mean_' + level], df['test_lower_' + level], df['test_upper_' + level] = df["correction_factor_" + level] * df['mean_0'], df["correction_factor_" + level] * df['lower_0'], df["correction_factor_" + level] * df['upper_0']
        return(df)
    df = factor_applier(df)

    levels=["1", "2", "3", "inj"]
    for level in levels:
        assert (df.loc[df["mean_" + level].notnull(), "mean_" + level] ==\
            df.loc[df["test_mean_" + level].notnull(), "test_mean_" + level]).\
            all(), ("different on level {}".format(level))
        assert (df.loc[df["upper_" + level].notnull(), "upper_" + level] ==\
            df.loc[df["test_upper_" + level].notnull(), "test_upper_" + level]).\
            all(), ("different on level {}".format(level))
        assert (df.loc[df["lower_" + level].notnull(), "lower_" + level] ==\
            df.loc[df["test_lower_" + level].notnull(), "test_lower_" + level]).\
            all(), ("different on level {}".format(level))
    # drop test cols for now until we run this for awhile without
    # tripping the assert
    test_cols = df.columns[df.columns.str.startswith("test_")]
    df.drop(test_cols, axis=1, inplace=True)


    return(df)


def get_parent_injuries(df):
    """
    roll up the child causes into parents add parent injuries
    """

    pc_injuries = pd.read_csv("FILEPATH")
    # take only parent causes from map
    # pc_injuries = pc_injuries[pc_injuries.parent==1]
    # create BID and ME ID dicts for parent causes
    me_dict = dict(zip(pc_injuries.e_code, pc_injuries['level1_meid']))
    bundle_dict = dict(zip(pc_injuries.e_code, pc_injuries['Level1-Bundle ID']))

    # loop over ecodes pulling every parent name
    df_list = []
    for parent in pc_injuries.loc[pc_injuries['parent']==1, 'e_code']:
       inj_df =\
        pc_injuries[(pc_injuries['baby sequela'].str.contains(parent)) &\
        (pc_injuries['parent'] != 1)]
       inj_df = inj_df[inj_df['Level1-Bundle ID'].notnull()]
       inj_df['me_id'] = me_dict[parent]
       inj_df['bundle_id'] = bundle_dict[parent]
       # inj_df['nonfatal_cause_name'] = parent
       df_list.append(inj_df)

    parent_df = pd.concat(df_list)

    # number of child injuries in csv should match new df
    assert pc_injuries.child.sum() == parent_df.child.sum(),\
        "sum of child causes doesn't match sum of parent causes"

    parent_df.drop(['baby sequela', 'ME name level 1', 'level 1 measure',
                    'e_code', 'parent', 'child'], axis=1,
                    inplace=True)

    parent_df.rename(columns={'me_id': 'parent_me_id',
                              'level1_meid': 'modelable_entity_id',# child me_id
                              'bundle_id': 'parent_bundle_id',
                              'Level1-Bundle ID': 'bundle_id'},# child bundle_id
                     inplace=True)

    # reorder cuz it's hard to look at
    col_before = parent_df.columns
    parent_df = parent_df[['modelable_entity_id', 'parent_me_id', 'bundle_id',
                           'parent_bundle_id']].copy()
    assert set(col_before) == set(parent_df.columns),\
        'you dropped a column while reordering columns'

    # duplicate data for parent injuries
    parent_df = parent_df.merge(df, how='left', on='bundle_id')

    # drop child ME and bundle IDs
    parent_df.drop(['modelable_entity_id', 'bundle_id'], axis=1, inplace=True)
    # add parent bundle and ME IDs
    parent_df.rename(columns={'parent_me_id': 'modelable_entity_id',
                              'parent_bundle_id': 'bundle_id'}, inplace=True)

    parent_df.drop('modelable_entity_id', axis=1, inplace=True)

    assert set(parent_df.columns).symmetric_difference(set(df.columns)) == set()

    groups = ['location_id', 'year_start', 'year_end',
              'age_start', 'age_end', 'sex_id', 'nid',
              'representative_id',
              'bundle_id']
    # sum parent rows of data together
    parent_df = parent_df.groupby(groups).agg({'mean_0': 'sum',
                                 'upper_0': 'sum',
                                 'lower_0': 'sum'}).reset_index()

    # rbind duped parent data back onto hospital data
    df = pd.concat([df, parent_df]).reset_index(drop=True)
    return(df)


def aggregate_to_bundle(df, write_maternal_denom=False,
                        adjust_maternal_denom=False):
    """
    Takes a dataframe aggregated to baby sequela level and returns
    the df aggregated to 5 year bands at the bundle level
    """
    ##############################
    # PULL POPULATION
    #
    # This is preparation for aggregating to 5 year bands
    ##############################

    # get rid of all corrected values and correction factors. we want to do
    # this because we are going to aggregate to 5 year bands and the bundle_id
    # level of analysis. We intend to makes cells where the correction factor
    # is over 50 NULL. it doesn't make sense to do that before aggregating,
    # because any rows with nulls in them would be lost entirely.  It doesn't
    # make sense to apply corrections and NOT delete cells that break the
    # over-50 rule before aggregation either cuz we would lose the correction
    # factors during the groupby. Also, it would not be robust to aggregate
    # a 16 columns. Therefore, it's best to apply corrections after aggregating.

    # Technically, the groupby would get rid of these for us. Honestly, this
    # just makes things easier to work with up to that point.
    df.drop(['mean_1', 'mean_2', 'mean_3', 'mean_inj',
             'upper_1', 'upper_2', 'upper_3', 'upper_inj',
             'lower_1', 'lower_2', 'lower_3', 'lower_inj',
             'correction_factor_1', 'correction_factor_2',
             'correction_factor_3', 'correction_factor_inj'],
            axis=1, inplace=True)

    if write_maternal_denom:
        df = df[df['bundle_id'] == 1010]
    # drop the non maternal data pronto
    if adjust_maternal_denom:
        # GET MATERNAL CAUSES
        causes = get_cause_metadata(cause_set_id=9)
        condition = causes.path_to_top_parent.str.contains("366")  # 366 happens
        # to always be in the third level
        # subset just causes that meet the condition sdf
        maternal_causes = causes[condition]
        # make list of maternal causes
        maternal_list = list(maternal_causes['cause_id'].unique())
        # get bundle to cause map
        bundle_cause = query("QUERY")
        # merge cause_id onto data
        df = df.merge(bundle_cause, how='left', on='bundle_id')
        # keep only maternal causes
        df = df[df['cause_id'].isin(maternal_list)]
        # drop cause_id
        df.drop('cause_id', axis=1, inplace=True)
        # drop the denominator bundle
        df = df[df['bundle_id'] != 1010]
    # pull age_group to age_start/age_end map

    ##############################
    # GET POPULATION
    ##############################
    age_group = query("QUERY")
    # correct age groups
    age_group.loc[age_group['age_group_years_end'] > 1, 'age_group_years_end']\
    = age_group.\
    loc[age_group['age_group_years_end'] > 1, 'age_group_years_end'] - 1

    # df of unique ages from hospital data to merge onto age_group map
    df_ages = pd.DataFrame([df.age_start.unique(), df.age_end.unique()]).transpose()
    df_ages.columns = ['age_group_years_start', 'age_group_years_end']
    df_ages = df_ages.merge(age_group, how='left', on=['age_group_years_start',
                                                       'age_group_years_end'])

    # this is the correct terminal age group (even though we use max age = 99)
    df_ages.loc[df_ages['age_group_years_start'] == 95, 'age_group_id'] = 235

    # there are two age_group_ids for age_start=0 and age_start=1
    df_ages = df_ages[df_ages.age_group_id != 161]

    # create age/year/location lists to use for pulling population
    age_list = list(df_ages.age_group_id.unique())
    loc_list = list(df.location_id.unique())
    year_list = list(df.year_start.unique())

    # pull population and merge on age_start and age_end
    pop = get_population(age_group_id=age_list, location_id=loc_list,
                         sex_id=[1, 2], year_id=year_list)

    # attach age_start and age_end to population information
    pop = pop.merge(age_group, how='left', on='age_group_id')
    pop.drop(['process_version_map_id', 'age_group_id'], axis=1, inplace=True)

    # rename pop columns to match hospital data columns
    pop.rename(columns={'age_group_years_start': 'age_start',
                        'age_group_years_end': 'age_end',
                        'year_id': 'year_start'}, inplace=True)
    pop['year_end'] = pop['year_start']

    # correct terminal age group to match our data
    pop.loc[pop['age_end'] == 124, 'age_end'] = 99

    demography = ['location_id', 'year_start', 'year_end', 'age_start',
                  'age_end', 'sex_id']

    ##############################
    # MAKE DATA SQUARE
    ##############################

    # create a series of sorted, non-zero mean values to make sure
    # the func doesn't alter anything
    check_mean = df.loc[df['mean_0'] > 0, 'mean_0'].sort_values().\
        reset_index(drop=True)

    print("Starting number of rows: {}".format(df.shape[0]))
    # square the dataset
    df = hosp_prep.make_zeroes(df, level_of_analysis='bundle_id',
            cols_to_square=['mean_0', 'upper_0', 'lower_0'],
            icd_len=5)
    # assert the sorted means are identical
    assert (check_mean == df.loc[df['mean_0'] > 0, 'mean_0'].sort_values().\
        reset_index(drop=True)).all()


    # delete rows where restrictions should be applied
    # create df where baby sequela are missing
    missing_nfc = df[df.nonfatal_cause_name.isnull()].copy()
    df = df[df.nonfatal_cause_name.notnull()]
    for col in ['mean_0', 'upper_0', 'lower_0']:
        df = hosp_prep.apply_restrictions(df, col)
        missing_nfc = hosp_prep.apply_bundle_restrictions(missing_nfc, col)
        df = df[df[col].notnull()]

    df = pd.concat([df, missing_nfc])
    print("Square number of rows: {}".format(df.shape[0]))
    # don't create the parent injury dupes until after the data is totally
    # square so that the denominators will match
    # df = get_parent_injuries(df)

    # check_parent_injuries(df, 'mean_0')

    pre_shape = df.shape[0]  # store for before comparison
    # then merge population onto the hospital data

    df = df.merge(pop, how='left', on=demography)  # attach pop info to hosp
    assert pre_shape == df.shape[0], "number of rows don't match after merge"

    ##############################
    # RATE SPACE TO COUNTS
    ##############################

    # go from rate space to additive counts
    # because of careful merging we just need to multiply across columns
    df['hosp_count'] = df['mean_0'] * df['population']
    df['upper_hosp_count'] = df['upper_0'] * df['population']
    df['lower_hosp_count'] = df['lower_0'] * df['population']

    # merge on "denominator" from file that was made back in
    # create_cause_fractions. This adds the denominator column, which is the
    # number of admissions in a demographic group
    df = df.merge(pd.read_csv("FILEPATH"),
                  how='left', on=["age_start", "age_end", "sex_id",
                                  "year_start", "year_end", "location_id"])


    # 5 year bins
    df = hosp_prep.year_binner(df)

    # add 5 year NIDs onto data
    df = hosp_prep.five_year_nids(df)


    ##################################################################
    ##################################################################
    #                         THE COLLAPSE                           #
    ##################################################################
    ##################################################################
    # The final collapse to 5 year bands, 5 year nids and bundle ID
    # this is doing a few things at once.  One is that we need to aggregate
    # to 5 year bands.  Another is aggregating to the Bundle_id level of
    # analysis.  Up to this point we were at the nonfatal_cause_name AKA
    # baby sequelae level of analysis.

    # WE NEED TWO COLLAPSES. One for data that doesn't have a sample_size value,
    # and annother for the data that does.  This is because:
    # https://goo.gl/e66OZ4 and https://goo.gl/Fb78xi

    # make df of data where there is full coverage (i.e., the UK)
    full_coverage_sources = ["UK_HOSPITAL_STATISTICS"]

    # make condition mask that indicates rows that have full coverage
    has_full_coverage = df.source.isin(full_coverage_sources)

    covered_df = df[has_full_coverage]

    # drop "denominator" from covered_df
    covered_df = covered_df.drop("denominator", axis=1)

    # drop this data from the main dataframe
    df = df[~has_full_coverage]

    assert (df.loc[df.denominator.isnull(), 'mean_0'] == 0).all(), ("mean_0"
        " should be 0")
    assert (df.loc[df.denominator.isnull(), 'lower_0'] == 0).all(), ("lower_0"
        " should be 0")
    assert (df.loc[df.denominator.isnull(), 'upper_0'] == 0).all(), ("upper_0"
        " should be 0")

    df = df[df.denominator.notnull()]

    # df already has sample size
    df.drop("sample_size", axis=1, inplace=True)
    # check if cases are lost in the groupby

    # rename "denominator" to "sample_size" in df (not covered_df)
    df.rename(columns={"denominator": "sample_size"}, inplace=True)

    pre_cases = df['hosp_count'].sum()
    # can use the same group columns for both dataframes
    groups = ['location_id', 'year_start', 'year_end',
              'age_start', 'age_end', 'sex_id', 'nid',
              'representative_id',
              'bundle_id']

    # sample_size has some null values from being made square, but it was just
    # population, so we're using pop instead. so remember,
    # population == sample_size for covered_df
    covered_df = covered_df.groupby(groups)\
        .agg({'hosp_count':'sum',
              'population': 'sum'}).reset_index()


    # add "sample_size" to the aggregate function
    df = df.groupby(groups).agg({'hosp_count': 'sum',
                                 'upper_hosp_count': 'sum',
                                 'lower_hosp_count': 'sum',
                                 'population': 'sum',
                                 'sample_size': 'sum'}).reset_index()
    assert round(pre_cases, 0) == round(df['hosp_count'].sum(), 0),\
        ("some cases were lost. "
         "From {} to {}".format(pre_cases, df['hosp_count'].sum()))

    # set sample size to np.nan when mean/upper/lower are greater than 0
    df.loc[(df['hosp_count'] > 0) & (df['lower_hosp_count'] > 0) & (df['upper_hosp_count'] > 0), 'sample_size'] = np.nan
    ##############################
    # COUNTS TO RATE SPACE
    ##############################
    # REMAKE mean and uncertainty
    # for the main df:

    df['mean_0'] = df['hosp_count'] / df['population']
    df['lower_0'] = df['lower_hosp_count'] / df['population']
    df['upper_0'] = df['upper_hosp_count'] / df['population']
    df.drop(['hosp_count', 'lower_hosp_count', 'upper_hosp_count'], axis=1,
            inplace=True)

    # add parent injuries
    # NOTE get parent injuries is ran before covered_df is concated with df.
    # it happens to not make a difference at the moment, because there are not
    # injuries in covered_df, but it could in the future.
    df = get_parent_injuries(df)
    for col in ['mean_0', 'lower_0', 'upper_0']:
        hosp_prep.check_parent_injuries(df, col_to_sum=col)

    # this drops the population that was merged on for coverting to counts.
    df.drop('population', axis=1, inplace=True)  # don't need pop anymore

    # for the covered df:
    covered_df['mean_0'] = covered_df['hosp_count'] / covered_df['population']

    covered_df.rename(columns={"population": "sample_size"}, inplace=True)

    # drop columns
    covered_df.drop(['hosp_count'], axis=1, inplace=True)

    ###############################
    # RE-ATTACH
    ###############################
    # bring covered_df and df together.
    # where we have full coverage, lower and upper should be null
    # mean_0 will never be null

    df = pd.concat([df, covered_df], ignore_index=True)

    # assert what we just said will be true in the comments above:
    assert df.loc[has_full_coverage, 'lower_0'].isnull().all(), ("where we have"
        " full coverage, lower_0 should be null")
    assert df.loc[has_full_coverage, 'upper_0'].isnull().all(), ("where we have"
        " full coverage, upper_0 should be null")
    assert df.mean_0.notnull().all(), ("mean_0 should never be null")

    # NOTE, remember, sample size will still have null values, and that's okay
    # we need to keep sample size from here on out.

    if "population" in df.columns:
        print "population was still in columns"
        df.drop("population", axis=1, inplace=True)


    ########################################

    # map measure onto data, just need this for ELMO reqs.
    clean_maps = pd.read_csv("FILEPATH")
    clean_maps = clean_maps[['bundle_id', 'bid_measure']]
    clean_maps.drop_duplicates(inplace=True)
    clean_maps.rename(columns={'bid_measure': 'measure'}, inplace=True)
    # remove null bundles from map
    clean_maps = clean_maps[clean_maps.bundle_id.notnull()]

    pre_shape = df.shape[0]  # store for comparison after merge

    # merge measure onto hosp data using bundle_id
    df = df.merge(clean_maps, how='left', on='bundle_id')
    assert pre_shape == df.shape[0], "number of rows don't match after merge."

    # get injuries bids so we can check for missing measures
    pc_injuries = pd.read_csv("FILEPATH")
    inj_bids = pc_injuries['Level1-Bundle ID'].unique()

    # some injuries bids didn't get measures!
    assert set(df[df.measure.isnull()].bundle_id).issubset(set(inj_bids)),\
        ("We expect that all null measures belong to injuries, but that is"
         "not the case. Something went wrong!")

    # fix any injuries that are missing measure, all inj are inc:
    df.loc[(df.measure.isnull())&(df.bundle_id.isin(inj_bids)), 'measure'] = 'inc'

    assert df.measure.isnull().sum() == 0, ("There are null values and we "
        "expect none")

    # read in correction factors (again)
    correction_factors = pd.read_csv("FILEPATH")

    correction_factors.drop("outpatient", axis=1, inplace=True)

    # rename columns to match df
    correction_factors.rename(columns={'sex': 'sex_id'}, inplace=True)

    # merge corr factors onto data
    df = df.merge(correction_factors, how='left', on=['age_start', 'sex_id',
                  'bundle_id'])
    assert pre_shape == df.shape[0] , ("You unexpectedly added rows while "
        "merging on the correction factors. Don't do that!")

    # if a Bundle ID doesn't have a corr factor from marketscan use 1
    # df.update(df[['a','b','c']].fillna(0))  # test code
    # http://stackoverflow.com/questions/36556256/how-do-i-fill-na-values-in-multiple-columns-in-pandas
    df.update(df[['indv_cf', 'incidence', 'prevalence', 'injury_cf']].fillna(1))

    # rename correction factors to match what we told people they would be
    df.rename(columns={'indv_cf': 'correction_factor_1',
                       'incidence': 'correction_factor_2',
                       'prevalence': 'correction_factor_3',
                       'injury_cf': 'correction_factor_inj'},
                       inplace=True)

    # NOTE we apply every correction factor to all data, even if it is not
    # relevant.  E.g., not all data is injuries, so not all data needs
    # correction_factor_inj.  It simply easier to apply all of them, and then
    # while writing to modeler's folders, drop the irrelevant columns.

    # make mean_1, lower_1, upper_1
    df['mean_1'] = df.correction_factor_1 * df.mean_0
    df['lower_1'] = df.correction_factor_1 * df.lower_0
    df['upper_1'] = df.correction_factor_1 * df.upper_0

    # make mean_2, lower_2, upper_2
    df['mean_2'] = df.correction_factor_2 * df.mean_0
    df['lower_2'] = df.correction_factor_2 * df.lower_0
    df['upper_2'] = df.correction_factor_2 * df.upper_0

    # make mean_3, lower_3, upper_3
    df['mean_3'] = df.correction_factor_3 * df.mean_0
    df['lower_3'] = df.correction_factor_3 * df.lower_0
    df['upper_3'] = df.correction_factor_3 * df.upper_0

    # make injury mean, lower, upper
    df['mean_inj'] = df.correction_factor_inj * df.mean_0
    df['lower_inj'] = df.correction_factor_inj * df.lower_0
    df['upper_inj'] = df.correction_factor_inj * df.upper_0

    # assert what we just said will be true in the comments above:
    levels=["1", "2", "3", "inj"]
    for level in levels:
        assert df.loc[has_full_coverage, 'lower_{}'.format(level)].isnull().all(), ("broke on level {}".format(level))
        assert df.loc[has_full_coverage, 'upper_{}'.format(level)].isnull().all(), ("broke on level {}".format(level))
        assert df["mean_{}".format(level)].notnull().all(), ("broke on level {}".format(level))

    def factor_applier(df, levels=["1", "2", "3", "inj"]):
        for level in levels:
            # initialize cols
            df['test_mean_' + level] = np.nan
            df['test_lower_' + level] = np.nan
            df['test_upper_' + level] = np.nan
            # apply corr factors
            df['test_mean_' + level], df['test_lower_' + level], df['test_upper_' + level] = df["correction_factor_" + level] * df['mean_0'], df["correction_factor_" + level] * df['lower_0'], df["correction_factor_" + level] * df['upper_0']
        return(df)
    df = factor_applier(df)

    levels=["1", "2", "3", "inj"]
    for level in levels:
        assert (df.loc[df["mean_" + level].notnull(), "mean_" + level] == df.loc[df["test_mean_" + level].notnull(), "test_mean_" + level]).all(), ("different on level {}".format(level))
        assert (df.loc[df["upper_" + level].notnull(), "upper_" + level] == df.loc[df["test_upper_" + level].notnull(), "test_upper_" + level]).all(), ("different on level {}".format(level))
        assert (df.loc[df["lower_" + level].notnull(), "lower_" + level] == df.loc[df["test_lower_" + level].notnull(), "test_lower_" + level]).all(), ("different on level {}".format(level))
    # drop test cols for now until we run this for awhile without
    # tripping the assert
    test_cols = df.columns[df.columns.str.startswith("test_")]
    df.drop(test_cols, axis=1, inplace=True)

    # RULE = if correction factor is greater than 50, make the data null
    # EXCEPTIONS are made for these bundles, which are capped at 100:
        # Preterm: 80, 81, 82, 500
        # Encephalopathy: 338
        # Sepsis: 92
        # Hemoloytic: 458
        # PAD/PUD: 345
        # Cirrhosis: 131

    # list of bundles which can have correction factors above 50
    cf_exceptions = [345, 80, 81, 82, 500, 338, 92, 458, 131]

    # NOTE when checking the number of nulls, consider the nulls that are caused
    # by the sample_size split

    def mean_capper(df, exceptions, levels=["1", "2", "3"]):
        exception_condition = df.bundle_id.isin(cf_exceptions)
        for level in levels:
            df.loc[(~exception_condition) & (df['correction_factor_' + level] > 50), ['mean_' + level, 'lower_' + level, 'upper_' + level, ]] = np.nan
            df.loc[(exception_condition) & (df['correction_factor_' + level] > 100), ['mean_' + level, 'lower_' + level, 'upper_' + level, ]] = np.nan

        return(df)
    # create df to test function
    df_test_capper = mean_capper(df, cf_exceptions)


    exception_condition = df.bundle_id.isin(cf_exceptions)  # make boolean mask that says if a bundle is in the list

    df.loc[(~exception_condition)&(df.correction_factor_1 > 50), ['mean_1', 'lower_1', 'upper_1']] = [np.nan, np.nan, np.nan]  # DOESN'T DO ANYTHING don't really need to apply here
    df.loc[(~exception_condition)&(df.correction_factor_2 > 50), ['mean_2', 'lower_2', 'upper_2']] = [np.nan, np.nan, np.nan]  # DID DO SOMETHING, affects 6% of prev rows, 0.01% of inc rows
    df.loc[(~exception_condition)&(df.correction_factor_3 > 50), ['mean_3', 'lower_3', 'upper_3']] = [np.nan, np.nan, np.nan]  # DOES A LOT, affects 57% percent of prevalence rows

    df.loc[(exception_condition)&(df.correction_factor_1 > 100), ['mean_1', 'lower_1', 'upper_1']] = [np.nan, np.nan, np.nan]
    df.loc[(exception_condition)&(df.correction_factor_2 > 100), ['mean_2', 'lower_2', 'upper_2']] = [np.nan, np.nan, np.nan]
    df.loc[(exception_condition)&(df.correction_factor_3 > 100), ['mean_3', 'lower_3', 'upper_3']] = [np.nan, np.nan, np.nan]


    #####################################################
    # CHECK that lower < mean < upper
    #####################################################
    # loop over every level of correction
    # can't compare null values, null comparisons always eval to False
    for i in ["0", "1", "2", "3", 'inj']:
        # lower < mean
        assert (df.loc[df['lower_'+i].notnull(), 'lower_'+i] <=
                df.loc[df["lower_"+i].notnull(), 'mean_'+i]).all(),\
            "lower_{} should be less than mean_{}".format(i, i)
        # mean < lower
        assert (df.loc[df["upper_"+i].notnull(), 'mean_'+i] <=
                df.loc[df["upper_"+i].notnull(), 'upper_'+i]).all(),\
            "mean_{} should be less than upper_{}".format(i, i)

    # compare the results between test df and proper df
    for uncertainty in ["mean", "upper", "lower"]:
        for level in ["1", "2", "3"]:
            # compare the sum of nulls rows between dfs
            assert df[uncertainty + "_" + level].isnull().sum() ==\
                df_test_capper[uncertainty + "_" + level].isnull().sum(),\
                "The new capping function is producing different results"

    # write the maternal denominator data, this is for the future when we work
    # in parallel
    if write_maternal_denom:
        def write_maternal_denom(df):
            mat_df = df[df.bundle_id==1010].copy()
            mat_df = mat_df.query("sex_id == 2 & age_start >=10 & age_end <=54")

            if mat_df.shape[0] == 0:
                return
            # NOTE sample size is dropped here, and we make a new one in the
            # following code
            mat_df = mat_df[['location_id', 'year_start', 'year_end',
                             'age_start', 'age_end', 'sex_id',
                             'mean_0', 'mean_1', 'mean_2', 'mean_3']].copy()

            bounds = ['upper_0', 'upper_1', 'upper_2', 'upper_3',
                        'lower_0', 'lower_1', 'lower_2', 'lower_3']
            for uncertainty in bounds:
                mat_df[uncertainty] = np.nan

            # PREP FOR POP ####################################################
            # we don't have years that we can merge on pop to yet, because
            # we aggregated to year bands; another problem with this method
            mat_df['year_id'] = mat_df.year_start + 2  # makes 2000,2005,2010

            # bunch of age things so we can use age_group_id to get pop
            age_group = query("QUERY")
            # correct age groups
            age_group.loc[age_group['age_group_years_end'] > 1,
                          'age_group_years_end'] =\
                age_group.loc[age_group['age_group_years_end'] > 1,
                              'age_group_years_end'] - 1

            # df of unique ages from hospital data to merge onto age_group map
            mat_df_ages = pd.DataFrame([mat_df.age_start.unique(),
                                       mat_df.age_end.unique()]).transpose()
            mat_df_ages.columns = ['age_group_years_start',
                                   'age_group_years_end']
            mat_df_ages = mat_df_ages.merge(age_group, how='left',
                                            on=['age_group_years_start',
                                                'age_group_years_end'])

            # this is the correct terminal age group (even though we use max
            # age = 99)
            mat_df_ages.loc[mat_df_ages['age_group_years_start'] == 95,
                            'age_group_id'] = 235

            # there are two age_group_ids for age_start=0 and age_start=1
            mat_df_ages = mat_df_ages[mat_df_ages.age_group_id != 161]

            # create age/year/location lists to use for pulling population
            age_list = list(mat_df_ages.age_group_id.unique())
            loc_list = list(mat_df.location_id.unique())
            year_list = list(mat_df.year_id.unique())

            # GET POP ########################################################
            # pull population and merge on age_start and age_end
            pop = get_population(age_group_id=age_list, location_id=loc_list,
                                 sex_id=[1, 2], year_id=year_list)

            # FORMAT POP ####################################################
            # attach age_start and age_end to population information
            pop = pop.merge(age_group, how='left', on='age_group_id')
            pop.drop(['process_version_map_id', 'age_group_id'], axis=1,
                     inplace=True)

            # rename pop columns to match hospital data columns
            pop.rename(columns={'age_group_years_start': 'age_start',
                                'age_group_years_end': 'age_end'}, inplace=True)

            # correct terminal age group to match our data
            pop.loc[pop['age_end'] == 124, 'age_end'] = 99

            # MERGE POP ######################################################
            demography = ['location_id', 'year_id', 'age_start',
                          'age_end', 'sex_id']

            pre_shape = mat_df.shape[0]  # store for before comparison
            # then merge population onto the hospital data

            # attach pop info to hosp
            mat_df = mat_df.merge(pop, how='left', on=demography)
            assert pre_shape == mat_df.shape[0], ("number of rows don't "
                "match after merge")


            # MAKE SAMPLE SIZE  ##############################################
            mat_df['sample_size'] = mat_df.population * mat_df.mean_0

            # DROP intermidiate columns
            mat_df.drop(['population', 'year_id'], axis=1, inplace=True)


            mat_df.to_hdf("FILEPATH", key='df', mode="w")
            # backup copy to _archive
            mat_df.to_hdf("FILEPATH", key='df', mode='w')
        write_maternal_denom(df)


    if adjust_maternal_denom:
        def adjust_maternal_denom(df):

            # drop sample_size, UTLAs already had it, but we need it for
            # everything, so we have to drop it.
            df.drop('sample_size', axis=1, inplace=True)
            df = df.query("sex_id == 2 & age_start >=10 & age_end <=54")

            # read in maternal denoms, this is needed when our process is
            # parallelized
            denom = pd.read_hdf("FILEPATH", key="df")
            # denom.drop('bundle_id', axis=1, inplace=True)
            denom_cols = sorted(denom.filter(regex="[0-9]$").columns)
            for col in denom_cols:
                denom.rename(columns={col: col + "_denominator"}, inplace=True)
            pre = df.shape[0]
            df = df.merge(denom, how='left', on=['location_id', 'year_start',
                                                 'year_end', 'age_start',
                                                 'age_end', 'sex_id'])
            assert pre == df.shape[0], ("shape should not have changed "
                "during merge")
            #print(df[df.mean_0_denominator.isnull()].shape)
            #print(df[df.mean_0_denominator.isnull()])
            df = df[(df['mean_0'] > 0) | (df['mean_0_denominator'].notnull())]

            assert df.mean_0_denominator.isnull().sum() == 0, ("shouldn't be "
                "any null values in this column")
            # regex to find the columns that start with l, m or u and end with
            #  a digit
            num_cols = sorted(df.filter(regex="^[lmu].*[0-9]$").columns)
            denom_cols =\
                sorted(df.columns[df.columns.str.endswith("denominator")])

            # divide each bundle value by bundle 1010 to get the adjusted rate
            for i in np.arange(0, 12, 1):
                df[num_cols[i]] = df[num_cols[i]] / df[denom_cols[i]]
            # drop the denominator columns
            df.drop(denom_cols, axis=1, inplace=True)
            # can't divide by zero
            df = df[df['sample_size'] != 0]
            # RETURN ONLY THE MATERNAL DATA
            return(df)
        df = adjust_maternal_denom(df)

    return(df)

def elmo_formatting(df):

    # rename columns for ELMO reqs
    df.rename(columns={'representative_id': 'representative_name',
                       'denominator': 'sample_size',
                       'sex_id': 'sex'},
              inplace=True)

    # drop any lingering zeros, should only appear in data that we deemed to
    # have full coverage, so that cause fractions and envelope wasn't applied
    # this is so that covered data is consistent with the rest, which had
    # zeros dropped in cause fractions
    # df = df[df.mean_0 != 0]

    # make dictionary for replacing representative id with representative name
    representative_dictionary = {-1: "Not Set",
                                 0: "Unknown",
                                 1: "Nationally representative only",
                                 2: "Representative for subnational " +
                                 "location only",
                                 3: "Not representative",
                                 4: "Nationally and subnationally " +
                                 "representative",
                                 5: "Nationally and urban/rural " +
                                 "representative",
                                 6: "Nationally, subnationally and " +
                                 "urban/rural representative",
                                 7: "Representative for subnational " +
                                 "location and below",
                                 8: "Representative for subnational " +
                                 "location and urban/rural",
                                 9: "Representative for subnational " +
                                 "location, urban/rural and below",
                                 10: "Representative of urban areas only",
                                 11: "Representative of rural areas only"}
    df.replace({'representative_name': representative_dictionary},
               inplace=True)

    # add elmo reqs
    df['source_type'] = 'Facility - inpatient'
    df['urbanicity_type'] = 'Unknown'
    df['recall_type'] = 'Not Set'
    df['unit_type'] = 'Person'
    df['unit_value_as_published'] = 1
    df['cases'] = np.nan
    df['is_outlier'] = 0
    df['sex'].replace([1, 2], ['Male', 'Female'], inplace=True)
    df['measure'].replace(["prev", "inc"], ["prevalence", "incidence"],
                          inplace=True)

    # when fixing the maternal denominators, this column will already
    # exist with important info!
    if 'sample_size' not in df.columns:
        df['sample_size'] = np.nan

    df['seq'] = np.nan  # auto
    df['underlying_nid'] = np.nan  # optional
    df['sampling_type'] = np.nan  # optional
    df['recall_type_value'] = np.nan  # optional
    df['uncertainty_type'] = np.nan  # auto

    # uncertainty_type_value needs to be non null when lower, mean, and upper
    # are not null.  A proxy for this is when sample_size is null.
    # When lower, upper are both null, uncertainty_type_value
    # needs to be null.  A proxy for this is when sample_size is not null.
    df['uncertainty_type_value'] = np.nan  # initialize column
    # fill with 95 where we're using mean, lower, upper
    # otherwise, where sample_size is not null, leave as NaN
    df.loc[df.sample_size.isnull(), 'uncertainty_type_value'] = 95

    df['input_type'] = np.nan
    df['standard_error'] = np.nan  # marked as auto
    df['effective_sample_size'] = np.nan
    df['design_effect'] = np.nan  # marked as optional
    df['response_rate'] = np.nan
    df['extractor'] = "USERNAME and USERNAME"

    # human readable location names
    print "now merging on location_name"
    loc_map = query("QUERY")
    df = df.merge(loc_map, how='left', on='location_id')
    print "done with merge"
    if 'location_name' in df.columns:
        print "confirmed: location_name is in the columns"
    else:
        print "location_name is not in the columns"

    # add bundle_name
    print "now merging on bundle_name"
    bundle_name_df = query("QUERY")
    df = df.merge(bundle_name_df, how="left", on="bundle_id")
    if 'bundle_name' in df.columns:
        print "confirmed: bundle_name is in the columns"
    else:
        print "bundle_name is not in the columns"

    # where mean upper lower ==1, make lower 0.999
    # replace values that would fail epi uploader bound validation tests
    # for prevalence, mean, upper, and lower cannot be greater than one
    cap_cols = ['mean_0', 'lower_0', 'upper_0',
                'mean_1', 'lower_1', 'upper_1',
                'mean_2', 'lower_2', 'upper_2',
                'mean_3', 'lower_3', 'upper_3']
    for col in cap_cols:
        df.loc[(df[col] > 1)&(df['measure'] == "prevalence"), col] = 1
    # df.loc[(df['mean_1'] > 1)&(df['measure'] == "prevalence"), 'mean_1'] = 1
    # df.loc[(df['lower_1'] > 1)&(df['measure'] == "prevalence"), 'lower_1'] = 1
    # df.loc[(df['upper_1'] > 1)&(df['measure'] == "prevalence"), 'upper_1'] = 1


    # for prevalence, mean, upper, and lower cannot be less than zero
    for col in cap_cols:
        df.loc[(df[col] < 0)&(df['measure'] == "prevalence"), col] = 0
    # df.loc[(df['mean'] < 0)&(df['measure'] == "prevalence"), 'mean'] = 0
    # df.loc[(df['lower'] < 0)&(df['measure'] == "prevalence"), 'lower'] = 0
    # df.loc[(df['upper'] < 0)&(df['measure'] == "prevalence"), 'upper'] = 0

    # for incidence, mean, upper, and lower cannot be less than zero
    for col in cap_cols:
        df.loc[(df[col] < 0)&(df['measure'] == "incidence"), col] = 0
    # df.loc[(df['mean'] < 0)&(df['measure'] == "incidence"), 'mean'] = 0
    # df.loc[(df['lower'] < 0)&(df['measure'] == "incidence"), 'lower'] = 0
    # df.loc[(df['upper'] < 0)&(df['measure'] == "incidence"), 'upper'] = 0


    # for prevalence, if upper, lower, and mean are all the same, epi uploader doesn't work
    df.loc[(df['mean_0'] == 1)&(df['upper_0'] == 1)&(df['lower_0'] == 1)&(df['measure'] == 'prevalence'), 'lower_0'] = 0.999

    df.loc[(df['mean_1'] == 1)&(df['upper_1'] == 1)&(df['lower_1'] == 1)&(df['measure'] == 'prevalence'), 'lower_1'] = 0.999

    df.loc[(df['mean_2'] == 1)&(df['upper_2'] == 1)&(df['lower_2'] == 1)&(df['measure'] == 'prevalence'), 'lower_2'] = 0.999

    df.loc[(df['mean_3'] == 1)&(df['upper_3'] == 1)&(df['lower_3'] == 1)&(df['measure'] == 'prevalence'), 'lower_3'] = 0.999

    return(df)
