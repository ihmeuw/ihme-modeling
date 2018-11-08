"""
This file contains all the functions relevant to the outpatient process.  Most
are an adaptation of the Inpatient functions.
"""


import datetime
import os
import platform
import re
import sys
import time
import warnings

import numpy as np
import pandas as pd
from pandas.compat import u  # for printing nice location names in Brazil
pd.options.display.max_rows = 100
pd.options.display.max_columns = 100

from db_tools.ezfuncs import query
from db_queries import get_cause_metadata, get_population

if platform.system() == "Linux":
    root = r"FILEPATH/j"
else:
    root = "J:"

path = r"FILEPATH/Functions"
sys.path.append(path)

import hosp_prep
import gbd_hosp_prep

def drop_data_for_outpatient(df):
    """
    Function that drops data not relevant to the outpatient process.  Drops
    Inpatient, Canada, Norway, Brazil, and Philippines data.

    Returns
        DataFrame with only relevant Outpatient data.
    """

    # facility type, exclusion stats
    # hospital : inpatient
    # day clinic : EXCLUDED
    # emergency : EXCLUDED
    # clinic in hospital : outpatient, but has different pattern than other Outpatient
    # outpatient clinic : outpatient
    # inpatient unknown : inpatient
    # outpatient unknown : outpatient

    # print how many rows we're starting wtih
    print("Starting number of rows = {}".format(df.shape[0]))

    # drop everything but inpatient
    # our envelope is only inpatient so we only keep inpatient, for now.
    print("Dropping inpatient data...")
    df = df[(df['facility_id'] == 'outpatient unknown') |
            (df['facility_id'] == 'outpatient clinic') |
            (df['facility_id'] == 'clinic in hospital') |
            (df['facility_id'] == 'emergency')]
    print("Number of rows = {}".format(df.shape[0]))

    # check that we didn't somehow drop all rows
    assert df.shape[0] > 0, "All data was dropped, there are zero rows!"

    print("Dropping Norway, Brazil, Canada, and Philippines data...")
    df = df[df.source != "NOR_NIPH_08_12"]  
    df = df[df.source != 'BRA_SIA']  
    df = df[df.source != 'PHL_HICC'] 
    df = df[df['source'] != 'CAN_NACRS_02_09']
    df = df[df['source'] != 'CAN_DAD_94_09']
    print("Number of rows = {}".format(df.shape[0]))

    
    print("Dropping 1991 from NHAMCS...")
    df = df.loc[(df.source != "USA_NHAMCS_92_10")|(df.year_start != 1991)]
    print("Number of rows = {}".format(df.shape[0]))


    print("Dropping unknown sexes because we can't age-sex split outpatient...")
    
    df = df[df.sex_id != 9].copy()
    df = df[df.sex_id != 3].copy()
    print("Number of rows = {}".format(df.shape[0]))

    print("Done dropping data")

    return(df)


def outpatient_mapping(df, which_map="current"):
    """
    Function to map ICD level data to Bundle level.  Also aggregates to bundle
    level.  Attemps to match ICD codes to bundles that failed to initially by
    truncating the ICD code by one digit and re mapping.  Continues to do this
    until the failed ICD codes are 3 digits long, the shortest ICD code
    possible in both ICD 9 and ICD 10.  Drops rows where the ICD code didn't
    match after printing how many didn't match.

    Args:
        df (Pandas DataFrame) Contains ICD outpatient data
        which_map (str) Should be either "current" or "old".  If "current" reads
        the most recent version of the map, and if it's "old" then reads the
        last map hard coded into this function

    Returns:
        DataFrame mapped and aggregated to bundles.
    """

    def map_to_truncated(df):
        """
        Takes a dataframe of icd codes which didn't map to baby sequela
        merges clean maps onto every level of truncated icd codes, from 7
        digits to 3 digits when needed. returns only the data that has
        successfully mapped to a baby sequelae.
        """
        df_list = []
        for i in [7, 6, 5, 4]:
            # drop last digit
            df['cause_code'] = df['cause_code'].str[0:i]
            good = df.merge(maps, how='left', on=['cause_code',
                                                  'code_system_id'])
            # keep only rows that successfully merged
            good = good[good['bundle_id'].notnull()].copy()
            # drop the icd codes that matched from df
            df = df[~df.cause_code.isin(good.cause_code)].copy()
            df_list.append(good)

        dat = pd.concat(df_list, ignore_index=True)
        return(dat)

    if which_map == 'current':
        map_path = "FILEPATH/clean_map.csv"
        print map_path
    if which_map == "old":
        map_path = "FILEPATH/clean_map_10.csv"
        print map_path

    warnings.warn("""

                  Please ensure that the clean_map file is up to date.
                  the file was last edited at {}

                  """.format(time.strftime('%Y-%m-%d %H:%M:%S',
                  time.localtime(os.path.getmtime(map_path)))))

    maps = pd.read_csv(map_path, dtype={'cause_code': object})

    maps = maps[['cause_code', 'bundle_id', 'code_system_id', 'bid_measure']].copy()
    maps.rename(columns={'bid_measure': 'measure'}, inplace=True)

    # drop duplicate values.  Note that subsetting a map can "create"
    # duplicated values
    maps = maps[maps.bundle_id.notnull()].copy()
    maps = maps.drop_duplicates()

    # match on upper case
    df['cause_code'] = df['cause_code'].str.upper()
    maps['cause_code'] = maps['cause_code'].str.upper()

    # store variables for data check later
    before_values = df['cause_code'].value_counts()  # value counts before

    # merge the hospital data with excel spreadsheet maps
    df = df.merge(maps, how='left',
                  on=["cause_code", "code_system_id"])
    
    no_map = df[df['bundle_id'].isnull()].copy()

    df = df[df.bundle_id.notnull()].copy()

    # create a raw cause code column to remove icd codes that were fixed
    # and mapped successfully
    no_map['raw_cause_code'] = no_map['cause_code']

    # split icd 9 and 10 codes that didn't map
    no_map_10 = no_map[no_map.code_system_id == 2].copy()
    no_map_9 = no_map[no_map.code_system_id == 1].copy()

    # create remap_df to retry mapping for icd 10
    remap_10 = no_map_10.copy()
    remap_10.drop(['bundle_id', 'measure'], axis=1, inplace=True)
    # now do the actual remapping, losing all rows that don't map
    remap_10 = map_to_truncated(remap_10)

    # create remap_df to retry mapping for icd 9
    remap_9 = no_map_9.copy()
    remap_9.drop(['bundle_id', 'measure'], axis=1, inplace=True)
    # now do the actual remapping, losing all rows that don't map
    remap_9 = map_to_truncated(remap_9)

    # remove the rows where we were able to re-map to baby sequela
    no_map_10 = no_map_10[~no_map_10.raw_cause_code.isin(remap_10.raw_cause_code)]
    no_map_9 = no_map_9[~no_map_9.raw_cause_code.isin(remap_9.raw_cause_code)]

    # bring our split data frames back together
    df = pd.concat([df, remap_10, remap_9, no_map_10, no_map_9],
                   ignore_index=True)

    # remove the raw_cause_code column
    df.drop(['raw_cause_code'], axis=1, inplace=True)

    no_match_count = float(df['bundle_id'].isnull().sum())
    no_match_per = round(no_match_count/df.shape[0] * 100, 4)
    print("###\n" + str(no_match_count) + " rows did not match the map\n###\n" +
    "This is " + str(no_match_per) + "% of total rows that did not match\n###")

    # drop missing bundle_ids
    print("dropping rows without bundles...")
    df = df[df.bundle_id.notnull()]

    groups = ['location_id', 'year_start', 'year_end', 'age_group_unit',
              'age_start', 'age_end', 'sex_id', 'source', 'nid',
              'facility_id', 'representative_id',
              'metric_id', 'bundle_id', 'measure']
    print("performing groupby...")
    df = df.groupby(groups).agg({'val': 'sum'}).reset_index()

    int_cols = ['location_id', 'year_start', 'year_end', 'age_group_unit',
                'age_start', 'age_end', 'sex_id', 'nid', 'representative_id',
                'metric_id', 'bundle_id']
    float_cols = ['val']
    str_cols = ['source', 'facility_id']

    print("compressing...")
    for col in int_cols:
        df[col] = pd.to_numeric(df[col], errors='raise', downcast='integer')
    for col in float_cols:
        df[col] = pd.to_numeric(df[col], errors='raise', downcast='float')
    for col in str_cols:
        df[col] = df[col].astype(str)

    print("Done Mapping")

    return(df)


def get_parent_injuries(df):
    """
    Function that rolls up the child causes into parents add parent injuries.
    That is, it finds all the parent-child injury relationships, adds up the
    values for each child belonging to a parent, and assigns that total to the
    parent.  This is done because the map doesn't contain parent injuries.

    Args:
        df (Pandas DataFrame) Contains outpatient data at bundle level.

    Returns:
        Data with parent injuries added.
    """

    # grab some info from before changing things
    shape_before = df.shape
    bundles_before = list(df.bundle_id.unique())

    pc_injuries = pd.read_csv("FILEPATH/parent_child_injuries_gbd2017.csv")


    pc_injuries = pc_injuries.drop(['level1_meid', 'ME name level 1'], axis=1)
    pc_injuries = pc_injuries.drop_duplicates()

    # create BID and dicts for parent causes
    bundle_dict = dict(zip(pc_injuries.e_code, pc_injuries['Level1-Bundle ID']))

    # loop over ecodes pulling every parent name
    df_list = []
    for parent in pc_injuries.loc[pc_injuries['parent']==1, 'e_code']:
        
        inj_df = pc_injuries[(pc_injuries['baby sequela'].str.contains(parent)) & (pc_injuries['parent'] != 1)]
        
        inj_df = inj_df[inj_df['Level1-Bundle ID'].notnull()]
        inj_df['bundle_id'] = bundle_dict[parent]
        df_list.append(inj_df)

    parent_df = pd.concat(df_list, ignore_index=True)

    # number of child injuries in csv should match new df
    assert pc_injuries.child.sum() == parent_df.child.sum(),\
        "sum of child causes doesn't match sum of parent causes"

    parent_df.drop(['baby sequela', 'level 1 measure',
                    'e_code', 'parent', 'child'], axis=1,
                    inplace=True)


    parent_df.rename(columns={'bundle_id': 'parent_bundle_id',
                              'Level1-Bundle ID': 'bundle_id'},  # child bundle_id
                     inplace=True)


    col_before = parent_df.columns
    parent_df = parent_df[['bundle_id', 'parent_bundle_id']].copy()
    assert set(col_before) == set(parent_df.columns), 'you dropped a column while reordering columns'


    parent_df = parent_df.merge(df, how='left', on='bundle_id')

    parent_df.drop(['bundle_id'], axis=1, inplace=True)

    parent_df.rename(columns={'parent_bundle_id': 'bundle_id'}, inplace=True)

    groups = ['location_id', 'year_start', 'year_end',
              'age_start', 'age_end', 'sex_id', 'nid',
              'representative_id', 'bundle_id',
              'metric_id', 'source', 'facility_id',
              'age_group_unit', 'measure']
    parent_df = parent_df.groupby(groups).agg({"val": "sum"}).reset_index()

    assert_msg = """
    columns do not match, the difference is
    {}
    """.format(set(parent_df.columns).symmetric_difference(set(df.columns)))
    assert set(parent_df.columns).symmetric_difference(set(df.columns)) == set(),\
        assert_msg

    # rbind duped parent data back onto hospital data
    df = pd.concat([df, parent_df])

    report = """
        shape before : {}
        shape after : {}

        number of bundles before : {}
        number of bundles after : {}

        new bundles : {}
        """.format(
        shape_before,
        df.shape,
        len(bundles_before),
        len(df.bundle_id.unique()),
        set(df.bundle_id.unique()) - set(bundles_before)
    )

    print report
    return(df)


def apply_outpatient_correction(df):
    """
    Function to apply claims derived correction to outpatient data. Adds the
    column "val_corrected" to the data, which is the corrected data. Applies the
    correction to every row, even though in the end we only want to apply it to
    non-injuries data.  That gets taken care of while writing data.

    Args:
        df (Pandas DataFrame) Contains outpatient data at bundle level.

    Returns:
        Data with corrections applied, with the new column "val_corrected"
    """

    filepath = "FILEPATH/wide_smoothed_by_bundle_id.csv"

    warnings.warn("""

                  Please ensure that the corrections file is up to date.
                  the file was last edited at {}

                  """.format(time.strftime('%Y-%m-%d %H:%M:%S',
                  time.localtime(os.path.getmtime(filepath)))))

    # load corrections
    corrections = pd.read_csv(filepath)

    corrections = corrections.drop(['indv_cf', 'incidence', 'prevalence',
        'injury_cf'], axis=1)

    corrections = corrections.rename(columns={'outpatient': 'smoothed_value'})
    corrections.rename(columns={'sex': 'sex_id'}, inplace=True)

    # age and sex restricted cfs are null, should become one. data would be zero anyways
    corrections.update(corrections['smoothed_value'].fillna(1))

    assert not corrections.smoothed_value.isnull().any(),\
        "shouldn't be any nulls in smoothed_value"

    print "these bundle_ids have smoothed value greater than 1",\
        sorted(corrections.loc[corrections.smoothed_value > 1,
                               'bundle_id'].unique()), "so we made their CF 1"
    # happend like half a percent of the time, corr factors needs some work
    corrections.loc[corrections.smoothed_value > 1, 'smoothed_value'] = 1

    pre_shape = df.shape[0]
    df = df.merge(corrections, how='left', on =['age_start', 'sex_id', 'bundle_id'])
    assert pre_shape == df.shape[0], "merge somehow added rows"
    print "these bundles didn't get corrections",\
        sorted(df.loc[df.smoothed_value.isnull(), 'bundle_id'].unique()),\
        " so we made their CF 1"
    
    df.update(df['smoothed_value'].fillna(1))
    assert not df.isnull().any().any(), "there are nulls!"

    # apply correction
    df['val_corrected'] = df['val'] * df['smoothed_value']

    # drop correction
    df.drop("smoothed_value", axis=1, inplace=True)

    return(df)


def apply_inj_factor(df, fillna=False):
    """
    Function that merges on and applies the injury-specific correction factor.
    This correction only increases values.  The correction is source specific.
    This adds the columns "factor", "remove", and "val_inj_corrected". "factor"
    is the injury correction, "remove" indicates that the injury team will want
    to drop that value, and "val_inj_corrected" is the corrected data.  The
    correction is meant for injuries, but will be applied to every row for
    simplicity.  The appropriate columns will be dropped later. For now there
    isn't going to be a column that has both the normal claims-derived
    outpatient correction and the injury correction applied.

    Args:
        df: (Pandas DataFrame) Contains your outpatient data.
        fillna: (bool) If True, will fill the columns "factor" and "remove" with
            1 and 0, respectively. Because the injury corrections are source
            specific, there is a chance that not all rows will get a factor.

    Returns:
        DataFrame with injury factors / corrections applied.  Has the new
        additional columns "factor", "remove", and "val_inj_corrected"
    """
    # load
    filepath = "FILEPATH/inj_factors.H5"

    warnings.warn("""

                  Please ensure that the factors file is up to date.
                  the file was last edited at {}

                  """.format(time.strftime('%Y-%m-%d %H:%M:%S',
                  time.localtime(os.path.getmtime(filepath)))))
    factors = pd.read_hdf(filepath)
    
    factors.drop("prop", axis=1, inplace=True)

    df = df.merge(factors, how='left', on=['location_id', 'year_start',
                                           'year_end', 'nid', 'facility_id'])

    null_msg = """
    There are {} nulls in the factor column after the merge,
    due to mismatched key columns. They appear in these ID rows:
    {}
    """.format(df[df.factor.isnull()].shape[0],
               df.loc[df.factor.isnull(),
                      ['source', 'location_id', 'year_start', 'year_end',
                       'facility_id', 'nid']].drop_duplicates().sort_values([
                       'source', 'location_id', 'year_start', 'year_end', 'nid',
                       'facility_id']))
    print(null_msg)

    if fillna:
        print("Filling the Null values with a factor of 1, and remove of 0")
        df.update(df['factor'].fillna(1))
        df.update(df['remove'].fillna(0))
        assert df.isnull().sum().sum() == 0,\
            "There are Nulls in some column besides factor and remove"

    # apply the correction
    df['val_inj_corrected'] = df['val'] * df['factor']


    return df


def outpatient_restrictions(df):
    """
    Function that applies USERNAME custom age sex restrictions.

    Args:
        df (Pandas DataFrame) Contains outpatient data at bundle level.

    Returns:
        Data with restrictions applied.
    """

    filepath = "FILEPATH/bundle_restrictions.csv"

    warnings.warn("""

                  Please ensure that the restrictions file is up to date.
                  the file was last edited at {}

                  """.format(time.strftime('%Y-%m-%d %H:%M:%S',
                  time.localtime(os.path.getmtime(filepath)))))

    cause = pd.read_csv(filepath)
    cause = cause[['bundle_id', 'male', 'female', 'yld_age_start', 'yld_age_end']].copy()
    cause = cause.drop_duplicates()

    # replace values below 1 with zero in get_cause, we don't differentiate
    # under one years old.
    cause['yld_age_start'].loc[cause['yld_age_start'] < 1] = 0

    # merge get_cause_metadata onto hospital using cause_id map
    pre_cause = df.shape[0]
    df = df.merge(cause, how='left', on = 'bundle_id')
    assert pre_cause == df.shape[0],\
        "The merge duplicated rows unexpectedly"

    # set mean to zero where male in cause = 0
    df.loc[(df['male'] == 0) & (df['sex_id'] == 1), 'val'] = 0
    df.loc[(df['male'] == 0) & (df['sex_id'] == 1), 'val_corrected'] = 0
    df.loc[(df['male'] == 0) & (df['sex_id'] == 1), 'val_inj_corrected'] = 0

    # set mean to zero where female in cause = 0
    df.loc[(df['female'] == 0) & (df['sex_id'] == 2), 'val'] = 0
    df.loc[(df['female'] == 0) & (df['sex_id'] == 2), 'val_corrected'] = 0
    df.loc[(df['female'] == 0) & (df['sex_id'] == 2), 'val_inj_corrected'] = 0

    # set mean to zero where age end is smaller than yld age start
    df.loc[df['age_end'] < df['yld_age_start'], 'val'] = 0
    df.loc[df['age_end'] < df['yld_age_start'], 'val_corrected'] = 0
    df.loc[df['age_end'] < df['yld_age_start'], 'val_inj_corrected'] = 0

    # set mean to zero where age start is larger than yld age end
    df.loc[df['age_start'] > df['yld_age_end'], 'val'] = 0
    df.loc[df['age_start'] > df['yld_age_end'], 'val_corrected'] = 0
    df.loc[df['age_start'] > df['yld_age_end'], 'val_inj_corrected'] = 0

    df.drop(['male', 'female', 'yld_age_start', 'yld_age_end'], axis=1,
            inplace=True)
    print("\n")
    print("Done with Restrictions")

    return(df)


def get_sample_size(df, fix_top_age=True):
    """
    Function that attaches a sample size to the outpatient data.  Sample size
    is necessary for DisMod so that it can infer uncertainty.  We use population
    as sample size, because the assumption is that our outpatient sources are
    representative of their respective populations

    Args:
        df (Pandas DataFrame) contains outpatient data at the bundle level.

    Returns:
        Data with a new column "sample_size" attached, which contains population
        counts.
    """

    # get the normal populations
    pop = get_population(QUERY)

    # this stuff is for making weird populations
    if fix_top_age:

        # make pop for age_group_id 160
        # age_group_id 160 is made of 31, 32, and 235
        pop_160 = get_population(QUERY)
        pre = pop_160.shape[0]
        pop_160['age_group_id'] = 160
        pop_160 = pop_160.groupby(pop_160.columns.drop('population').tolist()).agg({'population': 'sum'}).reset_index()
        assert pre/3.0 == pop_160.shape[0]

        # make pop for age_group_id 21
        # age_group_id 21 is made of 30, 31, 32, 235
        pop_21 = get_population(QUERY)
        pre = pop_21.shape[0]
        pop_21['age_group_id'] = 21
        pop_21 = pop_21.groupby(pop_21.columns.drop('population').tolist()).agg({'population': 'sum'}).reset_index()
        assert pre/4.0 == pop_21.shape[0]

    pop = pd.concat([pop, pop_160, pop_21], ignore_index=True)

    # rename pop columns to match hospital data columns
    pop.rename(columns={'year_id': 'year_start'}, inplace=True)
    pop['year_end'] = pop['year_start']
    pop.drop("run_id", axis=1, inplace=True)

    pop = pop.drop_duplicates(subset=pop.columns.drop('population').tolist())

    demography = ['location_id', 'year_start', 'year_end', 'age_group_id',
                  'sex_id']

    pre_shape = df.shape[0]  # store for before comparison
    # then merge population onto the hospital data

    df = df.merge(pop, how='left', on=demography)  # attach pop info to hosp
    assert_msg = """
    number of rows don't match after merge. before it was {} and now it
    is {} for a difference (before - after) of {}
    """.format(pre_shape, df.shape[0], pre_shape-df.shape[0])
    assert pre_shape == df.shape[0], assert_msg

    assert df.isnull().sum().sum() == 0, 'there are nulls'

    # df.drop('age_group_id', axis=1, inplace=True)

    print("Done getting sample size")

    return(df)


def outpatient_elmo(df, make_right_inclusive=True):
    """
    Function that prepares data for upload to the epi database.  Adds a lot of
    columns, renames a lot of columns.

    Args:
        df (Pandas DataFrame) contains outpatient data at the bundle level.
        make_right_inclusive: (bool) This switch changes values in the
            'age_demographer' column and the 'age_end' column.

            If True, 'age_demographer' column will be set to 1. age_end will be
            made have values ending in 4s and 9s. For example, these age groups
            would be 5-9, 10-14, ... That means that an age_end is inclusive.
            That is, a value of 9 in age_end means that 9 is included in the
            range.

            If False, then 'age_demographer' will be set to 0 and age_end will
            be right exclusive.  age_end will have values ending in 5s and 0s,
            like 5-10, 10-15, ... That is, a value of 10 in age_end would not
            include 10. It would be ages up to but not including 10.

    Returns:
        Data formatted and ready for uploading to Epi DB.
    """

    if make_right_inclusive:
        # first want to subtract 1 age end, and make sure that that makes sense
        assert (df.loc[df.age_end > 1, 'age_end'].values % 5 == 0).all(),\
            """age_end appears not to be a multiple of 5, indicating that
               subtracting 1 is a bad move"""

        # subtract
        df.loc[df.age_end > 1, 'age_end'] = df.loc[df.age_end > 1, 'age_end'] - 1

        # make age_demographer column
        df['age_demographer'] = 1
    else:
        # age_end should have values ending in 4s and 9s.
        assert (df.loc[df.age_end > 1, 'age_end'].values % 5 != 0).all(),\
            """age_end appears to be a multiple of 5, indicating that
               setting age_demographer to 0 is a bad move."""
        df['age_demographer'] = 0

    df.loc[df.age_end == 1, 'age_demographer'] = 0

    # drop
    df = df.drop(['source', 'facility_id', 'metric_id'], axis=1)  # used to drop diagnosis id here
    df.rename(columns={'representative_id': 'representative_name',
                       "val_inj_corrected": "cases_inj_corrected",
                       'val_corrected': 'cases_corrected',
                       'val': 'cases_uncorrected',
                       'population': 'sample_size',
                       'sex_id': 'sex'},
              inplace=True)


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
    df['source_type'] = 'Facility - outpatient'
    df['urbanicity_type'] = 'Unknown'
    df['recall_type'] = 'Not Set'
    df['unit_type'] = 'Person'
    df['unit_value_as_published'] = 1
    df['is_outlier'] = 0
    df['sex'].replace([1, 2], ['Male', 'Female'], inplace=True)
    df['measure'].replace(["prev", "inc"], ["prevalence", "incidence"], inplace=True)

    df['mean'] = np.nan
    df['upper'] = np.nan
    df['lower'] = np.nan
    df['seq'] = np.nan  
    df['underlying_nid'] = np.nan
    df['sampling_type'] = np.nan 
    df['recall_type_value'] = np.nan
    df['uncertainty_type'] = np.nan 
    df['uncertainty_type_value'] = np.nan
    df['input_type'] = np.nan
    df['standard_error'] = np.nan  
    df['effective_sample_size'] = np.nan
    df['design_effect'] = np.nan  
    df['response_rate'] = np.nan
    df['extractor'] = "USERNAME"

    # human readable location names
    loc_map = query("QUERY")
    df = df.merge(loc_map, how='left', on='location_id')

    # add bundle_name
    bundle_name_df = query("QUERY")

    pre_shape = df.shape[0]
    df = df.merge(bundle_name_df, how="left", on="bundle_id")
    assert df.shape[0] == pre_shape, "added rows in merge"

    print("DONE WITH ELMO")
    return(df)


def outpatient_write_bundles(df, write_location="test"):
    """
    Function that writes data to modeler's folders.  Reorders columns. Once it
    starts writing, CTRL C usually won't stop it; a CTRL Z may be required.

    Args:
        df: (Pandas DataFrame)  Ootpatient data that has been prepared for
            upload to Epi DB.
        write_location: (str) if "test", writes to
            FILEPATH if work, writes to
            FILEPATH  If "work", warns user and waits 5 seconds to
            give an opportunity to interrupt before continuing.

    Returns:
        list of directories where it failed to write data.
    """
    
    cause_id_info = query("QUERY")
    # get acause
    acause_info = query("QUERY")
    # merge acause, bid, cause_id info together
    acause_info = acause_info.merge(cause_id_info, how="left", on="cause_id")

    # REI INFORMATION
    # get rei_id so we can write to a rei
    rei_id_info = query("QUERY")
    # get rei
    rei_info = query("QUERY")
    # merge rei, bid, rei_id together into one dataframe
    rei_info = rei_info.merge(rei_id_info, how="left", on="rei_id")

    #COMBINE REI AND ACAUSE
    # rename acause to match
    acause_info.rename(columns={'cause_id': 'cause_rei_id',
                                'acause': 'acause_rei'}, inplace=True)
    # rename rei to match
    rei_info.rename(columns={'rei_id': 'cause_rei_id',
                             'rei': 'acause_rei'}, inplace=True)

    # concat rei and acause together
    folder_info = pd.concat([acause_info, rei_info])

    # drop rows that don't have bundle_ids
    folder_info = folder_info.dropna(subset=['bundle_id'])

    # drop cause_rei_id, because we don't need it for getting data into folders
    folder_info.drop("cause_rei_id", axis=1, inplace=True)

    # drop duplicates, just in case there are any
    folder_info.drop_duplicates(inplace=True)
    
    df = df.merge(folder_info, how="left", on="bundle_id")

    start = time.time()
    bundle_ids = df['bundle_id'].unique()

    # prevalence, indicence should be lower case
    df['measure'] = df['measure'].str.lower()

    # get injuries bundle_ids so we can keep injury corrected data later
    pc_injuries = pd.read_csv("FILEPATH/"\
                              r"parent_child_injuries_gbd2017.csv")
    inj_bid_list = pc_injuries['Level1-Bundle ID'].unique()

    # arrange columns
    columns_before = df.columns
    ordered = ['bundle_id', 'bundle_name', 'measure',
               'location_id', 'location_name', 'year_start', 'year_end',
               'age_start',
               'age_end', 'age_group_unit', 'age_demographer', 'sex', 'nid',
               'representative_name', 'cases_uncorrected',
               'cases_corrected', "cases_inj_corrected", "factor", "remove",
               'sample_size',
               'mean', 'upper', 'lower',
               'source_type', 'urbanicity_type',
               'recall_type', 'unit_type', 'unit_value_as_published',
               'is_outlier', 'seq', 'underlying_nid',
               'sampling_type', 'recall_type_value', 'uncertainty_type',
               'uncertainty_type_value', 'input_type', 'standard_error',
               'effective_sample_size', 'design_effect', 'response_rate',
               'extractor', 'acause_rei']
    df = df[ordered]
    columns_after = df.columns
    print "are they equal?", set(columns_after) == set(columns_before)
    print "what's the difference?", set(columns_after).symmetric_difference(set(columns_after))
    print 'before minus after:', set(columns_before) - set(columns_after)
    print 'after minus before:', set(columns_after) - set(columns_before)
    assert set(columns_after) == set(columns_before),\
        "you added/lost columns while reordering columns!!"
    failed_bundles = []  # initialize empty list to append to in this for loop
    print "WRITING FILES"
    for bundle in bundle_ids:
        # subset bundle data
        df_b = df[df['bundle_id'] == bundle].copy()

        acause_rei = str(df_b.acause_rei.unique()[0])
        df_b.drop('acause_rei', axis=1, inplace=True)

        if write_location == "work":
            writedir = 'FILEPATH'
        if write_location == "test":
            writedir = 'FILEPATH'
        print "{}".format(writedir)

        if not os.path.isdir(writedir):
            os.makedirs(writedir)  # make the directory if it does not exist

        # write for modelers
        # make path
        vers_id = "GBD2017_v7" 
        date = datetime.datetime.today().strftime("%Y_%m_%d")
        
        bundle_path = "{}{}_{}_{}.xlsx".format(writedir, int(bundle), vers_id, date)
        

        # fix columns for inj and non inj
        if bundle not in inj_bid_list:
            df_b.drop(['cases_inj_corrected', 'factor', 'remove'],
                       axis=1, inplace=True)
        if bundle in inj_bid_list:
            df_b.drop(["cases_corrected"], axis=1, inplace=True)


        
        try:
        
            writer = pd.ExcelWriter(bundle_path, engine='xlsxwriter')
            df_b.to_excel(writer, sheet_name="extraction", index=False)
            writer.save()
        except:
            failed_bundles.append(bundle)
            
    end = time.time()
   
    text = open(root + re.sub("\W", "_", str(datetime.datetime.now())) +
                "_OUTPATIENT_write_bundles_logs.txt", "w")
    text.write("function: write_bundles " + "\n"+
               "start time: " + str(start) + "\n" +
               " end time: " + str(end) + "\n" +
               " run time: " + str((end-start)/60.0) + " minutes")
    text.close()
    return(failed_bundles)
