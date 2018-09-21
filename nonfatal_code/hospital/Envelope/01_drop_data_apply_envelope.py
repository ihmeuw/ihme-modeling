"""
Function that creates cause fractions and applies the envelope
"""

import datetime
import platform
import re
import sys
import getpass
import warnings
import pandas as pd
import numpy as np
from db_queries import get_cause_metadata, get_population, get_covariate_estimates
from db_tools.ezfuncs import query

# load our functions
if getpass.getuser() == 'USERNAME':
    sys.path.append("FILEPATH")
import hosp_prep
import gbd_hosp_prep

if platform.system() == "Linux":
    root = "FILEPATH"
else:
    root = "FILEPATH"


def prepare_envelope(df, fix_maternal=False, return_only_inj=False,
                     apply_age_sex_restrictions=True, want_to_drop_data=True):
    """
    function that accepts as a parameter hospital dataframe and adds the
    the envelope to it, and adds meids and bundle_ids to it, duplicating
    baby sequela in the process (as expected)

    Parameters:
    df: DataFrame
        contains hospital inpatient primary diagnosis that has been mapped to
        Baby Sequelae
    fix_maternal: Boolean
        Should always be on, it's just an option for testing purposes to save
        time.
        switch that runs fix_maternal_denominators.  This function transforms
        hospital data from 'population' to 'live births' using Age Specific
        Fertility Rates.
    return_only_inj: Boolean
        switch that will make it so this function only returns injuries data.
        should always be off, it's just there for testing.
    apply_age_sex_restrictions: Boolean
        switch that if True, will apply age and sex restrictions as determined
        by cause_set_id=9.  DUSERts to true.  Useful if you want to look at
        data all the data that was present in the source.
    """

    ###############################################
    # DROP DATA
    ###############################################

    if want_to_drop_data:
        df = hosp_prep.drop_data(df, verbose=False)


    ###############################################
    # MAP AGE_GROUP_ID ONTO DATA
    ###############################################
    # These are our weird hospital age_group_id: we use 235 for 95-99, and we
    # don't use age_group_ids 2,3,4, we use 28 in their place
    ages = hosp_prep.get_hospital_age_groups()  # df that has age_group_id, age_start,
    # and age_end
    df = df.merge(ages, how='left', on=['age_start', 'age_end'])

    # comment out to test the new sources in the process
    if not df.age_group_id.notnull().all():
        warnings.warn("""Shouldn't be any null age_group_id, there could be
        unsplit ages in the data, or maybe an age_start/age_end changed""")

    # maps = pd.read_csv("FILEPATH")
    # maps = maps[['nonfatal_cause_name', 'cause_id', 'level']].copy()
    # maps = maps[maps.cause_id.notnull()]
    # maps = maps[maps.level == 1]
    # maps.drop("level", axis=1, inplace=True)# no need to merge level onto data
    # maps.drop_duplicates(inplace=True)
    #
    # pre = df.shape[0]
    # df = df.merge(maps, how='left', on='nonfatal_cause_name')
    # assert df.shape[0] == pre, "df shape changed during merge"

    #########################################################
    # Get denominator for sources where we don't want to use envelope
    #########################################################

    # Make list of sources that we don't want to use envelope
    full_coverage_sources = ["UK_HOSPITAL_STATISTICS"]

    if pd.Series(full_coverage_sources).isin(df.source.unique()).any():
        # make dataframe with sources
        full_coverage_df = df[df.source.isin(full_coverage_sources)].copy()

        # get denominator (aka population)
        full_coverage_df = gbd_hosp_prep.get_sample_size(full_coverage_df)

        full_coverage_df.rename(columns={'population': 'sample_size'}, inplace=True)
        # NOTE now there's going be a column "sample_size" that
        # is null for every source except the fully covered ones

        # make product
        full_coverage_df['product'] = full_coverage_df.val / full_coverage_df.sample_size
    else:
        # make empty dataframe so that it exists and wont break anything
        full_coverage_df = pd.DataFrame(columns=list(df.columns) + ['product'])

    # drop these sources from dataframe for now
    df = df[~df.source.isin(full_coverage_sources)].copy()

    #########################################
    # CREATE CAUSE FRACTIONS
    #########################################
    df = hosp_prep.create_cause_fraction(df)

    ###############################################
    # APPLY CAUSE RESTRICTIONS
    ###############################################

    # NOTE it is okay to apply corrections after cause fractions, because restrictions
    # were also applied before the cause fractions were made ( right before splitting)
    if apply_age_sex_restrictions:
        df = hosp_prep.apply_restrictions(df, 'cause_fraction')
        full_coverage_df = hosp_prep.apply_restrictions(full_coverage_df, 'product')
        # drop the restricted values
        df = df[df['cause_fraction'].notnull()]
        full_coverage_df = full_coverage_df[full_coverage_df['product'].notnull()]

    #########################################
    # APPLY ENVELOPE
    #########################################
    # read envelope
    env_df = pd.read_csv("FILEPATH")
    locs = df.location_id.unique()  # all locations in our data
    # find locations that are missing age end 99
    missing_99 = list(set(locs) -\
        set(env_df.loc[(env_df.location_id.isin(locs))&(env_df.age_end == 99),
                       'location_id'].unique()))
    # change their age_ends to be 99
    env_df.loc[(env_df.location_id.isin(missing_99))&(env_df.age_start == 95),
               'age_end'] = 99
    # since we do NOT want to merge on by age_group_id
    env_df.drop('age_group_id', axis=1, inplace=True)

    demography = ['location_id', 'year_start', 'year_end', 'age_start',
                  'age_end', 'sex_id']

    # merge envelope onto data
    pre_shape = df.shape[0]
    df = df.merge(env_df, how='left', on=demography)
    assert pre_shape == df.shape[0],\
        "The merge duplicated rows unexpectedly"

    # compute nfc hospitalization rate
    # aka "apply the envelope"
    df['product'] = df['cause_fraction'] * df['mean']
    df['upper_product'] = df['cause_fraction'] * df['upper']
    df['lower_product'] = df['cause_fraction'] * df['lower']

    ############################################
    # RE-ATTACH data that has sample size instead of
    ############################################

    df = pd.concat([df, full_coverage_df]).reset_index(drop=True)
    # NOTE now there's going be a column "sample_size" and "cases" that
    # is null for every source except the fully covered ones

    # if we just want injuries:
    if return_only_inj == True:
        inj_me = pd.read_csv("FILEPATH")
        inj_me = list(inj_me['level1_meid'])
        df = df[df['modelable_entity_id'].isin(inj_me)]

    # drop columns we don't need anymore
    # cause fraction: finished using it, was used to make product
    # mean: finished using it, was used to make product
    # upper: finished using, it was used ot make product_upper
    # lower: finished using, it was used ot make product_lower
    # val, numerator, denomantor: no longer in count space
    # NON FATAL_CAUSE_NAME IS LEFT TO SIGNIFY THAT THIS DATA IS STILL AT THE
    # NONFATAL_CAUSE_NAME LEVEL
    df.drop(['cause_fraction','mean', 'upper', 'lower', 'val',
             'numerator', 'denominator'], axis=1, inplace=True)

    if "bundle_id" in df.columns:
        level_of_analysis = "bundle_id"
    if "nonfatal_cause_name" in df.columns:
        level_of_analysis = "nonfatal_cause_name"
    # write some summary stats of the env being applied
    # this is to review against our aggregated data to find large differences
    today = datetime.datetime.today().strftime("%Y_%m_%d").replace(":", "_")
    # Zeros can really skew the summary stats so drop them
    env_stats = df[df['product'] != 0].\
        groupby(['location_id', level_of_analysis])['product'].\
        describe().reset_index()
    env_stats.rename(columns={'level_2': 'stats'}, inplace=True)
    env_stats.to_csv("FILEPATH", index=False)

    return(df)
