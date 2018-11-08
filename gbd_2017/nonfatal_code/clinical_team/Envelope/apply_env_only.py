"""
Function that creates cause fractions and applies just the envelope NOT the envelope * CF uncertainty
"""

import datetime
import platform
import re
import sys
import getpass
import warnings
import pandas as pd
import numpy as np

# load our functions
user = getpass.getuser()
prep_path = "FILEPATH"
sys.path.append(prep_path)
repo = "FILEPATH"
import hosp_prep
import gbd_hosp_prep

if platform.system() == "Linux":
    root = r"/FILEPATH/j"
else:
    root = "J:"


def apply_envelope_only(df, env_path, return_only_inj=False,
                     apply_age_sex_restrictions=True, want_to_drop_data=True,
                     create_hosp_denom=False, apply_env_subroutine=False,
                     fix_norway_subnat=False):
    """
    Function that converts hospital data into rates.  Takes hospital data in
    count space, at the baby Sequelae level, computes cause fractions, attaches
    the hospital utilization envelope, and multiplies the envelope and the cause
    fractions.  Data that represents fully covered populations are not made into
    cause fractions, nor is the envelope applied.  Instead, their counts are
    divided by population. Returns a DataFrame that is in Rate spaces at the
    Baby Sequelae level.

    Arguments:
        df (DataFrame) contains hospital inpatient primary diagnosis that
            has been mapped to Baby Sequelae
        return_only_inj (bool):
            switch that will make it so this function only returns injuries
            data. should always be off, it's just there for
            testing/convienience.
        apply_age_sex_restrictions (bool): switch that if True, will apply
            age and sex restrictions as determined by cause_set_id=9.  Defaults
            to true.  Useful if you want to look at data all the data that was
            present in the source.
        want_to_drop_data (bool): If True will run drop_data(verbose=False)
            from the hosp_prep module
        create_hosp_denom (bool): If true create_cause_fraction() will write
            the denominator for use later for imputed zeros.
    """


    ###############################################
    # DROP DATA
    ###############################################
    if want_to_drop_data:
        df = hosp_prep.drop_data(df, verbose=False)

    if not df.age_group_id.notnull().all():
        warnings.warn("""Shouldn't be any null age_group_id, there could be
        unsplit ages in the data, or maybe an age_start/age_end changed""")

    # Make list of sources that we don't want to use envelope.  This should be
    # the source in the 'source' column of the dataframe
    full_coverage_sources = ["UK_HOSPITAL_STATISTICS"]

    if pd.Series(full_coverage_sources).isin(df.source.unique()).any():
        # make dataframe with sources
        full_coverage_df = df[df.source.isin(full_coverage_sources)].copy()

        # get denominator (aka poplation)
        full_coverage_df = gbd_hosp_prep.get_sample_size(full_coverage_df,
            fix_group237=True)

        full_coverage_df.rename(columns={'population': 'sample_size'},
                                inplace=True)

        # make product
        full_coverage_df['product'] =\
            full_coverage_df.val / full_coverage_df.sample_size

    else:
        # make empty dataframe so that it exists and wont break anything
        full_coverage_df = pd.DataFrame(columns=list(df.columns) + ['product'])

    # drop these sources from dataframe for now
    df = df[~df.source.isin(full_coverage_sources)].copy()

    #########################################
    # CREATE CAUSE FRACTIONS
    #########################################
    if not apply_env_subroutine:
        df = hosp_prep.create_cause_fraction(df,
                                             create_hosp_denom=create_hosp_denom,
                                             store_diagnostics=True,
                                             tol=1e-6)

    if apply_age_sex_restrictions:
        df = hosp_prep.apply_restrictions(df, 'cause_fraction')
        if full_coverage_df.shape[0] > 0:
            full_coverage_df = hosp_prep.apply_restrictions(full_coverage_df,
                                                            'product')
        # drop the restricted values
        df = df[df['cause_fraction'].notnull()]
        full_coverage_df =\
            full_coverage_df[full_coverage_df['product'].notnull()]

    if env_path[-3:] == "csv":
        env_df = pd.read_csv(env_path)
    else:
        env_df = pd.read_hdf(env_path, key='df')

    # split the envelope into Norway subnationals
    if fix_norway_subnat:
        # make a df of national and subnational locs
        nor_locs = [4910, 4911, 4912, 4913, 4914, 4915, 4916, 4917, 4918,
                    4919, 4920, 4921, 4922, 4923, 4926, 4927, 4928, 53432]
        # repeat norway's national location n times
        nat_locs = np.repeat(90, len(nor_locs))
        nor_subnat_df = pd.DataFrame({'sub_location_id': nor_locs, 'location_id': nat_locs})

        # merge env on by national loc
        nor_subnat_df = nor_subnat_df.merge(env_df, how='left', on='location_id')
        # set subnation loc to proper loc id
        nor_subnat_df.drop('location_id', axis=1, inplace=True)
        nor_subnat_df.rename(columns={'sub_location_id': 'location_id'}, inplace=True)
        # append the new locations onto the env
        env_df = pd.concat([env_df, nor_subnat_df], ignore_index=True)
        assert env_df[env_df.location_id.isin(nor_locs)].shape[0] > 5000,\
            "Some of the norway subnational locations {} weren't merged on. {}".\
            format(nor_locs, env_df[env_df.location_id.isin(nor_locs)].location_id.unique())

    env_df.drop(['age_start', 'age_end'], axis=1, inplace=True)

    demography = ['location_id', 'year_start', 'year_end',
                  'age_group_id', 'sex_id']

    ############################################
    # MERGE ENVELOPE onto data
    ############################################
    pre_shape = df.shape[0]
    df = df.merge(env_df, how='left', on=demography)
    assert pre_shape == df.shape[0],\
        "The merge duplicated rows unexpectedly"

    # compute hospitalization rate
    # aka "apply the envelope"
    df['product'] = df['cause_fraction'] * df['mean']
    df['upper_product'] = df['cause_fraction'] * df['upper']
    df['lower_product'] = df['cause_fraction'] * df['lower']

    ############################################
    # RE-ATTACH data that has sample size
    ############################################

    df = pd.concat([df, full_coverage_df]).reset_index(drop=True)

    # if we just want injuries:
    if return_only_inj == True:
        inj_bundle = pd.read_csv(root +  r"FILEPATH"
                             r"parent_child_injuries_gbd2017.csv")
        inj_bundle = list(inj_bundle['Level1-Bundle ID'])
        df = df[df['bundle_id'].isin(inj_bundle)]

    if apply_env_subroutine:
        df.drop(['cause_fraction','mean', 'upper', 'lower'], axis=1, inplace=True)
        df.rename(columns={'product': 'mean',
                           'lower_product': 'lower',
                           'upper_product': 'upper'}, inplace=True)
    else:
        df.drop(['cause_fraction','mean', 'upper', 'lower', 'val',
                 'numerator', 'denominator'], axis=1, inplace=True)

    return(df)