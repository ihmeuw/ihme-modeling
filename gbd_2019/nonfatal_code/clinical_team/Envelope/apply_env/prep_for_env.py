"""
A set of functions that prepare inpatient hospital data for the correction
factor/envelope uncertainty data
There are 5 rough steps
1) Drop data
2) Split into covered and uncovered dataframes
    2.1) create "mean" from the covered data
3) Create cause fractions for the uncovered data
4) Apply age and sex restrictions
5) Split the envelope draws into smaller files to increase runtime
    Note: only needed when a new env is created OR new hospital
    data locations are included

Note: output is 2 dataframes, one with covered sources, one with uncovered
(ie sources that use the envelope) sources
"""

import platform
import sys
import getpass
import warnings
import pandas as pd
import numpy as np
import ipdb

# load our functions
from clinical_info.Mapping import clinical_mapping as cm
from clinical_info.Functions import hosp_prep, gbd_hosp_prep
from clinical_info.Envelope import redistribute_poisoning_drug as rpd


def split_sources(df, gbd_round_id, decomp_step):
    """

    The assumption here is that the population is fully covered, so we don't
    need to use the envelope

    This will be passed to PopEstimates to create population level rates
    """


    # Make list of sources that we don't want to use envelope.  This should be
    # the source in the 'source' column of the dataframe

    full_coverage_sources = ["UK_HOSPITAL_STATISTICS"]

    if pd.Series(full_coverage_sources).isin(df.source.unique()).any():
        # make dataframe with sources
        full_coverage_df = df[df.source.isin(full_coverage_sources)].copy()

        
    else:
        # make empty dataframe so that it exists and wont break anything
        full_coverage_df = pd.DataFrame(columns=list(df.columns))

    # drop these sources from dataframe for now
    df = df[~df.source.isin(full_coverage_sources)].copy()

    return df, full_coverage_df

def apply_restricts(df, full_coverage_df):
    """

    """

    df = cm.apply_restrictions(df,
                               age_set='age_group_id',
                               cause_type='icg')
    full_coverage_df = cm.apply_restrictions(full_coverage_df,
                                             age_set='age_group_id',
                                             cause_type='icg')

    # drop the restricted values
    df = df[df['cause_fraction'].notnull()]

    full_coverage_df =\
        full_coverage_df[full_coverage_df['val'].notnull()]

    return df, full_coverage_df


def split_env(df, env_path, run_id, fix_norway_subnat=False):
    """
    split the envelope into smaller files and drop locations
    and years we don't use. This dramatically increases performance
    in the parallel jobs which multiply the env draws by cf draws
    Note: only needs to be run when a new envelope is created OR
    new hospital data locations are processed

    Parameters:
        df: Pandas DataFrame
        of hospital data, used to pull unique locations and years
        fix_norway_subnat: (bool)
        The envelope from May 2017 does not include norway subnats
        so let's just duplicate the national data. This will be
        obsolete when a new envelope is created/vetted
    """

    if env_path[-13:] == "2017_05_29.H5":
        h5_key = "table"
    else:
        h5_key = "df"
    # read in draw envelope
    env = pd.read_hdf(env_path, key=h5_key)
    env.rename(columns = {'year_start': 'year_id'}, inplace = True)
    env.drop(columns = ['year_end'], inplace = True)
    # drop n draws if it's present
    if "n_draws" in env.columns:
        env.drop('n_draws', axis=1, inplace=True)

    # drop the other groups with age_start == 1
    env = env[(env.age_start > 0) | (env.age_group_id == 28)]
    env = env[(env.age_start < 95) | (env.age_group_id == 235)]

    if fix_norway_subnat:
        # make a df of national and subnational locs
        nor_locs = [4910, 4911, 4912, 4913, 4914, 4915, 4916, 4917, 4918,
                    4919, 4920, 4921, 4922, 4923, 4926, 4927, 4928, 53432]
        # repeat norway's national location n times
        nat_locs = np.repeat(90, len(nor_locs))
        nor_subnat_df = pd.DataFrame({'sub_location_id': nor_locs, 'location_id': nat_locs})

        # merge env on by national loc
        nor_subnat_df = nor_subnat_df.merge(env, how='left', on='location_id')
        # set subnation loc to proper loc id
        nor_subnat_df.drop('location_id', axis=1, inplace=True)
        nor_subnat_df.rename(columns={'sub_location_id': 'location_id'}, inplace=True)
        # append the new locations onto the env
        env = pd.concat([env, nor_subnat_df], ignore_index=True)
        assert env[env.location_id.isin(nor_locs)].shape[0] > 5000,\
            "Some of the norway subnational locations {} weren't merged on. {}".\
            format(nor_locs, env[env.location_id.isin(nor_locs)].location_id.unique())

    # get unique loc_years
    loc_years = df[['location_id', 'year_id']].drop_duplicates()
    # print(loc_years.shape)

    base = FILEPATH.format(run_id)
    for sex in env.sex_id.unique():
        for age in env.age_start.unique():
            cond = "(env.age_start == age) & (env.sex_id == sex)"
            filepath = "/{}/{}_{}.H5".format(base, int(age), int(sex))
            dat = env[eval(cond)]

            # drop the location-years we don't want
            # print(dat.shape)
            dat = dat.merge(loc_years, how='right', on=['location_id', 'year_id'])
            # print(dat.shape)

            dat.to_hdf(filepath, key='df', mode='w', format='fixed')


def write_env_demo(df, run_id):
    path = FILEPATH.format(run_id)
    cols = ['location_id', 'age_group_id', 'year_id', 'sex_id']
    df[cols].drop_duplicates().to_csv(path + 'hosp_demographics.csv', index=False)

def prep_for_env_main(df, env_path, run_id, gbd_round_id, decomp_step, new_env_or_data=True, write=True,
                      create_hosp_denom=True, drop_data=True, fix_norway_subnat=False):
    """
    run everything, returns 2 DataFrames
    """
    # data is already dropped when doing age_sex splitting so this should not usually be needed after that
    if drop_data:
        df = hosp_prep.drop_data(df, verbose=False)

    # redistribute the poisoning drug baby seq
    df = rpd.redistribute_poison_drug(df)

    if not df.age_group_id.notnull().all():
        warnings.warn("""Shouldn't be any null age_group_id, there could be
        unsplit ages in the data, or maybe an age_start/age_end changed""")

    print("Splitting covered and uncovered sources...")
    df, full_coverage_df = split_sources(df, gbd_round_id=gbd_round_id, decomp_step=decomp_step)

    # df.rename(columns={'year_id': 'year_start'}, inplace=True)
    print('Creating Cause Fractions...')

    df = hosp_prep.create_cause_fraction(df,
                                         run_id=run_id,
                                         write_cause_fraction=True,
                                         create_hosp_denom=create_hosp_denom,
                                         store_diagnostics=True,
                                         tol=1e-6)


    print('Applying Restrictions...')
    df, full_coverage_df = apply_restricts(df, full_coverage_df)

    # df.rename(columns={'year_start': 'year_id'}, inplace=True)
    # full_coverage_df.rename(columns={'year_start': 'year_id'}, inplace=True)


    if new_env_or_data:
        split_env(df, env_path, run_id, fix_norway_subnat)

    # write demographic info for the envelope process
    write_env_demo(df, run_id)

    if write:
        base = FILEPATH.format(run_id)

        print("Writing the df file...")
        file_path = "{}/prep_for_env_df.H5".format(base)
        hosp_prep.write_hosp_file(df, file_path, backup=False)

        print("Writing the full_coverage_df file...")
        file_path_cover = "{}/prep_for_env_full_coverage_df.H5".format(base)
        hosp_prep.write_hosp_file(full_coverage_df, file_path_cover, backup=False)

    print("df columns are {}".format(df.columns))
    print("full cover df columns are {}".format(full_coverage_df.columns))
    return df, full_coverage_df
