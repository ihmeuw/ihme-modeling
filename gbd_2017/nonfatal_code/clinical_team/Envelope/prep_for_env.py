"""
A set of functions that prepare inpatient hospital data for the correction
factor/envelope uncertainty data
There are 5 rough steps
1) remove records from data sources we won't use
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

# load our functions
user = getpass.getuser()
repo = r"{FILEPATH}".format(user)

sys.path.append(r"{}/Functions".format(repo))
sys.path.append(r"{}/Envelope".format(repo))

import hosp_prep
import gbd_hosp_prep
import redistribute_poisoning_drug as rpd

if platform.system() == "Linux":
    root = r"{FILEPATH}"
else:
    root = "{FILEPATH}"


def split_sources(df):
    """
    Get Denominator aka sample_size for sources where we don't want to use
    envelope
    """
    # Make list of sources that we don't want to use envelope.  This should be
    # the source in the 'source' column of the dataframe

    full_coverage_sources = ["UK_HOSPITAL_STATISTICS"]

    if pd.Series(full_coverage_sources).isin(df.source.unique()).any():
        # make dataframe with sources
        full_coverage_df = df[df.source.isin(full_coverage_sources)].copy()

        # get denominator (aka population)
        full_coverage_df = gbd_hosp_prep.get_sample_size(full_coverage_df)

        full_coverage_df.rename(columns={'population': 'sample_size'},
                                inplace=True)

        # NOTE now there's going be a column "sample_size" that
        # is null for every source except the fully covered ones

        # make mean
        full_coverage_df['mean'] =\
            full_coverage_df.val / full_coverage_df.sample_size

    else:
        # make empty dataframe so that it exists and wont break anything
        full_coverage_df = pd.DataFrame(columns=list(df.columns) + ['mean'])

    df = df[~df.source.isin(full_coverage_sources)].copy()

    return df, full_coverage_df


def expand_bundles(df, drop_null_bundles=True):
    """
    This Function maps groups of ICD codes to Bundles.
    """

    assert "bundle_id" not in df.columns, "bundle_id has already been attached"
    assert "nonfatal_cause_name" in df.columns,\
        "'nonfatal_cause_name' must be a column"

    # merge on bundle id
    maps = pd.read_csv(root + r"{FILEPATH}clean_map.csv")
    assert hosp_prep.verify_current_map(maps)
    maps = maps[['nonfatal_cause_name', 'bundle_id']].copy()
    maps = maps.drop_duplicates()

    df = df.merge(maps, how='left', on='nonfatal_cause_name')

    if drop_null_bundles:
        # drop rows without bundle id
        df = df[df.bundle_id.notnull()]

    return(df)

def aggregate_to_bundle(df, col_to_sum):
    """
    Takes data mapped to groups of ICD codes that has bundle_id merged on, and
    aggregates it to the bundle level.

    Parameters:
        df: Pandas DataFrame
            Must have bundle_id merged on.
    """

    assert 'cause_fraction' in df.columns or 'mean' in df.columns, "missing cols"
    assert "bundle_id" in df.columns, "bundle_id must be attached"

    groups = ['location_id', 'year_start', 'year_end', 'age_group_id', 'sex_id',
              'nid', 'representative_id', 'source', 'facility_id', 'bundle_id']

    # groupby and sum to go from ICD group to bundle
    df = df.groupby(groups).agg({col_to_sum: 'sum'}).reset_index()

    return df

def apply_restricts(df, full_coverage_df):
    """
    Apply the age-sex restrictions after expanding data to the bundle level
    """

    df = hosp_prep.apply_bundle_restrictions(df, 'cause_fraction')
    full_coverage_df = hosp_prep.apply_bundle_restrictions(full_coverage_df,
                                                    'mean')
    # drop the restricted values
    df = df[df['cause_fraction'].notnull()]

    full_coverage_df =\
        full_coverage_df[full_coverage_df['mean'].notnull()]

    return df, full_coverage_df


def split_env(df, env_path):
    """
    split the envelope into smaller files and drop locations
    and years we don't use. This dramatically increases performance
    in the parallel jobs which multiply the env draws by cf draws
    Note: only needs to be run when a new envelope is created OR
    new hospital data locations are processed

    Parameters:
        df: Pandas DataFrame
        of hospital data, used to pull unique locations and years
    """

    if env_path[-13:] == "2017_05_29.H5":
        h5_key = "table"
    else:
        h5_key = "df"
    # read in draw envelope
    env = pd.read_hdf(env_path, key=h5_key)
    # drop n draws if it's present
    if "n_draws" in env.columns:
        env.drop('n_draws', axis=1, inplace=True)

    # drop the other groups with age_start == 1
    env = env[(env.age_start > 0) | (env.age_group_id == 28)]
    env = env[(env.age_start < 95) | (env.age_group_id == 235)]


    # get unique loc_years
    loc_years = df[['location_id', 'year_start']].drop_duplicates()
    # print(loc_years.shape)

    for sex in env.sex_id.unique():
        for age in env.age_start.unique():
            cond = "(env.age_start == age) & (env.sex_id == sex)"
            filepath = "{}/FILEPATH/{}_{}.H5".\
                format(root, int(age), int(sex))
            dat = env[eval(cond)]

            dat = dat.merge(loc_years, how='right', on=['location_id', 'year_start'])

            dat.to_hdf(filepath, key='df', mode='w', format='fixed')


def write_env_demo(df):
    write_vers = pd.read_csv(r"{FILEPATH}version_log.csv")
    write_vers = int(write_vers['version'].max()) + 1
    path = "FILEPATH/hosp_v{}_demographics.csv".format(write_vers)
    cols = ['location_id', 'age_group_id', 'year_start', 'year_end', 'sex_id']
    df[cols].drop_duplicates().to_csv(path, index=False)

def prep_for_env_main(df, env_path, new_env_or_data=False, write=False,
                      create_hosp_denom=False, drop_data=True):
    """
    run everything, returns 2 DataFrames
    """
    # ensure unwanted data sources are not retained
    if drop_data:
        df = hosp_prep.drop_data(df, verbose=False)

    # redistribute the poisoning drug icd groups
    df = rpd.redistribute_poison_drug(df)

    if not df.age_group_id.notnull().all():
        warnings.warn("""Shouldn't be any null age_group_id, there could be
        unsplit ages in the data, or maybe an age_start/age_end changed""")

    print("Splitting covered and uncovered sources...")
    df, full_coverage_df = split_sources(df)

    df = hosp_prep.create_cause_fraction(df,
                                     create_hosp_denom=create_hosp_denom,
                                     store_diagnostics=True,
                                     tol=1e-6)

    print("Expanding and aggregating to the bundle level...")
    df = expand_bundles(df, drop_null_bundles=True)
    df = aggregate_to_bundle(df, "cause_fraction")

    full_coverage_df = expand_bundles(full_coverage_df, drop_null_bundles=True)
    full_coverage_df = aggregate_to_bundle(full_coverage_df, "mean")

    df, full_coverage_df = apply_restricts(df, full_coverage_df)

    if new_env_or_data:
        split_env(df, env_path)

    # this annoying thing where it converts bundle id to an object
    df['bundle_id'] = df['bundle_id'].astype(float)
    full_coverage_df['bundle_id'] = full_coverage_df['bundle_id'].astype(float)

    # write demographic info for the envelope process
    write_env_demo(df)

    if write:
        print("Writing the df file...")
        file_path = "FILEPATH"\
                r"/prep_for_env_df.H5"
        hosp_prep.write_hosp_file(df, file_path, backup=True)
        print("Writing the full_coverage_df file...")
        file_path_cover = "FILEPATH"\
                r"/prep_for_env_full_coverage_df.H5"
        hosp_prep.write_hosp_file(full_coverage_df, file_path_cover, backup=True,
            include_version_info=True)

    print("df columns are {}".format(df.columns))
    print("full cover df columns are {}".format(full_coverage_df.columns))
    return df, full_coverage_df
