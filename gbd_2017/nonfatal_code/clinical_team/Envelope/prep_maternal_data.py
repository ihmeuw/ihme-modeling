"""
Set of functions to create or adjust the maternal denominators
"""

import datetime
import platform
import sys
import getpass
import warnings
import pandas as pd
import numpy as np
import os
import time
import functools
import glob
from db_tools.ezfuncs import query
from db_queries import get_population, get_cause_metadata, get_covariate_estimates

# load our functions
user = getpass.getuser()
prep_path = r"/FILEPATH".format(user)
sys.path.append(prep_path)
repo = r"/FILEPATH".format(user)
import hosp_prep
import gbd_hosp_prep
import agg_to_five_years as agg

if platform.system() == "Linux":
    root = ROOT
else:
    root = ROOT


def run_shared_funcs(mat):
    """
    get all the central inputs we'll need. Population and asfr and ifd covariates
    """
    years = list(np.arange(1988, 2018, 1))
    locs = mat.location_id.unique().tolist()
    ages = mat.age_group_id.unique().tolist()
    # get pop
    pop = get_population(age_group_id=ages, location_id=locs, year_id=years, sex_id=[2])
    
    # GET ASFR and IFD
    # has age/location/year
    asfr = get_covariate_estimates(covariate_id=13, location_id=locs, age_group_id=ages, year_id=years)
    ifd = get_covariate_estimates(covariate_id=51)
    return pop, asfr, ifd
    

def merge_pop_asfr_ifd(pop, asfr, ifd):
    """
    central inputs will be multiplied together to get ifd*asfr*population
    to do this we first need to merge the data together into the same df
    """
    # first merge asfr and ifd, NOTE ifd has no age related information
    new_denom = asfr.merge(ifd, how='outer', on=['location_id', 'sex_id', 'year_start'])
    # drop data before 1987, our earliest hosp year is 1988
    new_denom = new_denom[new_denom.year_start > 1987]
    # keep only female data
    new_denom = new_denom[new_denom.sex_id == 2]
    print(ifd.shape, asfr.shape, new_denom.shape)
    
    pai = pop.merge(new_denom, how='outer', on=['location_id', 'sex_id', 'age_group_id', 'year_start'])
    print(new_denom.shape, pai.shape)
    
    # create the new denominator
    pai['ifd_asfr_denom'] = pai['population'] * pai['asfr_mean'] * pai['ifd_mean']
    
    return pai


def clean_shared_output(pop, asfr, ifd):
    """
    prep the central inputs to work with our process
    """
    pop.drop('run_id', axis=1, inplace=True)
    pop.rename(columns={'year_id': 'year_start'}, inplace=True)

    keeps = ['location_id', 'sex_id', 'age_group_id', 'year_id', 'mean_value']
    asfr = asfr[keeps].copy()
    asfr.rename(columns={'year_id': 'year_start', 'mean_value': 'asfr_mean'}, inplace=True)
    
    ifd = ifd[keeps].copy()
    ifd.rename(columns={'year_id': 'year_start', 'mean_value': 'ifd_mean'}, inplace=True)
    # IFD doesn't return data with age or sex specified
    ifd.drop('age_group_id', axis=1, inplace=True)
    ifd['sex_id'] = 2
    
    return pop, asfr, ifd


def agg_to_five_years(pai):
    """
    We're comparing this data to our total maternal denom which are in
    5 year groups. Groupby demographic columns and sum the new denominator.
    We can do this b/c it's in count space
    """
    pai['year_end'] = pai['year_start']
    # group to 5 years and sum
    pai = hosp_prep.year_binner(pai)
    print(pai.shape)
    pai = pai.groupby(['age_group_id', 'location_id', 'sex_id', 'year_start', 'year_end']).\
        agg({'ifd_asfr_denom': 'sum'}).reset_index()
    print(pai.shape)
    return pai


def agg_pop_to_5(pop):
    """
    prep pop into 5 year groups to merge onto maternal data
    """
    agg_pop = pop.copy()
    agg_pop['year_end'] = agg_pop['year_start']
    agg_pop = hosp_prep.year_binner(agg_pop)
    agg_pop = agg_pop.groupby(['age_group_id', 'location_id', 'sex_id', 'year_start', 'year_end']).\
        agg({'population': 'sum'}).reset_index()
    print(agg_pop.shape)
    return agg_pop

def merge_on_agged_pop(mat_res, agg_pop):
    """
    We need to multiply population * mean_raw in order to go from rates to counts
    """
    pre = mat_res.shape[0]
    mat_res = mat_res.merge(agg_pop, how='left', on=['age_group_id', 'location_id', 'sex_id', 'year_start', 'year_end'])
    assert pre == mat_res.shape[0]

    mat_res['ifd_asfr_denom_rate'] = mat_res['ifd_asfr_denom'] / mat_res['population']
    mat_res.drop(['population'], axis=1, inplace=True)

    return mat_res


def write_maternal_denom(df, denom_type):
    """
    Write the maternal denominator data, updating this on 2/1/2018 with an option
    to use ifd-asfr denoms

    Parameters:
        df: Pandas DataFrame
    """
    if denom_type == 'ifd_asfr':
        # get the three estimates from central Dbs
        pop, asfr, ifd = run_shared_funcs(df)
        pop, asfr, ifd = clean_shared_output(pop, asfr, ifd)

        # multiply them together to get our new denom which is IFD * ASFR * POP
        mat_df = merge_pop_asfr_ifd(pop, asfr, ifd)
        # keep only good ages
        good_age_group_ids = [7,  8,  9, 10, 11, 12, 13, 14, 15]
        mat_df = mat_df[mat_df.age_group_id.isin(good_age_group_ids)]

        # agg to five years
        mat_df = agg_to_five_years(mat_df)

        # go to rate space
        agg_pop = agg_pop_to_5(pop)
        mat_df = merge_on_agged_pop(mat_df, agg_pop)
        mat_df.rename(columns={'ifd_asfr_denom_rate': 'mean_raw',
                               'ifd_asfr_denom': 'sample_size'}, inplace=True)


        # write denom to file
        mat_df.to_hdf(root + r"/FILEPATH", key='df', mode="w")
        # backup copy to _archive
        mat_df.to_hdf(root + r"/FILEPATH", key='df', mode='w')

    if denom_type == 'bundle1010':
        # select just the total_maternal bundle. it should have already been
        # selected so just in case:
        mat_df = df[df.bundle_id==1010].copy()

        # keep only allowed ages and sexes
        # NOTE could use hosp_prep.group_id_start_end_switcher(), but i want
        # age_gorup_id later for population.  however, this way is less flexible
        good_age_group_ids = [7,  8,  9, 10, 11, 12, 13, 14, 15]
        mat_df = mat_df[mat_df.age_group_id.isin(good_age_group_ids)].copy()
        mat_df = mat_df[mat_df.sex_id == 2].copy()

        if mat_df.shape[0] == 0:
            return

        # NOTE sample size is dropped here, and we make a new one in the
        # following code
        mat_df = mat_df[['location_id', 'year_start', 'year_end',
                         'age_group_id', 'sex_id',
                         'mean_raw', 'mean_incidence',
                         'mean_prevalence', 'mean_indvcf']].copy()

        # we can't use upper and lower for maternal adjusted data.
        bounds = mat_df.filter(regex="^upper|^lower").columns
        for uncertainty in bounds:
            mat_df[uncertainty] = np.nan

        mat_df['year_id'] = mat_df.year_start + 2  # makes 2000,2005,2010

        # create age/year/location lists to use for pulling population
        age_list = list(mat_df.age_group_id.unique())
        loc_list = list(mat_df.location_id.unique())
        year_list = list(mat_df.year_id.unique())

        # GET POP ########################################################
        # pull population and merge on age_start and age_end
        pop = get_population(age_group_id=age_list, location_id=loc_list,
                             sex_id=[1, 2], year_id=year_list)

        # FORMAT POP ####################################################
        pop.drop(['run_id'], axis=1,
                 inplace=True)

        # MERGE POP ######################################################
        demography = ['location_id', 'year_id', 'age_group_id', 'sex_id']

        pre_shape = mat_df.shape[0]  # store for before comparison
        # then merge population onto the hospital data

        # attach pop info to df
        mat_df = mat_df.merge(pop, how='left', on=demography)
        assert pre_shape == mat_df.shape[0], ("number of rows don't "
            "match after merge")


        # MAKE SAMPLE SIZE  ##############################################
        mat_df['sample_size'] = mat_df.population * mat_df.mean_raw

        # DROP intermidiate columns
        mat_df.drop(['population', 'year_id'], axis=1, inplace=True)

        # print("before writing denoms", mat_df.info())
        mat_df.sex_id = mat_df.sex_id.astype(int)
        mat_df.location_id = mat_df.location_id.astype(int)
        # print(mat_df.info())

        mat_df.to_hdf(root + r"/FILEPATH", key='df', mode="w")
        # backup copy to _archive
        mat_df.to_hdf(root + r"/FILEPATH", key='df', mode='w')


def select_maternal_data(df):
    """
    Function that filters out non maternal data. Meant to be ran at the start
    of this process.  If we are only interested in adjusting the maternal denom,
    then we don't need non maternal data.

    Parameters:
        df: Pandas DataFrame
            Must have 'bundle_id' as a column
    """

    assert "bundle_id" in df.columns, "'bundle_id' must be a column."

    # get causes
    causes_5 = get_cause_metadata(cause_set_id=9, gbd_round_id=5) # round 5 was updated to keep bundles 79 and 646
    causes_4 = get_cause_metadata(cause_set_id=9, gbd_round_id=4)

    causes = pd.concat([causes_4, causes_5])
    causes.drop_duplicates(inplace = True)

    # create condiational mask that selects maternal causes
    condition = causes.path_to_top_parent.str.contains("366")

    # subset just causes that meet the condition sdf
    maternal_causes = causes[condition]

    # make list of maternal causes
    maternal_list = list(maternal_causes['cause_id'].unique())

    # get bundle to cause map
    bundle_cause = query("QUERY",
                         conn_def=DATABASE)

    # merge cause_id onto data
    df = df.merge(bundle_cause, how='left', on='bundle_id')

    # keep only maternal causes in df
    df = df[df['cause_id'].isin(maternal_list)]

    # drop cause_id
    df.drop('cause_id', axis=1, inplace=True)

    # drop the denominator bundle
    df = df[df['bundle_id'] != 1010]

    return df

def apply_corrections(df):
    """
    Note, this was reworked a little to work with the maternal denominators

    Applies the marketscan correction factors to the hospital data at the
    bundle level.  The corrections are merged on by 'age_start', 'sex_id',
    and 'bundle_id'.  Reads in the corrections from 3 static csv.

    With the new cf uncertainty our process has been updated and this only
    applies to the sources with full care coverage.

    Parameters:
        df: Pandas DataFrame
            Must be aggregated and collapsed to the bundle level.
    """

    assert "bundle_id" in df.columns, "'bundle_id' must exist."
    assert "nonfatal_cause_name" not in df.columns, ("df cannot be at the baby ",
        "sequelae level")

    start_columns = df.columns

    # get a list of files, 1 for each type of CF
    corr_files = glob.glob(root + r"/FILEPATH")
    corr_list = []  # to append the CF DFs to
    for f in corr_files:
        # pull out the name of the correction type
        draw_name = os.path.basename(f)[:-6]
        # read in a file
        dat = pd.read_csv(f)
        pre_rows = dat.shape[0]
        # get the draw col names
        draw_cols = dat.filter(regex=draw_name).columns
        assert len(draw_cols) == 1000, "wrong number of draw cols"

        # create the single mean value from all the draws
        dat[draw_name] = dat[draw_cols].median(axis=1)
        # drop the draw cols
        dat.drop(draw_cols, axis=1, inplace=True)
        assert dat.shape[0] == pre_rows, "The number of rows changed"
        corr_list.append(dat)

    # merge the dataframes in the list together
    correction_factors = functools.reduce(lambda x, y: pd.merge(x, y,
            on=['age_start', 'sex', 'bundle_id']), corr_list)

    # rename columns to match df
    correction_factors.rename(columns={'sex': 'sex_id'}, inplace=True)

    # switch from age group id to age start/end
    df = hosp_prep.group_id_start_end_switcher(df)

    pre_shape = df.shape[0]
    # merge corr factors onto data
    df = df.merge(correction_factors, how='left', on=['age_start', 'sex_id',
                  'bundle_id'])
    assert pre_shape == df.shape[0] , ("You unexpectedly added rows while "
        "merging on the correction factors. Don't do that!")

    # apply the corrections.
    for level in ["incidence", "prevalence", "indvcf"]:
        df["mean_" + level] = \
            df["mean_raw"] * df[level]

    # switch from age_start and age_end back to age_group_id
    df = hosp_prep.group_id_start_end_switcher(df)

    # drop the CF cols. We'll add them manually later for all sources
    df.drop(['incidence', 'prevalence', 'indvcf'], axis=1, inplace=True)

    assert set(start_columns).issubset(set(df.columns)), """
        Some columns that were present at the start are missing now"""

    return(df)


def adjust_maternal_denom(df, denom_type):
    """
    Function that adjusts the maternal bundles by dividing each of them by
    bundle 1010.

    Parameters:
        df: Pandas DataFrame
    """

    if "sample_size" in df.columns:
        df.drop('sample_size', axis=1, inplace=True)
    if "mean_inj" in df.columns:
        df.drop('mean_inj', axis=1, inplace=True)

    # keep only allowed ages and sexes
    good_age_group_ids = [ 7,  8,  9, 10, 11, 12, 13, 14, 15]
    df = df[df.age_group_id.isin(good_age_group_ids)].copy()
    df = df[df.sex_id == 2].copy()

    denom = pd.read_hdf("/FILEPATH", key="df")

    # rename denominator columns before merging
    denom_cols = sorted(denom.filter(regex="^mean").columns)
    # list comprehension to make new col names
    new_denom_cols = [x + "_denominator" for x in denom_cols]
    denom.rename(columns=dict(zip(denom_cols, new_denom_cols)), inplace=True)

    # merge on denominator
    pre = df.shape[0]
    # set cols to merge onto df
    merge_on = ['location_id', 'year_start', 'year_end', 'age_group_id', 'sex_id']

    df = df.merge(denom, how='left', on=merge_on)
    assert pre == df.shape[0], ("shape should not have changed "
        "during merge")

    df = df[(df['mean_raw'] > 0) | (df['mean_raw_denominator'].notnull())]

    assert df.mean_raw_denominator.isnull().sum() == 0, ("shouldn't be "
        "any null values in this column")

    num_cols = sorted(df.filter(regex="^mean.*[^(tor)]$").columns)

    if denom_type == "bundle1010":
        # num cols and denom cols should be same length
        assert len(num_cols) == len(new_denom_cols),\
            "the denom cols and num are different lengths, gonna break the division {} and {}".format(num_cols, new_denom_cols)

        # make sure the col names line up, ie that "mean_incidence" will be
        # divided by "mean_incidence_denominator"
        for i in np.arange(0, len(new_denom_cols), 1):
            print(num_cols[i], new_denom_cols[i], new_denom_cols[i][:-12])
            assert num_cols[i] == new_denom_cols[i][:-12]

        # divide each bundle value by maternal denom to get the adjusted rate
        for i in np.arange(0, len(new_denom_cols), 1):
            df[num_cols[i]] = df[num_cols[i]] / df[new_denom_cols[i]]

    # for ifd asfr method we divide mean raw by adjusted denom then multiply by our scalars
    if denom_type == 'ifd_asfr':
        # divide mean raw by mean raw denom
        df['mean_raw'] = df['mean_raw'] / df['mean_raw_denominator']

        # apply the CFs to mean raw
        for cf_type in ['indvcf', 'incidence', 'prevalence']:
            df["mean_" + cf_type] =\
                df["mean_raw"] * df["cf_mean_" + cf_type]


    # drop the denominator columns
    df.drop(new_denom_cols, axis=1, inplace=True)

    # the current upper and lower cols are not methodologically correct
    # fill them with NaNs
    bounds = df.filter(regex="^upper|^lower").columns
    for uncertainty in bounds:
        df[uncertainty] = np.nan

    # can't divide by zero
    df = df[df['sample_size'] != 0]
    # RETURN ONLY THE MATERNAL DATA

    return(df)


def prep_maternal_main(df, write_denom=True, write=False, denom_type='ifd_asfr'):
    back = df.copy()

    # if write the maternal denoms
    if write_denom:
        df = df[df['bundle_id'] == 1010].copy()

        df = agg.agg_to_five_main(df, write=False, maternal=True)

        # write the actual denominator file
        write_maternal_denom(df, denom_type=denom_type)

    # then adjust the maternal data
    df = back.copy()  # get the backup copy of data
    df = select_maternal_data(df)  # subset just maternal causes
    df = agg.agg_to_five_main(df, write=False, maternal=True)

    df = adjust_maternal_denom(df, denom_type=denom_type)

    # data types constantly changing, probably from merges
    cols_to_numer = ['bundle_id', 'location_id', 'sex_id',
                    'year_start', 'year_end', 'age_group_id']
    for col in cols_to_numer:
        df[col] = pd.to_numeric(df[col], errors='raise')

    if write:
        print("Writing the df file...")
        file_path = "/FILEPATH"
        hosp_prep.write_hosp_file(df, file_path, backup=True,
            include_version_info=True)

    return df
