"""
Set of functions to agg to five years
"""

import datetime
import platform
import sys
import getpass
import warnings
import pandas as pd
import numpy as np
import os
import glob
import time
import functools
from db_tools.ezfuncs import query
from db_queries import get_population, get_cause_metadata, get_covariate_estimates, get_location_metadata

# load our functions
user = getpass.getuser()
prep_path = r"{FILEPATH}Functions".format(user)
sys.path.append(prep_path)
repo = r"{FILEPATH}".format(user)
import hosp_prep
import gbd_hosp_prep

if platform.system() == "Linux":
    root = r"DRIVE"
else:
    root = "DRIVE"


def rename_raw_cols(df):
    df.rename(columns={"mean": "mean_raw", "lower": "lower_raw",
                        "upper": "upper_raw"}, inplace=True)
    return df


def make_square(df):
    """
    Function that inserts zeros for demographic and etiolgy groups that were not
    present in the source.  A zero is inserted for every age/sex/etiology
    combination for each location and for only the years available for that
    location. If a location has data from 2004-2005 we will create explicit
    zeroes for those years but not for any others.

    Age and sex restrictions are applied after the data is made square.

    Parameters:
        df: Pandas DataFrame
            Must be aggregated and collapsed to the bundle level.
    """
    start_square = time.time()
    assert "bundle_id" in df.columns, "'bundle_id' must exist"
    assert "nonfatal_cause_name" not in df.columns, ("Data must not be at the",
        " nonfatal_cause_name level")

    # create a series of sorted, non-zero mean values to make sure
    # the func doesn't alter anything
    check_mean = df.loc[df['mean_raw'] > 0, 'mean_raw'].sort_values().\
        reset_index(drop=True)

    # square the dataset
    df = hosp_prep.make_zeros(df, etiology='bundle_id',
                              cols_to_square=['mean_raw'], #, 'upper_raw', 'lower_raw'],
                              icd_len=5)

    # assert the sorted means are identical
    assert (check_mean == df.loc[df['mean_raw'] > 0, 'mean_raw'].sort_values().\
        reset_index(drop=True)).all()

    for col in ['mean_raw']: #, 'upper_raw', 'lower_raw']:
         df = hosp_prep.apply_bundle_restrictions(df, col)

    # assign upper and lower to 0 where mean is 0
    df.loc[df['mean_raw'] == 0, ['upper_raw', 'lower_raw']] = 0

    print("Data made square and age/sex restrictions applied in {} min".\
        format((time.time() - start_square)/60))

    return df


def merge_population(df):
    """
    Function that attaches population info to the DataFrame.  Checks that there
    are no nulls in the population columns.  This has to be run after the data
    has been made square!

    Parameters:
        df: Pandas DataFrame
    """

    assert (df.mean_raw == 0).any(), """There are no rows with zeros, implying
        that the data has not been made square.  This function should be run
        after the data is square"""

    # create age/year/location lists to use for pulling population
    age_list = list(df.age_group_id.unique())
    loc_list = list(df.location_id.unique())
    year_list = list(df.year_start.unique())

    # pull population
    pop = get_population(age_group_id=age_list, location_id=loc_list,
                         sex_id=[1, 2], year_id=year_list)

    # rename pop columns to match hospital data columns
    pop.rename(columns={'year_id': 'year_start'}, inplace=True)
    pop['year_end'] = pop['year_start']


    demography = ['location_id', 'year_start', 'year_end', 'sex_id',
                  'age_group_id']

    pre_shape = df.shape[0]  # store for before comparison
    # then merge population onto the hospital data

    df = df.merge(pop, how='left', on=demography)  # attach pop info to hosp
    assert pre_shape == df.shape[0], "number of rows don't match after merge"

    # assert that there are no nulls in population column:
    hosp_prep.report_if_merge_fail(df, check_col="population",
                                   id_cols=demography, store=True,
                                   filename="population_merge_failure")

    return df


def rate_to_count(df):
    """
    Converts information in ratespace to countspace by mutliplying rates by
    population.

    Parameters:
        df: Pandas DataFrame
            Must already have population information.
    """

    assert "population" in df.columns, "'population' has not been attached yet."
    assert {'mean_raw', 'upper_raw', 'lower_raw'}.issubset(df.columns)

    # we want to take every single rate column into count space
    cols_to_count = df.filter(regex="^mean|^upper|^lower").columns

    # do the actual conversion
    for col in cols_to_count:
        df[col + "_count"] = df[col] * df['population']

    # Drop the rate columns
    df.drop(cols_to_count, axis=1, inplace=True)

    return df


def split_covered_sources(df):
    """
    Function that splits the data into two pieces: one for sources that
    fully cover their population, and another for sources that do not.
    The data is split based on a list of sources that is hard coded into
    this function.

    RETURNS TWO DATAFRAMES, the SECOND will be the dataframe with fully
    covered populations

    Example call:
        df, covered_df = split_covered_sources(df)

    Parameters:
        df: Pandas DataFrame
    """

    # make df of data where there is full coverage (i.e., the UK)
    full_coverage_sources = ["UK_HOSPITAL_STATISTICS"]

    # make condition mask that indicates rows that have full coverage
    has_full_coverage = df.source.isin(full_coverage_sources)

    # make dataframe that only contains fully covered sources
    covered_df = df[has_full_coverage].copy()

    # drop this data from the main dataframe
    df = df[~has_full_coverage]

    return df, covered_df


def merge_denominator(df):
    """
    Merges denominator stored from when cause fractions were run.  Returns df
    with a column named "denominator".  Meant to be run after data has been
    made square.

    Parameters:
        df: Pandas DataFrame
    """

    warnings.warn("""

                  ENSURE THAT THE DENOMINATORS FILE IS UP TO DATE.
                  the file was last edited at {}

                  """.format(time.strftime('%Y-%m-%d %H:%M:%S',
                  time.localtime(os.path.getmtime('{FILEPATH}/hospital_denominators.csv'
                  )))))

    pre = df.shape[0]

    df = df.merge(pd.read_csv(r"{FILEPATH}"
                              r"hospital_denominators.csv"),
                  how='left', on=["age_group_id", "sex_id",
                                  "year_start", "year_end", "location_id"])

    assert pre == df.shape[0], "number of rows changed after merge"

    # check that data is zero when denominator is null
    assert (df.loc[df.denominator.isnull(), 'mean_raw_count'] == 0).all(), ("mean_raw_count"
        " should be 0")
    assert (df.loc[df.denominator.isnull(), 'lower_raw_count'] == 0).all(), ("lower_raw_count"
        " should be 0")
    assert (df.loc[df.denominator.isnull(), 'upper_raw_count'] == 0).all(), ("upper_raw_count"
        " should be 0")

    print("pre null denom drop shape", df.shape)
    df = df[df.denominator.notnull()]
    print("post null denom", df.shape)

    return df


def aggregate_to_dismod_years(df):
    """
    Function to collapse data into 5-year bands.  This is done in order to
    reduce the strain on on dismod by reducing the number of rows of data. Data
    must already be square! Data must be in count space!

    Before we have years like 1990, 1991, ..., 2014, 2015.  After we will have:
        1988-1992
        1993-1997
        1998-2002
        2003-2007
        2008-2012
        2013-2017

    Parameters:
        df: Pandas DataFrame
            Contains data at the bundle level that has been made square.
    """

    assert (df.mean_raw_count == 0).any(), """There are no rows with zeros, implying
        that the data has not been made square.  This function should be ran
        after the data is square"""

    assert {"mean_raw_count", "upper_raw_count", "lower_raw_count"}.\
        issubset(df.columns), """The columns mean_raw_count, upper_raw_count,
            lower_raw_count, are not all present, implying that the data has
            not been coverted into count space."""


    # first change years to 5-year bands
    df = hosp_prep.year_binner(df)

    # we are mixing data from different NIDs together.  There are special NIDs
    # for aggregated data
    df = hosp_prep.apply_merged_nids(df, assert_no_nulls=False, fillna=True)

    if "sample_size" in df.columns:
        # then df already has sample size
        df.drop("sample_size", axis=1, inplace=True)

    # rename "denominator" to "sample_size" in df (not covered_df)
    df.rename(columns={"denominator": "sample_size"}, inplace=True)

    cols_to_sum = df.filter(regex="count$").columns.tolist()
    cols_to_sum = cols_to_sum + ['population', 'sample_size']
    pre_cases = 0
    for col in cols_to_sum:
        pre_cases += df[col].sum()

    groups = ['location_id', 'year_start', 'year_end', 'age_group_id', 'sex_id',
              'nid', 'representative_id', 'bundle_id']

    sum_dict = dict(zip(cols_to_sum, ["sum"] * len(cols_to_sum)))
    df = df.groupby(groups).agg(sum_dict).reset_index()

    post_cases = 0
    for col in cols_to_sum:
        post_cases += df[col].sum()

    accepted_loss = 300
    print("The acceptable loss is {} cases".format(accepted_loss))
    assert abs(pre_cases - post_cases) < accepted_loss,\
        ("some cases were lost. "
         "From {} to {}. A difference of {}".format(pre_cases, post_cases, abs(pre_cases - post_cases)))

    # set sample size to np.nan when mean/upper/lower are greater than 0
    df.loc[(df['mean_raw_count'] > 0) &
           (df['lower_raw_count'] > 0) &
           (df['upper_raw_count'] > 0), 'sample_size'] = np.nan

    return df


def aggregate_covered_sources(covered_df):
    """
    Aggregate sources with full coverage into 5 year bands. Before we have
    years like 1990, 1991, ..., 2014, 2015.  After we will have:
        1988-1992
        1993-1997
        1998-2002
        2003-2007
        2008-2012
        2013-2017

    Parameters:
        covered_df: Pandas DataFrame
            df containing the covered sources.
    """

    assert "mean_raw_count" in covered_df.columns, """The column 'mean_raw_count' is
        not present in the columns of covered_df, implying that the df hasn't
        been transformed into count space."""


    # first change years to 5-year bands
    covered_df = hosp_prep.year_binner(covered_df)

    # we are mixing data from different NIDs together.  There are special NIDs
    # for aggregated data
    covered_df = hosp_prep.apply_merged_nids(covered_df)

    # These are the same groups as for the other df.
    groups = ['location_id', 'year_start', 'year_end', 'age_group_id', 'sex_id',
              'nid', 'representative_id', 'bundle_id']

    pre_cases = covered_df['mean_raw_count'].sum()

    covered_df = covered_df.groupby(groups)\
        .agg({'mean_raw_count':'sum',
              'population': 'sum'}).reset_index()

    assert round(pre_cases, 0) == round(covered_df['mean_raw_count'].sum(), 0),\
        ("some cases were lost. "
         "From {} to {}".format(pre_cases, covered_df['mean_raw_count'].sum()))

    return covered_df


def count_to_rate(df, covered_df):
    """
    Function that transforms from count space to rate space by dividing by
    the (aggregated) population.  Returns two dataframes.  This
    performs the transformation for each dataframe separatly.
    Pass in both dataframes!
    The main part of hosital data, i.e., sources without full coverage, is
    returned first.  Second item returned is the fully covered sources.

    Example call:
        not_fully_covered, full_covered = count_to_rate(df, covered_df)

    Parameters:
        df: Pandas DataFrame
            df containing most hospital data, the sources that are not fully
            covered.
        covered_df: Pandas DataFrame
            df containing the covered sources.
    """

    # get a list of the count columns
    cnt_cols = df.filter(regex="count$").columns

    for col in cnt_cols:
        new_col_name = col[:-6]
        print(new_col_name, col)
        df[new_col_name] = df[col] / df['population']

    # drop the count columns
    df.drop(cnt_cols, axis=1, inplace=True)

    # for covered_df
    covered_df['mean_raw'] = covered_df['mean_raw_count'] / covered_df['population']
    covered_df.drop('mean_raw_count', axis=1, inplace=True)

    return df, covered_df


def concat_covered_sources(df, covered_df):
    """
    Reattaches the data that was split up into fully covered and not fully
    covered sources.  Both data frames should be in rate space!

    Parameters:
        df: Pandas DataFrame
            df containing most hospital data, the sources that are not fully
            covered.
        covered_df: Pandas DataFrame
            df containing the covered sources.
    """

    # this drops the population that was merged on for coverting to counts.
    df.drop('population', axis=1, inplace=True)  # don't need pop anymore

    covered_df.rename(columns={"population": "sample_size"}, inplace=True)

    # bring covered_df and df together.
    # where we have full coverage, lower and upper should be null
    # mean_raw will never be null
    df = pd.concat([df, covered_df], ignore_index=True)

    return df


def get_parent_injuries(df):
    """
    roll up the child causes into parents add parent injuries

    Parameters:
        df: Pandas DataFrame
    """

    pc_injuries = pd.read_csv(root + r"{FILEPATH}"\
                              "parent_child_injuries_gbd2017.csv")

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

    # reorder
    col_before = parent_df.columns
    parent_df = parent_df[['modelable_entity_id', 'parent_me_id', 'bundle_id',
                           'parent_bundle_id']].copy()
    assert set(col_before) == set(parent_df.columns),\
        'you dropped a column while reordering columns'

    # create data for parent injuries
    parent_df = parent_df.merge(df, how='left', on='bundle_id')

    # drop child ME and bundle IDs
    parent_df.drop(['modelable_entity_id', 'bundle_id'], axis=1, inplace=True)
    # add parent bundle and ME IDs
    parent_df.rename(columns={'parent_me_id': 'modelable_entity_id',
                              'parent_bundle_id': 'bundle_id'}, inplace=True)

    parent_df.drop('modelable_entity_id', axis=1, inplace=True)

    assert set(parent_df.columns).symmetric_difference(set(df.columns)) == set()

    groups = ['location_id', 'year_start', 'year_end',
              'age_group_id', 'sex_id', 'nid',
              'representative_id',
              'bundle_id']

    cols_to_sum = df.filter(regex="^mean|^upper|^lower").columns.tolist()
    cols_to_sum = cols_to_sum + ['sample_size']
    # sum parent rows of data together
    parent_df = parent_df.groupby(groups).agg(\
        dict(zip(cols_to_sum, ['sum'] * len(cols_to_sum)))).reset_index() # keep sample size

    # set sample size to np.nan when upper exists and isn't zero
    parent_df.loc[(parent_df.upper_raw != 0) &\
                (parent_df.upper_raw.notnull()) &\
                (parent_df.sample_size.notnull()), 'sample_size'] = np.nan
    # rbind duped parent data back onto hospital data
    df = pd.concat([df, parent_df]).reset_index(drop=True)
    return(df)


def add_measure(df):
    """
    Adds bundle measure onto the df and renames the cols to fit with
    remaining process.  Data must already have bundle_id attached.

    Parameters:
        df: Pandas DataFrame
            Must have a 'bundle_id' column
    """

    assert "measure" not in df.columns, "'measure' already exists."
    assert "bundle_id" in df.columns, "'bundle_id' must exist."

    # Read in clean maps to merge measure onto data
    # need this for ELMO reqs
    clean_maps = pd.read_csv(root + r"{FILEPATH}"
                             r"clean_map.csv")
    assert hosp_prep.verify_current_map(clean_maps)
    clean_maps = clean_maps[['bundle_id', 'bid_measure']]
    clean_maps.drop_duplicates(inplace=True)
    clean_maps.rename(columns={'bid_measure': 'measure'}, inplace=True)

    # remove null bundle ids in map
    clean_maps = clean_maps[clean_maps.bundle_id.notnull()]

    # check to make sure rows aren't duplicated
    pre_shape = df.shape[0]
    # merge measure onto data
    df = df.merge(clean_maps, how='left', on='bundle_id')

    # get injuries bids so we can check for missing measures
    pc_injuries = pd.read_csv(root + r"{FILEPATH}"
                              r"parent_child_injuries_gbd2017.csv")
    inj_bids = pc_injuries['Level1-Bundle ID'].unique()

    # some injuries bids didn't get measures!
    assert set(df[df.measure.isnull()].bundle_id).issubset(set(inj_bids)), """
        We expect that all null measures belong to injuries, but that is
        not the case. Something went wrong!"""

    # fix any injuries that are missing measure, all inj are inc:
    df.loc[(df.measure.isnull()) &
           (df.bundle_id.isin(inj_bids)), 'measure'] = 'inc'

    # assert that all rows had a measure merged on
    hosp_prep.report_if_merge_fail(df, check_col='measure', id_cols='bundle_id',
                                   store=True, filename="measure_merge_failure")

    assert pre_shape == df.shape[0], "number of rows don't match after merge"

    return(df)

def get_5_year_haqi_cf(min_treat=10, max_treat=75):
    """
    A function to get the health access quality covariates data which we'll
    use to divide our mean_raw values by to adjust our estimates

    Parameters:
        min_treat: float
            minimum access. Sets a floor for the CF. If 10 then the lowest possible CF will be 10,
        max_treat: float or int
            maximum acess. Sets a cap for the CF. If 75 then any loc/year with a covariate above 75
            will have a CF of 1 and the data will be unchanged
    """
    # get a dataframe of haqi covariate estimates
    df = get_covariate_estimates(covariate_id=1099)
    df.rename(columns={'year_id': 'year_start'}, inplace=True)

    # set the max value
    df.loc[df.mean_value > max_treat, 'mean_value'] = max_treat

    # get min df present in the data
    min_df = df.mean_value.min()

    # make the correction
    df['haqi_cf'] = \
        min_treat + (1 - min_treat) * ((df['mean_value'] - min_df) / (max_treat - min_df))

    # drop the early years so year binner doesn't break
    df['year_end'] = df['year_start']
    df = df[df.year_start > 1987].copy()
    df = hosp_prep.year_binner(df)

    # Take the average of each 5 year band
    df = df.groupby(['location_id', 'year_start', 'year_end']).agg({'haqi_cf': 'mean'}).reset_index()

    assert df.haqi_cf.max() <= 1, "The largest haqi CF is too big"
    assert df.haqi_cf.min() >= min_treat, "The smallest haqi CF is too small"

    return df

def apply_haqi_corrections(df):
    """
    merge the haqi correction (averaged over 5 years) onto the hospital data
    """
    haqi = get_5_year_haqi_cf()

    pre = df.shape
    df = df.merge(haqi, how='left', on=['location_id', 'year_start', 'year_end'])
    assert pre[0] == df.shape[0],\
        "DF row data is different. That's not acceptable. Pre shape {}. Post shape {}".\
        format(pre, df.shape)
    assert df.haqi_cf.isnull().sum() == 0,\
        "There are rows with a null haqi value. \n {}".format(\
            df[df.haqi_cf.isnull()])

    return df

def apply_corrections(df, use_modified):
    """
    Applies the marketscan correction factors to the hospital data at the
    bundle level.  The corrections are merged on by 'age_start', 'sex_id',
    and 'bundle_id'.

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
    if use_modified:
        corr_files = glob.glob(root + r"{FILEPATH}/mod_*.csv")
        idx = -4
        id_cols = ['age_start', 'sex_id', 'cf_location_id', 'bundle_id']

    else:
        corr_files = glob.glob(root + r"{FILEPATH}/*sm.csv")
        idx = -6
        id_cols = ['age_start', 'sex', 'bundle_id']

    corr_list = []  # to append the CF DFs to
    cf_names = []  # to apply the cfs
    for f in corr_files:
        # pull out the name of the correction type
        draw_name = os.path.basename(f)[:idx]
        if use_modified:
            draw_name = draw_name[4:]
        cf_names.append(draw_name)
        # read in a file
        dat = pd.read_csv(f)
        # rename the mean draw name cols back to just draw name
        if use_modified:
            dat.rename(columns={'mean_' + draw_name: draw_name}, inplace=True)
        if "Unnamed: 0" in dat.columns:
            dat.drop("Unnamed: 0", 1, inplace = True) 
        pre_rows = dat.shape[0]

        # only need to take the mean if it's not modeled/modifed CF data
        if not use_modified:
            # get the draw col names
            draw_cols = dat.filter(regex=draw_name).columns
            assert len(draw_cols) == 1000, "wrong number of draw cols"

            # create the single mean value from all the draws
            dat[draw_name] = dat[draw_cols].mean(axis=1)
            # drop the draw cols
            dat.drop(draw_cols, axis=1, inplace=True)

        assert dat.shape[0] == pre_rows, "The number of rows changed"
        corr_list.append(dat)

        del dat

    # merge the dataframes in the list together
    correction_factors = functools.reduce(lambda x, y: pd.merge(x, y,
            on=id_cols), corr_list)

    if 'sex' in correction_factors.columns:
        # rename columns to match df
        correction_factors.rename(columns={'sex': 'sex_id'}, inplace=True)

    # switch from age group id to age start/end
    df = hosp_prep.group_id_start_end_switcher(df)

    # switch from sex to sex id in our identifier columns
    id_cols = [f + "_id" if f == "sex" else f for f in id_cols]

    pre_shape = df.shape[0]
    if not use_modified:
        # merge corr factors onto data
        df = df.merge(correction_factors, how='left', on=id_cols)

    if use_modified:
        # merge country id aka cf loc id, onto the data in order for the later merge to work
        locs = get_location_metadata(location_set_id=35)[['location_id', 'path_to_top_parent']]
        locs = pd.concat([locs, locs.path_to_top_parent.str.split(",", expand=True)], axis=1)
        locs = locs[locs[3].notnull()]
        locs['cf_location_id'] = locs[3].astype(int)
        locs = locs[['cf_location_id', 'location_id']]
        df = df.merge(locs, how='left', on='location_id')

        # merge CFs onto hosp data
        df = df.merge(correction_factors, how='left', on=id_cols)


    assert pre_shape == df.shape[0] , ("You unexpectedly added rows while "
        "merging on the correction factors. Don't do that!")

    # drop unneeded cols
    for col in ['super_region_id', 'model_prediction', 'cf_location_id']:
        if col in df.columns:
            df.drop(col, axis=1, inplace=True)

    # apply the mean, smoothed corr factors without the env to covered sources
    full_coverage_sources = ["UK_HOSPITAL_STATISTICS"]

    # apply the corrections.
    for level in cf_names:
        df.loc[df.source.isin(full_coverage_sources), "mean_" + level] = \
            df.loc[df.source.isin(full_coverage_sources), "mean_raw"] *\
            df.loc[df.source.isin(full_coverage_sources), level]

    # switch from age_start and age_end back to age_group_id
    df = hosp_prep.group_id_start_end_switcher(df)

    # drop the CF cols. We'll add them manually later for all sources
    df.drop(cf_names, axis=1, inplace=True)

    assert set(start_columns).issubset(set(df.columns)), """
        Some columns that were present at the start are missing now"""

    return(df)


def cap_corrections(df, view_corrections=False, use_modified=False):
    """
    Applies the caps to the corrected data with exceptions.  The logic is if
    the corrections are very large then small differences in raw data will be
    magnified.  The data becomes too unstable and is deemed unusable.

    RULE = if correction factor is greater than 50, make the data null
    EXCEPTIONS are made for these bundles, which are capped at 100:
        Preterm: 80, 81, 82, 500
        Encephalopathy: 338
        Sepsis: 92
        Hemoloytic: 458
        PAD/PUD: 345
        Cirrhosis: 131

    Parameters:
        df: Pandas DataFrame
    """

    # list of bundles which can have correction factors above 50
    cf_exceptions = [345, 80, 81, 82, 500, 338, 92, 458, 131]

    # conditional mask for the exeptions
    exception_condition = df.bundle_id.isin(cf_exceptions)

    # get a list of all the rate columns
    rate_cols = df.filter(regex="^mean|^lower|^upper").columns

    # there should be exactly 12 columns 3 unc types and 4 corr types
    assert len(rate_cols) == 12,\
        "The regex to pull the rate columns failed. cols are {}".format(rate_cols)

    for col in rate_cols:
        # the denominator is always the raw value, extract the type here
        # it should be mean or upper or lower
        raw_col = col.split("_")[0] + "_raw"
        df["cf_" + col] = df[col] / df[raw_col]

    if not use_modified:  # we only cap the older CFs
        # now do the actual capping. We will use only a single correction factor
        # from the mean
        # drop all non mean columns
        cap_cols = [i for i in rate_cols if "mean" in i]
        for col in cap_cols:
            to_cap = [col, "upper_" + col[5:], "lower_" + col[5:]]
            df.loc[(~exception_condition) &
                   (df["cf_" + col] > 50), to_cap] = np.nan
            df.loc[(exception_condition) &
                   (df["cf_" + col] > 100), to_cap] = np.nan

    # test it to make sure the caps worked and removed upper and lower
    for col in ['incidence', 'prevalence', 'indvcf']:
        # when upper is null and it's not UTLA data confirm that rows where mean
        # is not null equal zero
        assert df[(df["upper_" + col].isnull()) &\
                (df["mean_" + col].notnull()) &\
                (df['source'] != "UK_HOSPITAL_STATISTICS")].shape[0] == 0,\
                "There are null upper values when mean is not null"
        assert df[(df["lower_" + col].isnull()) &\
                (df["mean_" + col].notnull()) &\
                (df['source'] != "UK_HOSPITAL_STATISTICS")].shape[0] == 0,\
                "There are null lower values when mean is not null"
        # this is probably redundant but check that when upper/lower are not null
        # then mean is also not null
        assert df.loc[(df["lower_" + col].notnull()) &\
                (df['source'] != "UK_HOSPITAL_STATISTICS"),
                "mean_" + col].notnull().all(),\
                "There are non null means when lower is null"
        assert df.loc[(df["upper_" + col].notnull()) &\
                (df['source'] != "UK_HOSPITAL_STATISTICS"),
                "mean_" + col].notnull().all(),\
                "There are non null means when upper is null"

    return df


def test_corrections(df):

    print(df.mean_incidence.isnull().sum())
    # CHECK that lower < mean < upper
    # loop over every level of correction
    for ctype in ["incidence", "prevalence", "indvcf"]:
        # lower < mean
        assert (df.loc[df['lower_' + ctype].notnull(), 'lower_' + ctype] <=
                df.loc[df["lower_" + ctype].notnull(), 'mean_' + ctype]).all(),\
            "lower_{} should be less than mean_{}".format(ctype, ctype)
        # mean < upper
        assert (df.loc[df["upper_" + ctype].notnull(), 'mean_' + ctype] <=
                df.loc[df["upper_" + ctype].notnull(), 'upper_' + ctype]).all(),\
            "mean_{} should be less than upper_{}".format(ctype, ctype)
    return


def apply_inj_corrections(df):
    """
    apply the output of `prop_code_for_hosp_team.py` to the data aggregated into
    5 year bands

    Parameters:
        df: Pandas DataFrame
        The 5 year aggregated hospital data
    """
    inj_cf = pd.read_hdf("{FILEPATH}/inj_factors_collapsed_years.H5")

    # prep inj cf data
    inj_cf = inj_cf[inj_cf['facility_id'] == "inpatient unknown"]
    inj_cf.drop(['facility_id', 'prop', 'remove'], axis=1, inplace=True)
    inj_cf.rename(columns={'factor': 'correction_factor_inj'}, inplace=True)

    # merge scalars on by loc and year
    pre = df.shape[0]
    df = df.merge(inj_cf, how='left', on=['location_id', 'year_start', 'year_end'])
    assert pre == df.shape[0], "The merge changed rows. Review this {}".format(\
        pre, df.shape[0])

    no_inj_srcs = ['IDN_SIRS', 'UK_HOSPITAL_STATISTICS', 'GEO_COL', 'IRN_MOH']

    assert df[~df.source.isin(no_inj_srcs)].\
        correction_factor_inj.isnull().sum() == 0,\
        "There shouldn't be null correction factors in source(s) {} \n{}".format(\
                [x for x in df[df["correction_factor_inj"].isnull()].source.unique() if x not in no_inj_srcs],
                df[(df["correction_factor_inj"].isnull()) & (~df.source.isin(no_inj_srcs))])

    # apply the scalar to mean/lower/upper
    levels = ['mean_raw', 'lower_raw', 'upper_raw']
    for level in levels:
        lev_name = level[:-4]
        df[lev_name + "_inj"] = df[level] * df['correction_factor_inj']

        # test that the inj CFs aren't null
        assert df.loc[~df.source.isin(no_inj_srcs), lev_name + "_inj"].\
            isnull().sum() == 0,\
            "There shouldn't be null correction factors in source(s) {} \n{}".format(\
                [x for x in df[df[lev_name + "_inj"].isnull()].source.unique() if x not in no_inj_srcs],
                df[(df[lev_name + "_inj"].isnull()) & (~df.source.isin(no_inj_srcs))])
    return df


def agg_to_five_main(df, write=False, maternal=False, use_modified=False):
    # create a map from location and year to source
    source_map = df[['location_id', 'year_start', 'year_end', 'source']].drop_duplicates()

    # bin to 5 year groups so it matches with the data after agg to dismod
    source_map = hosp_prep.year_binner(source_map).drop_duplicates()

    df = rename_raw_cols(df)

    print("Making the data square...")
    df = make_square(df)

    df.bundle_id = df.bundle_id.astype(float)

    print("Merging on population and going to rate space...")
    df = merge_population(df)

    print("Going from rates to count...")
    df = rate_to_count(df)

    # here the process splits into two streams
    df, covered_df = split_covered_sources(df)

    print("Merging on denominator file...")
    df = merge_denominator(df)

    print("Aggregating to dismod years...")
    df = aggregate_to_dismod_years(df)

    covered_df = aggregate_covered_sources(covered_df)

    print("df columns after agg to dismod are {}".format(df.columns))
    # this function acts on both dataframes, but they are still separate
    print("Going back to rate space...")
    df, covered_df = count_to_rate(df, covered_df)

    # bring the two streams back into one.
    df = concat_covered_sources(df, covered_df)

    if not maternal:
        df = get_parent_injuries(df)
        for col in ['mean_raw', 'lower_raw', 'upper_raw']:
            hosp_prep.check_parent_injuries(df, col_to_sum=col, verbose=False)

    print("Adding measure and applying/capping correction factors...")
    df = add_measure(df)

    # merge source back on
    pre_source = df.shape[0]
    df = df.merge(source_map, how='left', on=['location_id',
                                              'year_start', 'year_end'])
    assert pre_source == df.shape[0],\
        "Merging the source names back on appears to have altered the "\
        "data. Take a look into these sources {}".format(\
            source_map[source_map.duplicated(['location_id', 'year_start'], keep=False)])

    # this only needs to be applied to the covered data sources
    df = apply_corrections(df, use_modified=use_modified)

    df['bundle_id'] = df['bundle_id'].astype(float)

    # every source type should be capped
    df = cap_corrections(df, use_modified=use_modified)

    test_corrections(df)

    # A bunch of uncertainty checks
    # When mean_raw is zero, then sample_size should not be null.
    assert df.loc[df.mean_raw == 0, "sample_size"].notnull().all(),\
        "Imputed zeros are missing sample_size in some rows."


    # # Make upper and lower Null when sample size is not null
    cols = ["incidence", "prevalence", "indvcf"]
    df.loc[df.sample_size.notnull(), ["upper_" + x for x in cols]] = np.nan
    df.loc[df.sample_size.notnull(), ["lower_" + x for x in cols]] = np.nan

    # when upper / lower are not null, then sample_size should be null
    for x in cols:
        assert df.loc[df["upper_" + x].notnull(), "sample_size"].isnull().all()
        assert df.loc[df["lower_" + x].notnull(), "sample_size"].isnull().all()

    print("Applying the haqi and injury corrections")
    # apply the haqi correction
    df = apply_haqi_corrections(df)

    if not maternal:
        # apply the 5 year inj corrections
        df = apply_inj_corrections(df)

    if write:
        # convert mixed type cols to numeric
        df = hosp_prep.fix_col_dtypes(df, errors='raise')
        print("Writing the df file...")
        file_path = "{FILEPATH}/agg_to_five_years.H5"
        hosp_prep.write_hosp_file(df, file_path, backup=True,
                                  include_version_info=True)

    return df
