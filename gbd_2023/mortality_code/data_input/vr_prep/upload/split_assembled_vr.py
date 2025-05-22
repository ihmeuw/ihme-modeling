"""
The functions in this script are used in the age sex splitting step. After the
This script performs age sex splitting on CoD VR. The core functions that
perform the splitting are imported. This scripts prepares the data and handles
the split. It also:
- Adjusts the maori/non-maori deaths to address a change in definition that
  must be done after age sex splitting.
- Prepares and saves data input data for the age-sex model
- Aggregates subnational data to the national level with imported functions
"""

import sys
import pandas as pd
import getpass
from db_queries import get_population, get_location_metadata, get_demographics
from db_tools import ezfuncs
from hierarchies.dbtrees import agetree
import numpy as np
import warnings

sys.path.append("FILEPATH")
from cod_prep.downloaders.ages import get_ages
from cod_prep.claude import adjust_nzl_deaths

# Get the age sex splitting functions/classes
sys.path.append("FILEPATH")
import age_sex_splitting as asp

# get the aggregation to national functions
import agg_vr_national

# Globals / passed in arguments
NEW_RUN_ID = sys.argv[1]
IS_COD_VR = sys.argv[2].upper()
GBD_ROUND_ID = sys.argv[3]
RELEASE_ID = sys.argv[4]

# Check arguments
assert IS_COD_VR in ["TRUE", "FALSE"], "IS_COD_VR should be one of {}, not {}".format(["TRUE", "FALSE"], IS_COD_VR)
if not isinstance(GBD_ROUND_ID, int):
    GBD_ROUND_ID = int(GBD_ROUND_ID)
if not isinstance(RELEASE_ID, int):
    RELEASE_ID = int(RELEASE_ID)

NID_MAP_PATH = "FILEPATH"

def remove_overlapping_ages(vr_data):
    """
    Due to the way sources have been selected up to this point, there can be
    more than one source for a location-year but have different, distinct,
    overlapping age groups.  This function finds those and removed them.
    """
    print("Removing overlapping ages...")
    # attach age information to the VR data
    ages_df = get_ages()[['age_group_id', 'age_group_name',
                          'age_group_days_start', 'age_group_days_end']]
    pre = vr_data.shape[0]
    vr_data = vr_data.merge(ages_df, how='left', on="age_group_id")
    assert pre == vr_data.shape[0], "Merge changed number of rows."

    # store this for a check
    original_shape = vr_data.shape[0]

    # isolate the all ages group. we don't want these to count as duplicates
    # on age_start
    all_ages = vr_data[vr_data.age_group_id == 22]
    vr_data = vr_data[vr_data.age_group_id != 22]

    # find the duplicates on age_start
    duplicates_age_start = vr_data.duplicated(subset=['location_id', 'year_id',
                                                      'sex_id',
                                                      'age_group_days_start',
                                                      'source_type_id',
                                                      'estimate_stage_id'],
                                              keep=False)

    # isolate the duplicated rows from the rest of the data
    df = vr_data[duplicates_age_start]
    vr_data = vr_data[~duplicates_age_start]

    # check that all our dataframes are complementary
    assert df.shape[0] + all_ages.shape[0] + vr_data.shape[0] == original_shape

    # initialize keep column
    df.loc[:,'keep'] = 0

    # make function that will mark the youngest age_end among the duplicates
    # of age_start to be kept
    def mark_dups_to_keep(df):
        df.loc[df.age_group_days_end == df.age_group_days_end.min(), "keep"] = 1
        return df

    # group by location year sex and age start, and apply the marking function
    df = df.groupby(["location_id", "sex_id", "year_id",
                     "age_group_days_start", "source_type_id", 
                     "estimate_stage_id"]).apply(mark_dups_to_keep)
    df = df[df.keep == 1]
    df = df.drop("keep", axis=1)

    vr_data = pd.concat([df, all_ages, vr_data], ignore_index=True)

    vr_data[vr_data.age_group_id != 22].duplicated(
        subset=['age_group_days_start', 'year_id', 'sex_id', 'location_id', 'source_type_id', 'estimate_stage_id'],
        keep=False).sum()

    age_columns = vr_data.filter(regex="age_group_.*").columns.tolist()
    age_columns.remove("age_group_id")
    vr_data = vr_data.drop(age_columns, axis=1)

    return vr_data


def drop_missing_terminal_ages(df):
    """
    Some sources don't have a terminal age group properly marked. For example,
    a source may have 70-74 instead of 70 plus.  This function finds sources
    like those and drops them.
    """

    print("Dropping sources that don't have proper terminal age groups...")

    starting_cols = df.columns.tolist()

    keep = df[df.age_group_id.isin([283])].copy()
    df = df[~df.age_group_id.isin([283])].copy()

    # attach age information to the VR data
    ages_df = get_ages()[['age_group_id', 'age_group_years_end']]
    pre = df.shape[0]
    df = df.merge(ages_df, how='left', on="age_group_id")
    assert df.shape[0] == pre

    # within each source (group) identify the oldest age end
    groupby_cols = ["location_id", "sex_id", "year_id", "source_type_id", "estimate_stage_id"]
    df.loc[:,'oldest_age_group_years_end'] = df.groupby(by=groupby_cols)['age_group_years_end'].transform("max")

    # drop sources whose oldest age end is too small
    df = df[df.oldest_age_group_years_end > 100]

    # check that all the oldest ages are good
    assert list(df.oldest_age_group_years_end.unique()) == [125]

    # drop columns that were used but shouldn't be passed on
    df = df[starting_cols]

    # re-attach the data that wasn't checked
    df = pd.concat([df, keep], ignore_index=True)
    assert "age_group_years_end" not in df.columns

    return df


def get_age_split_mapping(age_group_list):
    """
    Helper function to collapse_small_age_groups()
    Look up each age group id to determine the underlying most-detailed ages
    """
    mapping = []
    for age_group_id in age_group_list:
        for l in agetree(age_group_id, release_id=RELEASE_ID).leaves():
            mapping.append({
                'age_group_id_aggregate': age_group_id,
                'age_group_id': l.id})
    return pd.DataFrame(mapping)


def age_binning(df, age_column):
    """
    Helper function to collapse_small_age_groups()

    Takes age values and creates age ranges that correspond to standard gbd
    age groups.
    """
    assert df[age_column].notnull().any(), "{} cannot have nulls".format(age_column)

    age_bins = np.append(np.array([0, 1, 2, 5]), np.arange(10, 101, 5))

    # labels for age columns are the lower and upper ages of bin
    age_start_list = np.append(np.array([0, 1, 2]), np.arange(5, 96, 5))
    age_end_list = np.append(np.array([1, 2]), np.arange(5, 96, 5))
    age_end_list = np.append(age_end_list, 125)

    # Create 2 new age columns
    df.loc[:,'age_start_binned'] = pd.cut(df[age_column], age_bins, labels=age_start_list,
                             right=False)
    df.loc[:,'age_end_binned'] = pd.cut(df[age_column], age_bins, labels=age_end_list,
                           right=False)

    # make age_start_binned and age_end_binned numeric
    df.loc[:,'age_start_binned'] = pd.to_numeric(df['age_start_binned'], errors='raise')
    df.loc[:,'age_end_binned'] = pd.to_numeric(df['age_end_binned'], errors='raise')

    df.loc[df[age_column] >= 95, "age_start_binned"] = 95
    df.loc[df[age_column] >= 95, "age_end_binned"] = 125

    assert_msg = """ Age binning failed. These ages did not bin correctly:
    {}
    """.format(df[df.isnull().any(axis=1)].to_string())
    assert df.notnull().all().all(), assert_msg

    return df


def collapse_small_age_groups(df):
    """
    This function is for noncod data that has finely detailed ages, including
    single years age groups such as 2 years old and unsual ages such as 98-plus
    and 99-plus, that should be aggregated up into standard age groups.

    Arguments:
        df (Pandas DataFrame): noncod data.

    Raises:
        AssertionError for various checks

    Returns:
        Pandas DataFrame with aggregated ages.
    """

    print("Collapsing small detailed ages up to standard age groups...")

    # save starting info for later
    starting_deaths = df.deaths.sum()
    starting_shape = df.shape[0]
    starting_cols = df.columns.tolist()

    mapping = get_age_split_mapping(
        age_group_list=df.age_group_id.unique())

    # get a list of the definitively good age group ids
    good_age_group_ids = get_demographics(gbd_team='cod', gbd_round_id=GBD_ROUND_ID, release_id=RELEASE_ID)['age_group_id']

    # look for aggregate age_group_ids that mapped to themselves for their
    # age_group_id 
    bad_age_group_ids = mapping[
        (mapping.age_group_id == mapping.age_group_id_aggregate) &
        ~(mapping.age_group_id_aggregate.isin(good_age_group_ids))].\
        age_group_id.unique()

    keep = df[~df.age_group_id.isin(bad_age_group_ids)].copy()
    # drop the good ages
    df = df[df.age_group_id.isin(bad_age_group_ids)].copy()
    assert df.shape[0] + keep.shape[0] == starting_shape

    # get age start and age end for the bad age groups
    age_query = """
                """.format(", ".join(str(a) for a in bad_age_group_ids))
    age_metadata = ezfuncs.query(age_query, conn_def="shared")

    # make sure nothing is missing
    assert set(bad_age_group_ids) == set(age_metadata.age_group_id.unique())

    # Use age_group_years_start to bin the bad ages into buckets that
    # corresepond to good age_group_ids
    age_metadata = age_binning(age_metadata,
                               age_column='age_group_years_start')
    # drop age_group_years_start & age_group_years_end, which are bad age ranges
    age_metadata = age_metadata.drop(["age_group_years_start",
                                      "age_group_years_end"], axis=1)
    # rename age_start_binned and age_end_binned, which are good ages, so that we can merge on
    # new good age_group_ids
    age_metadata = age_metadata.rename(
        columns={"age_start_binned": "age_group_years_start",
                 "age_end_binned": "age_group_years_end"})

    # merge the binned age start and age end onto the data with the bad ages
    pre = df.shape[0]
    df = df.merge(age_metadata, how='left', on='age_group_id')
    assert pre == df.shape[0], "Merge changed the number of rows."
    assert df.age_group_years_start.notnull().all()
    assert df.age_group_years_end.notnull().all()

    # good_age_group_ids
    age_query = """
                """.format(", ".join(str(a) for a in good_age_group_ids))
    good_age_metadata = ezfuncs.query(age_query, conn_def="shared")

    # rename the age_group_id that we want to replace
    df = df.rename(columns={"age_group_id": "bad_age_group_id"})

    # this merge attaches the standard age_group_ids to the data
    pre = df.shape[0]
    df = df.merge(good_age_metadata, how='left',
                  on=['age_group_years_start', 'age_group_years_end'])
    assert pre == df.shape[0], "Merge changed number of rows"
    assert df.age_group_id.notnull().all(), \
        "Merge didn't work, not everything got a valid age_group_id"
    assert df.age_group_years_start.notnull().all()
    assert df.age_group_years_end.notnull().all()

    # prepare to collapse into the good ages
    # drop columns that contain detailed age info which prevent the collapse
    df = df.drop(["age_group_years_start", "age_group_years_end",
                  "bad_age_group_id"], axis=1)
    # fill nulls so that rows with nulls don't get dropped during collapse
    df = df.fillna("--filled-na--")

    # collapse to good ages
    pre_collapse_death_total = df.deaths.sum()
    groupby_cols = [col for col in starting_cols if col != 'deaths']
    df = df.groupby(by=groupby_cols, as_index=False).deaths.sum()

    # put nulls back
    df = df.replace(to_replace="--filled-na--", value=pd.np.nan, inplace=False)

    # tests on collapsed data
    assert abs(pre_collapse_death_total - df.deaths.sum()) < 0.0001,\
        "Too large of difference in deaths"
    assert set(df.age_group_id) <= set(good_age_group_ids),\
        "There are still bad ages."
    assert df[df.age_group_id.isin(bad_age_group_ids)].shape[0] == 0,\
        "There are still bad ages."

    # put the data pack together
    df = pd.concat([keep, df], ignore_index=True, sort=False)

    # tests on full data
    assert starting_shape > df.shape[0],\
        "There should be fewer rows after collapsing"
    assert abs(starting_deaths - df.deaths.sum()) < 0.001,\
        "Too large of difference in deaths"

    return df


def re_collapse(df):
    """
    This function is used for the data prep of age-sex model input and in main
    It collapses the dataframe using every column besides "deaths" as the by
    variables.  It fills nulls in underlying_nid before collapse to avoid
    losing rows with null values, and puts them back after the collapse. Thus,
    it assumes that that is the only column with nulls in it.  If that condition
    is not true the AssertionError will catch it.

    Raises:
        AssertionError if number of deaths changes during the collapse

    """

    # store for comparison
    deaths_before = df.deaths.sum()

    print("Re-collapsing data...")
    # Need to fill NaNs before collapse so they don't get dropped
    df.loc[df.underlying_nid.isnull(), "underlying_nid"] = "--filled-na--"
    group_cols = [col for col in df.columns if col not in ["deaths"]]
    assert_msg = "There are Nulls present in the grouping key columns; These rows with Nulls will be dropped.\n{}".format(df[group_cols].isnull().sum().to_string())
    assert df[group_cols].notnull().all().all(), assert_msg
    df = df.groupby(by=group_cols, as_index=False).deaths.sum()
    # Want to put the NaNs back
    df.loc[df.underlying_nid == "--filled-na--", "underlying_nid"] = pd.np.nan

    # check if deaths were lost
    diff = abs(deaths_before - df.deaths.sum())
    assert diff < 9e-2, "Deaths changed during collapse. The absolute difference is {}".format(diff)

    return df


def adjust_nzl(df):
    """
    Adjustment to the Maori/Non-Maori data needed
    to account for a change in definition of how the nzl govt counted
    demographic information.
    """
    print("Adjusting NZL subnational values...")
    locs = get_location_metadata(location_set_id=82, release_id=RELEASE_ID)
    maori_locs = locs[locs.location_name.str.contains("Maori")].location_id.unique()

    # select just the NZL locations we want to adjust
    nzl = df[df.location_id.isin(maori_locs)]
    df = df[~df.location_id.isin(maori_locs)]

    # Drop pre-1994 NZL data
    nzl = nzl.loc[nzl.year_id > 1994]

    assert nzl.shape[0] > 0, "There isn't any New Zealand data."

    # run the adjustment
    nzl_adjusted = adjust_nzl_deaths.correct_maori_non_maori_deaths(nzl)

    assert set(nzl_adjusted.columns) == set(df.columns),\
        "Columns are not the same"
    df = pd.concat([df, nzl_adjusted], ignore_index=True)

    return df


def drop_splits(df):
    """
    This function drops particular age sex split data because that
    is required by the age-sex model data prep script.
    """

    # make marker that indicates that the data was split out
    was_split = ((df.age_group_id_aggregate != df.age_group_id) |
                 (df.sex_id_aggregate != df.sex_id))

    # want to drop the split data from age 1 to 4
    age_1_to_4 = df.age_group_id == 5

    # make new condition that meets both the above conditions
    drop_rows = age_1_to_4 & was_split

    # drop the splits in 1-4 years old
    df = df[~drop_rows]

    return df


def format_for_age_sex_model(df):
    """
    This function prepares a copy of the split cod VR data for the age-sex
    model.
    """

    print("Preparing a copy of data for the age-sex model...")

    df = df.copy()

    # drop split data in age group 1 to 4 years old
    df = drop_splits(df)

    df = df.drop(["age_group_id_aggregate", "sex_id_aggregate"], axis=1)

    df = re_collapse(df)
    
    # remove and check for duplicates
    duplicates = df.duplicated(subset=['location_id', 'age_group_id',
                                       'sex_id', 'year_id', 'source_type_id',
                                       'estimate_stage_id'],
                               keep=False)
    assert_msg = ("The data that was being prepared for "
                  "the age-sex model has {} duplicates.".format(duplicates.sum()))
    assert not duplicates.any(), assert_msg

    # adjust NZL maori
    df = adjust_nzl(df)

    # Aggregate to national
    df = agg_vr_national.aggregate_to_country_level(
        orig_df=df.copy(),
        nid_map=pd.read_csv(NID_MAP_PATH),
        gbd_round_id=GBD_ROUND_ID,
        release_id=RELEASE_ID
    )

    duplicates = df.duplicated(subset=['location_id', 'age_group_id',
                                       'sex_id', 'year_id', 'source_type_id',
                                       'estimate_stage_id'],
                               keep=False)
    assert not duplicates.any(), assert_msg

    df = cast_agesex_split_wide(df)

    print("Successfully prepared data for input into the age-sex model.")

    return df


def cast_agesex_split_wide(df):
    """
    At the end of this we just need the columns
    deaths, iso3, location_id, year, sex, source, NID, and age_group_id
    """
    print("Formatting data to meet requirements of the next step...")
    needed_columns = ["deaths", "iso3", "location_id", "year", 'source_type_id',
                      "sex", "source", "NID", "underlying_nid", "age_group_id",
                      "estimate_stage_id"]


    # rename
    df = df.rename(columns={'nid': "NID", 'year_id': 'year', 'sex_id': 'sex'})

    # Re-impute missing NA underlying_nids
    df.loc[df.underlying_nid.isna(), 'underlying_nid'] = '--filledna--'

    # attach iso3
    locs = get_location_metadata(location_set_id=82, release_id=RELEASE_ID)\
        [['location_id', 'ihme_loc_id']]
    locs.loc[:,'iso3'] = locs.ihme_loc_id.str[0:3]
    locs = locs.drop("ihme_loc_id", axis=1)
    df = df.merge(locs, how='left', on='location_id')

    # keep the needed columns
    df = df[needed_columns]

    # check that we have all the columns we want
    diff  = set(needed_columns) - set(df.columns)
    assert_msg = "These columns are missing: {}".format(diff)
    assert set(df.columns) == set(needed_columns), assert_msg

    # Ensure we have the right column order at the end
    age_group_cols = ['deaths{}'.format(str(i)) for i in np.sort(df.age_group_id.unique())]
    non_age_cols = list(df.drop(columns=['age_group_id', 'deaths']).columns)
    # Cast wide on age group ID
    df.age_group_id = ['deaths{}'.format(str(i)) for i in df.age_group_id]
    output = pd.pivot_table(df, index=['iso3', 'location_id', 'year', 'sex', 'source', 'NID', 'underlying_nid', 'source_type_id', 'estimate_stage_id'], columns='age_group_id', values='deaths').reset_index()

    col_order = non_age_cols + age_group_cols
    output = output[col_order]

    # Reset nids
    output.loc[output.underlying_nid == "--filledna--", 'underlying_nid'] = pd.np.nan

    print("Done formatting.")
    return output


def save_outputs(df, output_file):
    """
    Saves split data at location specified by output_file.
    output_file should be a full filepath, including the file extension.
    """
    obj_cols = df.select_dtypes(include=[object]).columns.tolist()
    for col in obj_cols:
        df.loc[df[col].notnull(), col] = df.loc[df[col].notnull(), col].astype(str)

    # some ID vars were renamed in cast_agesex_split_wide()
    for col in ['sex', 'year', 'location_id', 'source_type_id', 'estimate_stage_id']:
        df[col] = df[col].astype(int)

    print("Saving...")
    df.to_stata(output_file, write_index=False)
    print("Saved at {}".format(output_file))


def prep_vr_input_data(df, is_cod_vr):
    """
    Reads in input data and does processing to get it ready for age sex
    splitting.

    Args:
        input_vr_file (str): filepath to input data
        is_cod_vr (str): if equal to "TRUE" indicates that the data is CoD data

    Raises:
        AssertionErrors if:
            - Any age_group_id 22 (all ages) present
            - Any sex_id 3 (both sexes) present)
            - There are any duplicates

    Returns
        Input data to be split

    """

    # check input data for aggregate age/sex groups that if split could lead to
    # double counting.

    if (df.age_group_id == 22).any():
        warn_msg = """

        There are some age group id 22 (all ages) present in the  data. Be aware: these
        will be dropped.
        Here's the value counts: \n\n{}

        """.format(df.sex_id.value_counts().to_string())
        warnings.warn(warn_msg)

    df = df[df.age_group_id != 22]

    if (df.sex_id == 3).any():
        warn_msg = """

        There are some sex_id 3 (both sexes) present in the  data. Be aware: these
        will be split onto sex_id 1 and 2 and this could lead to double counting.
        Here's the value counts: \n\n{}

        """.format(df.sex_id.value_counts().to_string())
        warnings.warn(warn_msg)

    # Now convert the id on unknown ages to the id for all ages. 
    df.loc[df.age_group_id == 283, 'age_group_id'] = 22

    # enforce dtypes
    for col in ['sex_id', 'age_group_id', 'year_id', 'location_id']:
        df[col] = df[col].astype(int)

    df = remove_overlapping_ages(df)

    if is_cod_vr == "FALSE":
        df = collapse_small_age_groups(df)

    # check for duplicates
    df = df.sort_values(by=df.columns.tolist())
    duplicates = df.duplicated(
        [col for col in df.columns if col != 'deaths'],
        keep=False)
    assert duplicates.sum() == 0,\
        ("There are {} rows of duplicated data present. There cannot be any "
         "for age sex splitting to work.".format(duplicates.sum()))

    # For now only want to do this on noncod data
    # Drop data whose oldest age is not properly labeled.
    if is_cod_vr == "FALSE":
        df = drop_missing_terminal_ages(df)

    return df


def prep_population_input(vr_input_object, is_cod_vr):
    """
    prepares population as an input to age sex splitting, including creating
    population for old andhra pradesh
    """

    print("Pulling most recent population...")

    # get a list of the definitively good age group ids
    good_age_group_ids = get_demographics(gbd_team='cod', gbd_round_id=GBD_ROUND_ID, release_id=RELEASE_ID)['age_group_id']

    pop = pd.read_csv("/mnt/team/mortality/pub/data_prep/death_number_empirical/{}/inputs/population_for_splitting.csv".format(NEW_RUN_ID))

    # Subset
    pop = pop.loc[(pop.age_group_id.isin(good_age_group_ids + [4,5])) & \
                  (pop.location_id.isin(list(vr_input_object.data.location_id.unique()))) & \
                  (pop.sex_id.isin(list(vr_input_object.data.sex_id.unique()))) & \
                  (pop.year_id.isin(list(vr_input_object.data.year_id.unique())))]


    if is_cod_vr != "TRUE":

        # get location information
        location_metadata = get_location_metadata(location_set_id=82, release_id=RELEASE_ID)

        # get location ids for current Andhra Pradesh and Old Andhra Pradesh
        new_andhra_pradesh_loc_id = location_metadata[
                location_metadata.location_name == "Andhra Pradesh"
            ].location_id.values[0]
        old_andhra_pradesh_loc_id = location_metadata[
                location_metadata.location_name == "Old Andhra Pradesh"
            ].location_id.values[0]

        # make a copy of the pop that correspond to just current/new Andhra Pradesh
        andhra_pradesh_pop = pop[pop.location_id == new_andhra_pradesh_loc_id].copy()

        # make a copy of that
        old_andhra_pradesh_pop = andhra_pradesh_pop.copy()

        # set the new andhra pradesh location id to the old one
        # Now we have pop for old andhra pradesh
        old_andhra_pradesh_pop.location_id = old_andhra_pradesh_loc_id

        # some simple asserts / test
        assert old_andhra_pradesh_pop.shape == andhra_pradesh_pop.shape
        assert (old_andhra_pradesh_pop.location_id == old_andhra_pradesh_loc_id).all()
        assert (andhra_pradesh_pop.location_id == new_andhra_pradesh_loc_id).all()
        # check that besides the location ids the two frames are identical
        pd.testing.assert_frame_equal(left=old_andhra_pradesh_pop.drop("location_id", axis=1),
                                      right=andhra_pradesh_pop.drop("location_id", axis=1))

        # attach pop for old andhra pradesh back to the pop data frame
        pre = pop.shape[0]
        pop = pd.concat([pop, old_andhra_pradesh_pop], ignore_index=True)

        # more tests
        assert pop.shape[0] == pre + old_andhra_pradesh_pop.shape[0]
        assert old_andhra_pradesh_loc_id in pop.location_id.unique()
        assert new_andhra_pradesh_loc_id in pop.location_id.unique()
        assert (pop[pop.location_id == new_andhra_pradesh_loc_id].shape[0] ==
                pop[pop.location_id == old_andhra_pradesh_loc_id].shape[0])

    return pop

def main(input_vr_file, input_distribution_file, is_cod_vr, out_dir):
    """
    Main function that gathers the data and functions/objects needed to
    run age sex splitting. The code that performs the splitting is
    imported in.
    """
    print("Using {} for the data to be split and {} for the splitting "
          "distribution".format(input_vr_file, input_distribution_file))

    # Input data
    vr_data = pd.read_csv(input_vr_file)

    # Fill NAs for grouping
    vr_data = vr_data.fillna("--filled-na--")

    if is_cod_vr == "TRUE":
        vr_data = vr_data[["sex_id","nid","year_id","age_group_id","location_id","source","underlying_nid","deaths","source_type_id", "estimate_stage_id"]]

    # prepare input data
    vr_data = prep_vr_input_data(df=vr_data, is_cod_vr=is_cod_vr)
    
    # prepare input data object
    print("Preparing input data object...")
    asp_input = asp.AgeSexInputData(input_data=vr_data,
                                    sex_id_column='sex_id',
                                    age_group_id_column='age_group_id',
                                    value_column='deaths')

    # Distribution Data
    print("Preparing distribution object...")
    dist_data = pd.read_csv(input_distribution_file)
    dist_data_dup = dist_data.copy()
    dist_data['estimate_stage_id'] = 21 
    dist_data_dup['estimate_stage_id'] = 22
    dist_data = pd.concat([dist_data, dist_data_dup], ignore_index = True)

    asp_dist = asp.AgeSexDistribution(
        distribution_data=dist_data,
        index_columns=["location_id", "year_id", "sex_id", "age_group_id", "estimate_stage_id"],
        weight_column="weight"
    )

    # Population Data
    pop_input_data = prep_population_input(asp_input, is_cod_vr)

    # prepare age sex splitting object
    print("Preparing age sex splitting object...")
    asp_split_data = asp.AgeSexDataSplit(
        input_data=asp_input,
        distribution_data=asp_dist,
        pop_data=pop_input_data,
        release_id=RELEASE_ID,
        is_cod_vr = is_cod_vr,
        out_dir = out_dir
    )

    # Run age sex splitting
    print("Performing age sex splitting...")
    split = asp_split_data.run_relative_rate_split()
    print("Data was successfully split.")

    if is_cod_vr == "TRUE":
        # spin off / save a copy of the data formatted and prepared for the
        # age-sex model
        age_sex_model_input = format_for_age_sex_model(split)
        
    # need to drop these columns
    split = split.drop(["age_group_id_aggregate", "sex_id_aggregate"], axis=1)

    # aggregate the data
    split = re_collapse(split)
    print("Deaths grand total after re_collapse: {}".format(split["deaths"].sum()))

    # check for duplicates that crop up during splitting
    assert_msg = "There are duplicates."
    duplicated = split.duplicated(subset=['sex_id', 'age_group_id',
                                          'location_id', 'year_id',
                                          'source_type_id', 'estimate_stage_id'],
                                keep=False)
    assert not duplicated.any(), assert_msg

    # adjust NZL Maori
    if is_cod_vr == "TRUE":
        split = adjust_nzl(split)

    # Aggregate to national
    split = agg_vr_national.aggregate_to_country_level(
        orig_df=split.copy(),
        nid_map=pd.read_csv(NID_MAP_PATH),
        gbd_round_id=GBD_ROUND_ID,
        release_id=RELEASE_ID
    )

    assert_msg = "There are duplicates after the agg_vr_national() function"
    assert not split.duplicated(subset=['sex_id', 'age_group_id',
                                        'location_id', 'year_id',
                                        'source_type_id', 'estimate_stage_id'],
                                keep=False).any(), assert_msg

    # format for the next step
    split = cast_agesex_split_wide(df=split)

    return_list = [split]
    if is_cod_vr == "TRUE":
         return_list.append(age_sex_model_input)

    return return_list


if __name__ == "__main__":

    print("NEW_RUN_ID is set to {}".format(NEW_RUN_ID))
    print("IS_COD_VR is set to {}".format(IS_COD_VR))
    if IS_COD_VR == "TRUE":
        in_filename = "FILEPATH"
        out_filename = "FILEPATH"
    else:
        in_filename = "FILEPATH"
        out_filename = "FILEPATH"

    # Unfortunately this filepath needs to be updated MANUALLY
    input_distribution_file = ("FILEPATH")

    # set up output locations
    input_vr_file = ("FILEPATH")
    out_dir = ("FILEPATH")
    out_file = "FILEPATH"

    # run the splitting code
    output = main(
        input_vr_file=input_vr_file,
        input_distribution_file=input_distribution_file,
        is_cod_vr = IS_COD_VR,
        out_dir = out_dir
    )

    split_vr = output[0]

    if IS_COD_VR == "TRUE":
        age_sex_model_input = output[1]

    # save the split data
    save_outputs(df=split_vr, output_file=out_file)

    # save the age_sex_model_input data
    if IS_COD_VR == "TRUE":
        save_outputs(df=age_sex_model_input,
                 output_file="FILEPATH")

    # all done!
