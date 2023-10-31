"""
This file contains all the functions relevant to the outpatient process.  Most
are an adaptation of the Inpatient functions.
"""


import os
import time
import warnings

import numpy as np
import pandas as pd

from db_tools.ezfuncs import query
from db_queries import get_population, get_location_metadata

# clinical team repo
from clinical_info.Mapping import clinical_mapping as cm
from clinical_info.Functions import bundle_to_cause_hierarchy
from clinical_info.Database.bundle_relationships.relationship_methods import ParentInjuries


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

    # study began in dec 1991, not complete data
    print("Dropping 1991 from NHAMCS...")
    df = df.loc[(df.source != "USA_NHAMCS_92_10") | (df.year_start != 1991)]
    print("Number of rows = {}".format(df.shape[0]))

    print("Dropping unknown sexes because we can't age-sex split outpatient...")
    df = df[df.sex_id != 9].copy()
    df = df[df.sex_id != 3].copy()
    print("Number of rows = {}".format(df.shape[0]))

    print("Done dropping data")

    return(df)


def create_parent_injuries(df, run_id):
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

    parent_method = ParentInjuries(run_id=run_id)
    parent_method.get_tables()
    parent_df = parent_method.bundle_relationship.copy()
    parent_df.rename(columns={'origin_bundle_id': 'child_bundle_id',
                              'output_bundle_id': 'parent_bundle_id'},
                     inplace=True)

    parent_df = parent_df[['child_bundle_id', 'parent_bundle_id']].copy()

    df = df.rename(columns={"bundle_id": "child_bundle_id"})

    parent_df = parent_df.merge(df, how='left', on='child_bundle_id')
    assert parent_df.notnull().all().all(),\
        f"There are nulls!\n{parent_df.isnull().sum()}"

    # rename it back, recall that this is the main dataframe
    df = df.rename(columns={"child_bundle_id": "bundle_id"})

    # drop child bundle IDs
    parent_df.drop(['child_bundle_id'], axis=1, inplace=True)

    # rename so that what was parent bundle id is the main bundle id
    parent_df.rename(columns={'parent_bundle_id': 'bundle_id'}, inplace=True)

    df.drop(['outcome_id', 'diagnosis_id'], axis=1, inplace=True)
    parent_df.drop(['outcome_id', 'diagnosis_id'], axis=1, inplace=True)
    groups = ['location_id', 'year_start', 'year_end',
              'age_start', 'age_end', 'sex_id', 'nid',
              'representative_id',
              'bundle_id',  # NOTE: this was parent_bundle_id
              'metric_id', 'source', 'facility_id',
              'age_group_unit']
    parent_df = parent_df.groupby(groups).agg({"val": "sum"}).reset_index()

    assert_msg = """
    columns do not match, the difference is
    {}
    """.format(set(parent_df.columns).symmetric_difference(set(df.columns)))
    assert set(parent_df.columns).symmetric_difference(set(df.columns)) == set(),\
        assert_msg

    # rbind duped parent data back onto hospital data
    df = pd.concat([df, parent_df], ignore_index=True, sort=False)

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

    print(report)
    return(df)


def apply_outpatient_correction(df, run_id):
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

    filepath = FILEPATH
    filepath = filepath.replace("\r", "")

    warnings.warn("""

                  Please ensure that the corrections file is up to date.
                  the file was last edited at {}
                  the filepath is {}

                  """.format(time.strftime('%Y-%m-%d %H:%M:%S',
                                           time.localtime(os.path.getmtime(filepath))), filepath))

    # load corrections
    corrections = pd.read_csv(filepath)

    corrections = corrections.rename(columns={'outpatient': 'smoothed_value'})
    corrections.rename(columns={'sex': 'sex_id'}, inplace=True)

    corrections.update(corrections['smoothed_value'].fillna(1))

    assert not corrections.smoothed_value.isnull().any(),\
        "shouldn't be any nulls in smoothed_value"

    print("these bundle_ids have smoothed value greater than 1",
          sorted(corrections.loc[corrections.smoothed_value > 1,
                                 'bundle_id'].unique()), "so we made their CF 1")

    corrections.loc[corrections.smoothed_value > 1, 'smoothed_value'] = 1

    pre_shape = df.shape[0]
    df = df.merge(corrections, how='left', on=[
                  'age_start', 'sex_id', 'bundle_id'])
    missing_cf_bundles = sorted(
        df.loc[df.smoothed_value.isnull(), 'bundle_id'].unique())
    assert 240 not in missing_cf_bundles
    assert 271 not in missing_cf_bundles
    assert 355 not in missing_cf_bundles
    assert pre_shape == df.shape[0], "merge somehow added rows"
    print("these bundles didn't get corrections",
          missing_cf_bundles,
          " so we made their CF 1")

    df.update(df['smoothed_value'].fillna(1))
    assert not df.isnull().any().any(), "there are nulls!"

    # apply correction
    df['val_corrected'] = df['val'] * df['smoothed_value']

    # drop correction
    df.drop("smoothed_value", axis=1, inplace=True)

    return(df)


def apply_inj_factor(df, gbd_round_id, run_id, fillna=False):
    """
    Function that merges on and applies the injury-specific correction factor.
    This correction only increases values.  The correction is source specific.
    This adds the columns "factor", "remove", and "val_inj_corrected". "factor"
    is the injury correction, "remove" indicates that the injury team will want
    to drop that value, and "val_inj_corrected" is the corrected data.  The
    correction is meant for injuries, and is only applied to injuries bundles.
    The appropriate columns will be dropped later. For now there
    isn't going to be a column that has both the normal claims-derived
    outpatient correction and the injury correction applied.

    Args:
        df: (Pandas DataFrame) Contains your outpatient data.
        fillna: (bool) If True, will fill the columns "factor" and "remove" with
            1 and 0, respectively. Because the injury corrections are source
            specific, there is a chance that not all rows will get a factor.

    Returns:
        DataFrame with injury factors / corrections applied.  Has the new
        additional columns "factor", and "val_inj_corrected"
    """

    # get list of injuries bundles
    inj_bundles = bundle_to_cause_hierarchy.BundleCause(gbd_round_id=gbd_round_id,
                                                        cause_id=687,
                                                        map_version=cm.get_current_map_version("icg_bundle"),
                                                        include_nonmap_bundles=True
                                                        ).bundles_by_cause()

    # filter down to injuries
    inj_mask = df.bundle_id.isin(inj_bundles)
    inj_df = df.loc[inj_mask, :]

    # drop injuries
    df = df.loc[~inj_mask, :]

    filepath = FILEPATH

    warnings.warn("""

                  Please ensure that the factors file is up to date.
                  the file was last edited at {}
                  the filepath is {}

                  """.format(time.strftime('%Y-%m-%d %H:%M:%S',
                                           time.localtime(os.path.getmtime(filepath))), filepath))
    factors = pd.read_hdf(filepath)

    # data and these corrections are at 1 year intervals: 2010, 2011, 2012, ...
    # merge on by location_id, years, nid, facility_id.  columns of interest are
    # factor and remove

    factors.drop("prop", axis=1, inplace=True)

    inj_df = inj_df.merge(factors, how='left', on=[
                          'location_id', 'year_start', 'year_end',
                          'nid', 'facility_id'])

    null_df_cols = ['source', 'location_id', 'year_start',
                    'year_end', 'nid', 'facility_id']
    null_df = inj_df.loc[inj_df.factor.isnull(), null_df_cols]
    assert null_df.shape[1] == len(null_df_cols)
    null_df = null_df.drop_duplicates().sort_values(null_df_cols)
    null_msg = """
    There are {} nulls in the factor column after the merge,
    due to mismatched key columns. They appear in these ID rows:
    {}
    """.format(inj_df[inj_df.factor.isnull()].shape[0],
               null_df)
    print(null_msg)
    null_df.to_csv(FILEPATH index=False)

    if fillna:
        print("Filling the Null values with a factor of 1, and remove of 0")
        inj_df.update(inj_df['factor'].fillna(1))
        inj_df.update(inj_df['remove'].fillna(0))
        assert inj_df.isnull().sum().sum() == 0,\
            "There are Nulls in some column besides factor and remove"

    # apply the correction
    inj_df['val_inj_corrected'] = inj_df['val'] * inj_df['factor']

    # we also want to drop the rows where remove is one:
    inj_df = inj_df[inj_df.remove != 1].copy()

    # and we don't need the remove column any more
    inj_df = inj_df.drop("remove", axis=1)

    # fill columns that df didn't get but inj_df did
    df['val_inj_corrected'] = df.val
    df['factor'] = 1

    # put back together
    df = pd.concat([inj_df, df], ignore_index=True, sort=False)

    return df


def outpatient_restrictions(df):
    """
    Function that applies create_bundle_restrictions to read from clinical_data.

    Args:
        df (Pandas DataFrame) Contains outpatient data at bundle level.

    Returns:
        Data with restrictions applied.
    """

    cause = cm.create_bundle_restrictions('current')
    cause = cause[['bundle_id', 'male', 'female',
                   'yld_age_start', 'yld_age_end']].copy()
    cause = cause.drop_duplicates()

    # replace values below 1 with zero in get_cause, we don't differentiate
    # under one years old.
    cause['yld_age_start'].loc[cause['yld_age_start'] < 1] = 0

    # merge get_cause_metadata onto hospital using cause_id map
    pre_cause = df.shape[0]
    df = df.merge(cause, how='left', on='bundle_id')
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


def get_sample_size_outpatient(df, gbd_round_id, decomp_step, fix_top_age=True):
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
    pop = get_population(age_group_id=list(df.age_group_id.unique()),
                         location_id=list(df.location_id.unique()),
                         sex_id=[1, 2],
                         year_id=list(df.year_start.unique()),
                         gbd_round_id=gbd_round_id,
                         decomp_step=decomp_step)

    if fix_top_age:

        # make pop for age_group_id 160
        # age_group_id 160 is made of 31, 32, and 235
        pop_160 = get_population(
            age_group_id=[31, 32, 235],
            location_id=list(df.location_id.unique()),
            sex_id=[1, 2],
            year_id=list(df.year_start.unique()),
            gbd_round_id=gbd_round_id,
            decomp_step=decomp_step)
        pre = pop_160.shape[0]
        pop_160['age_group_id'] = 160
        pop_160 = pop_160.groupby(pop_160.columns.drop('population').tolist()).agg(
            {'population': 'sum'}).reset_index()
        assert pre / 3.0 == pop_160.shape[0]

        # make pop for age_group_id 21
        # age_group_id 21 is made of 30, 31, 32, 235
        pop_21 = get_population(
            age_group_id=[30, 31, 32, 235],
            location_id=list(df.location_id.unique()),
            sex_id=[1, 2],
            year_id=list(df.year_start.unique()),
            gbd_round_id=gbd_round_id,
            decomp_step=decomp_step)
        pre = pop_21.shape[0]
        pop_21['age_group_id'] = 21
        pop_21 = pop_21.groupby(pop_21.columns.drop('population').tolist()).agg(
            {'population': 'sum'}).reset_index()
        assert pre / 4.0 == pop_21.shape[0]
        pop = pd.concat([pop, pop_160, pop_21], ignore_index=True, sort=False)

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
    """.format(pre_shape, df.shape[0], pre_shape - df.shape[0])
    assert pre_shape == df.shape[0], assert_msg

    assert df.isnull().sum().sum() == 0, 'there are nulls'

    print("Done getting sample size")

    return(df)


def outpatient_elmo(df, gbd_round_id, make_right_inclusive=True):
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

    df = df.drop(['source', 'facility_id', 'metric_id'],
                 axis=1)
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
    df['measure'].replace(
        ["prev", "inc"], ["prevalence", "incidence"], inplace=True)

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
    df['design_effect'] = np.nan_to_num
    df['response_rate'] = np.nan
    df['extractor'] = USER

    # human readable location names
    loc_map = get_location_metadata(
        location_set_id=35, gbd_round_id=gbd_round_id)
    loc_map = loc_map[['location_id', 'location_name']]
    df = df.merge(loc_map, how='left', on='location_id')

    # add bundle_name
    bundle_name_df = query(QUERY)

    pre_shape = df.shape[0]
    df = df.merge(bundle_name_df, how="left", on="bundle_id")
    assert df.shape[0] == pre_shape, "added rows in merge"
    assert df.bundle_name.notnull().all().all(), 'bundle name df has nulls'

    print("DONE WITH ELMO")
    return(df)
