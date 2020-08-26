import sys
import os
import json
import shutil

import numpy as np
import pandas as pd

import db_queries


from cod_prep.downloaders import (
    get_package_list,
    get_country_level_location_id,
    get_cause_map,
    add_location_metadata,
    get_current_location_hierarchy
)
from cod_prep.utils import (
    print_log_message,
    report_duplicates,
    report_if_merge_fail,
    cod_timestamp
)
from cod_prep.claude.claude_io import get_claude_data, makedirs_safely
from cod_prep.claude.configurator import Configurator
from save_proportions_for_tableau import SharedPackage

CONF = Configurator()

MODEL_DATA_CODE_SYSTEMS = [1, 6]
RDP_REG_DIR = CONF.get_directory('rdp_regressions')


def get_package_code_ids(regression_specification, code_system_id):
    """Returns code_ids for garbage codes in package for given code system"""
    package_description = regression_specification[
        'package_descriptions'
    ][code_system_id]

    packages = get_package_list(code_system_id)
    package_id = packages.loc[
        packages['package_description'] == package_description,
        'package_id'
    ]
    assert len(package_id) == 1
    package_id = package_id.iloc[0]

    pkg = json.load(open(
        "FILEPATH".format(code_system_id, package_id)))
    garbage_codes = list(pkg['garbage_codes'])

    code_map = get_cause_map(code_system_id=code_system_id, force_rerun=False)
    code_map['value'] = code_map['value'].str.replace(".", "")
    is_package_garbage = code_map['value'].isin(garbage_codes)

    garbage_code_ids = list(code_map.loc[
        is_package_garbage,
        'code_id'
    ].unique())

    return garbage_code_ids


def collapse_to_reg_df(df, garbage_code_ids, target_cause_ids, country_loc_map):
    """Exec function for get_claude_data call. Filter data to only relevant
    codes (aka codes that fall into garbage or package targets) as well
    as locations, and then collapse to the cause level."""

    # filter out locations not in hierarchy
    df = df[df['location_id'].isin(country_loc_map.keys())]

    # replace location with country location
    df['location_id'] = df['location_id'].map(country_loc_map)

    # add national sample size
    df['sample_size'] = df.groupby([
        'nid', 'extract_type_id', 'location_id',
        'year_id', 'age_group_id', 'sex_id'
    ])['deaths'].transform(np.sum)

    # keep only relevant garbage or targets
    is_package_garbage = df['code_id'].isin(garbage_code_ids)
    is_package_target = df['cause_id'].isin(target_cause_ids)
    df = df[is_package_garbage | is_package_target]

    # collapse to cause_id level
    df = df.groupby([
        'nid', 'extract_type_id', 'location_id',
        'year_id', 'age_group_id', 'sex_id',
        'cause_id', 'sample_size'
    ], as_index=False)['deaths'].sum()

    return df


def get_country_loc_id_map(location_hierarchy):
    """Creates a map of location_id -> country id, meaning countries are
    mapped to themselves and subnationals are mapped to their parent country.
    This is so that we can aggregate data up to the country level.
    """
    all_locs = list(location_hierarchy.query('level >= 3')['location_id'].unique())
    country_location_map = get_country_level_location_id(all_locs, location_hierarchy)
    country_location_map = country_location_map.set_index('location_id').to_dict()['country_location_id']
    return country_location_map


def pull_vr_data_for_rdp_reg(reg_spec, location_hierarchy, data_id, small_test=False,
                             vr_pull_timestamp=None):
    """Pull vr used to make redistribution proportions.

    If vr_pull_timestamp is passed, and it does in fact exist, then this will
    just read that. Otherwise, it runs a custom get_claude_data based on
    the passed regression specification.
    """

    shared_package_id = reg_spec['shared_package_id']

    if vr_pull_timestamp is not None:
        timestamp = vr_pull_timestamp
    else:
        timestamp = cod_timestamp()

    outdir = "FILEPATH".format(RDP_REG_DIR, shared_package_id)
    outpath = "FILEPATH".format(outdir, data_id, timestamp)

    if vr_pull_timestamp is not None:
        print_log_message("Reading VR data pulled on {}".format(vr_pull_timestamp))
        if not os.path.exists(outpath):
            raise ValueError(
                "Passed [vr_pull_timestamp={}], but {} does not exist. "
                "Need to either pass a different version that does exist, or"
                " run a new vr pull by passing vr_pull_timestamp=None.".format(
                    vr_pull_timestamp, outpath)
            )
        df = pd.read_csv(outpath)

    else:
        print_log_message(
            "Pulling a fresh version of VR with timestamp {}".format(
                timestamp)
        )
        # regressions only use detailed code systems
        code_system_id = MODEL_DATA_CODE_SYSTEMS

        # regressions only use national-level data to avoid biasing the sample
        # toward subnational datasets
        country_loc_map = get_country_loc_id_map(location_hierarchy)

        if small_test:
            year_id = [2010, 2011]
            print("Pulling data for year subset: {}".format(year_id))
        else:
            year_id = range(1980, 2018)

        dfs = []
        for code_system_id in code_system_id:
            print_log_message("Code system id: {}".format(code_system_id))
            garbage_code_ids = get_package_code_ids(reg_spec, code_system_id)
            target_cause_ids = reg_spec['target_cause_ids']
            df = get_claude_data(
                "disaggregation",
                data_type_id=9, code_system_id=code_system_id,
                is_active=True, year_id=year_id, location_set_id=35,
                exec_function=collapse_to_reg_df,
                exec_function_args=[garbage_code_ids, target_cause_ids, country_loc_map],
                attach_launch_set_id=True
            )
            dfs.append(df)
        df = pd.concat(dfs, ignore_index=True)

        df['vr_pull_timestamp'] = timestamp

        df.to_csv(outpath, index=False)

    return df


def add_model_group(df, with_age=False):
    df = df.copy()
    if with_age:
        df['model_group'] = df[['sex_id', 'cause_id', 'age']].apply(
            lambda x: "{}_{}_{}".format(x['cause_id'], x['sex_id'], x['age']), axis=1
        )
    else:
        df['model_group'] = df[['sex_id', 'cause_id']].apply(
            lambda x: "{}_{}".format(x['cause_id'], x['sex_id']), axis=1
        )
    return df


def reshape_wide_to_garbage_target(df, reg_spec):

    df = df.copy()

    gar_val_col = "prop_garbage"
    tgt_val_col = "prop_target"

    dem_cols = ['location_id', 'year_id', 'year_window', 'age_group_id',
                'age', 'sex_id', 'vr_pull_timestamp']
    id_cols = dem_cols + ['cause_id']
    keep_cols = id_cols + ['deaths',
                           'prop_pkgtarg', 'prop_pkgtarg_bigage',
                           'prop_pkgtarg_all_years',
                           'prop_pkgtarg_year_window']

    target_causes = reg_spec['target_cause_ids']

    df = df[keep_cols].copy()
    report_duplicates(df, id_cols)

    gar_df = df.query('cause_id == 743')  # garbage codes
    gar_df = gar_df.rename(columns={
        'deaths': "deaths_garbage",
        'prop_pkgtarg': gar_val_col,
        'prop_pkgtarg_bigage': gar_val_col + "_bigage",
        'prop_pkgtarg_all_years': gar_val_col + "_all_years",
        'prop_pkgtarg_year_window': gar_val_col + '_year_window'
    })
    gar_df = gar_df.drop('cause_id', axis=1)

    tar_df = df.query('cause_id != 743')
    tar_df = tar_df.rename(columns={
        'deaths': "deaths_target",
        'prop_pkgtarg': tgt_val_col,
        'prop_pkgtarg_bigage': tgt_val_col + '_bigage',
        'prop_pkgtarg_all_years': tgt_val_col + "_all_years",
        'prop_pkgtarg_year_window': tgt_val_col + '_year_window'
    })

    df = tar_df.merge(gar_df, on=dem_cols, how='outer')


    df['cause_id'] = df['cause_id'].astype(int)

    return df


def add_reg_location_metadata(df, location_hierarchy):

    df = add_location_metadata(df, ['region_id', 'super_region_id'], location_meta_df=location_hierarchy)
    report_if_merge_fail(df, 'region_id', 'location_id')
    df['region_id'] = df['region_id'].astype(int)
    report_if_merge_fail(df, 'super_region_id', 'location_id')
    df['super_region_id'] = df['super_region_id'].astype(int)
    df = df.rename(columns={
        'super_region_id': 'super_region',
        'region_id': 'region',
        'location_id': 'country'
    })
    df['global'] = 1
    return df


def add_proportional_prop(df, target_col):
    df = df.copy()
    # calculate the global proportion of each target of age-sex deaths as a backup proportion
    df['target_deaths'] = df[target_col] * df['sample_size']
    df['age_sex_group_deaths'] = df.groupby(['age', 'sex_id']).target_deaths.transform(np.sum)
    df['age_sex_group_target_deaths'] = df.groupby(['age', 'sex_id', 'cause_id']).target_deaths.transform(np.sum)
    df['cf_target_proportional'] = df['age_sex_group_target_deaths'] / df['age_sex_group_deaths']
    df = df.drop(['target_deaths', 'age_sex_group_deaths', 'age_sex_group_target_deaths', 'sample_size'], axis=1)
    return df


def add_age_group(df, reg_spec):
    df = df.copy()
    # ADD AGE
    if "cancer" in reg_spec["name_short"]:
        age_group_map = {
            0: [2, 3, 4, 5, 6, 7],
            15: [8, 9, 10],
            30: [11, 12, 13],
            45: [14, 15, 16],
            60: [17, 18, 19],
            70: [20, 30, 31, 32, 235]
        }
    else:
        age_group_map = {
            0: [2, 3, 4, 5, 6, 7],
            15: [8, 9, 10],
            30: [11, 12, 13],
            45: [14, 15, 16],
            60: [17, 18],
            70: [19, 20],
            80: [30, 31, 32, 235]
        }

    id_to_age_group_map = dict()
    for key in age_group_map.keys():
        vals = age_group_map[key]
        for val in vals:
            assert val not in id_to_age_group_map.keys(), "Duplicate age group: {}".format(val)
            id_to_age_group_map[val] = key
    df['age'] = df['age_group_id'].map(id_to_age_group_map)
    return df


def add_year_window(df):
    """
    Adds a column, year_window, to df so that data can be aggregated into
    year ranges.
    """
    start = min(df.year_id)
    end = max(df.year_id)

    df.loc[df.year_id <= 2004, "year_window"] = "{} - 2004".format(int(start))
    df.loc[df.year_id > 2004, "year_window"] = "2005 - {}".format(int(end))

    return df


def add_covariate(df, covariate_id, covariate_column_name, by_sex=True):

    assert 'location_id' not in df.columns, "Unexpected df structure: has location_id"
    assert 'country' in df.columns, "Unexpected df structure: lacks country"

    merge_cols = ['country', 'year_id', 'sex_id']
    cov_df = None

    if not by_sex:
        merge_cols.remove('sex_id')

    # For covariates passed to me via flat files (aka .csvs), I manually
    # assign them negative covariate values. Handle them as a separate case
    if (covariate_id < 0):
        cov_df, merge_cols = get_flat_covariate_estimates(covariate_id)
    else:
        cov_df = db_queries.get_covariate_estimates(covariate_id, decomp_step="step1")

    cov_df = cov_df.rename(columns={
        'location_id': 'country',
        'mean_value': covariate_column_name
    })

    cov_df = cov_df[merge_cols + [covariate_column_name]]
    report_duplicates(cov_df, merge_cols)
    df = df.merge(cov_df, on=merge_cols, how='left')

    # As of 2/7/19, flat files go from 1990 - 2017, so missing
    # 80s throws an error. I deal with this in regression setup
    if (covariate_id > 0):
        report_if_merge_fail(df, covariate_column_name, merge_cols)
    return df


def get_flat_covariate_estimates(covariate_id):
    """ Loads covariate values stored in flat files (.csvs),
    NOT in any database. Since these are custom files sent to me, each covar id
    is manually handled.

    Covariate flat file names are expected to be in the form:
    FILEPATH

    Returns: tuple of (cov_df, merge_cols)
    """
    base_file = "FILEPATH".format(RDP_REG_DIR)
    merge_cols = ['country', 'year_id', 'sex_id', 'age']
    if covariate_id == -1:  # AGE-SPECIFIC LDL (cholesterol) levels
        file = "{}-1_ldl_age_specific.csv".format(base_file)
        cov_df = pd.read_csv(file)
    elif covariate_id == -2:  # AGE-SPECIFIC blood pressure
        file = "{}-2_sbp_age_specific.csv".format(base_file)
        cov_df = pd.read_csv(file)
    elif covariate_id == -3:  # STROKE SURVIVORSHIP (in 28 days?), tbh not sure
        file = "{}-3_stroke_survivor_i64_split.csv".format(base_file)
        merge_cols.append("cause_id")
        cov_df = pd.read_csv(file)

        cov_df = cov_df.reset_index()
        cov_df = pd.melt(cov_df, 
                         id_vars=["age", "location_id", "sex_id", "year_id"],
                         value_vars=['495', '496', '497'])
        cov_df = cov_df.rename(columns={
            'variable': 'cause_id',
            'value': 'mean_value'
        })
        cov_df["cause_id"] = pd.to_numeric(cov_df["cause_id"])

        # expand years from 1995, 2000, 2005, etc to
        # 1990, 1991, 1992, etc
        years = pd.DataFrame(data={"year_id_extend": range(1990, 2018),
                                   "merge_var": 1})
        cov_df['merge_var'] = 1  # dummy var

        cov_df = cov_df.merge(years, on='merge_var', how='left')

        # Back fill data 5 years except for 2010, which is also front filled to 2017
        cov_df= cov_df.loc[((cov_df.year_id == 1995) & (cov_df.year_id_extend == 1990)) |
                           ((cov_df.year_id == 2010) & (cov_df.year_id_extend >= 2010)) |
                           (cov_df.year_id_extend <= cov_df.year_id) &
                           (cov_df.year_id_extend > cov_df.year_id - 5)]
        cov_df = cov_df.drop(['merge_var', 'year_id'], axis=1)
        cov_df = cov_df.rename(columns={
            'year_id_extend': 'year_id'
        })
    elif covariate_id == -4:  # type 1 diabetes prevalence
        # Passed by Liane 2/25/19
        # All countries in GBD 2019, new countries are backfilled (by region?)
        file = "{}-4_type1_diabetes_prevalence.csv".format(base_file)
        cov_df = pd.read_csv(file)
    elif covariate_id == -5:  # stroke incidence porportions,
        # ie. of all stroke incident cases, what proportion are ischemic, etc
        # USING same process as for stroke survivorship (-3)
        file = "{}-5_stroke_incidence_proportion_by_target.csv".format(base_file)
        merge_cols.append("cause_id")
        cov_df = pd.read_csv(file)

        cov_df = cov_df.reset_index()
        cov_df = pd.melt(cov_df, 
                         id_vars=["age", "location_id", "sex_id", "year_id"],
                         value_vars=['495', '496', '497'])
        cov_df = cov_df.rename(columns={
            'variable': 'cause_id',
            'value': 'mean_value'
        })
        cov_df["cause_id"] = pd.to_numeric(cov_df["cause_id"])

        # expand years from 1995, 2000, 2005, etc to
        # 1990, 1991, 1992, etc
        years = pd.DataFrame(data={"year_id_extend": range(1990, 2018),
                                   "merge_var": 1})
        cov_df['merge_var'] = 1  # dummy var

        cov_df = cov_df.merge(years, on='merge_var', how='left')

        # Back fill data 5 years except for 2010, which is also front filled to 2017
        cov_df= cov_df.loc[((cov_df.year_id == 1995) & (cov_df.year_id_extend == 1990)) |
                           ((cov_df.year_id == 2010) & (cov_df.year_id_extend >= 2010)) |
                           (cov_df.year_id_extend <= cov_df.year_id) &
                           (cov_df.year_id_extend > cov_df.year_id - 5)]
        cov_df = cov_df.drop(['merge_var', 'year_id'], axis=1)
        cov_df = cov_df.rename(columns={
            'year_id_extend': 'year_id'
        })
    else:
        raise NotImplementedError("Covariate id {} has not been added. If "
                                  "this is a mistake, add above!".format(covariate_id))

    return (cov_df, merge_cols)


def move_implausible_deaths(df):
    """Written to 'correct' implausibly coded diabetes
    deaths. Specifically, deaths coded to type 2 under age 15
    and deaths coded to type 1 in 50+ are moved to unspecifed diabetes garbage.
    """
    print_log_message("For diabetes, moving implausibly coded deaths to unspecified")
    unique_cols = ["location_id", "year_id", "age_group_id", "age", "sex_id",
                   "vr_pull_timestamp", "year_window"]
    df = df.drop("join_key", axis=1)

    # shape long to wide to have deaths by all causes on 1 row
    df = df.pivot_table(index=unique_cols, columns='cause_id', values='deaths')
    df = pd.DataFrame(df.to_records())

    # For ages < 15, move all type 2 deaths to garbage
    df.loc[df.age_group_id <= 7, '743'] += df.loc[df.age_group_id <= 7, '976']
    df.loc[df.age_group_id <= 7, '976'] = 0

    # For ages 50+, move all type 1 deaths to garbage
    df.loc[df.age_group_id >= 15, '743'] += df.loc[df.age_group_id >= 15, '975']
    df.loc[df.age_group_id >= 15, '975'] = 0

    # reshape back to long, clean up
    df = df.set_index(unique_cols).stack().reset_index()
    df = df.rename(columns={'level_7': 'cause_id',
                            0: 'deaths'})
    df['cause_id'] = df['cause_id'].astype(int)

    return(df)


def format_reg_data_for_modeling(df, reg_spec, location_hierarchy):
    # group together different nids on same datapoints
    # validly happens e.g. in china, since location_id is actually the
    # country-id and hong kong has a different nid than mainland china
    dem_cols = ['location_id', 'year_id', 'age_group_id', 'sex_id',
                'vr_pull_timestamp']
    group_cols = dem_cols + ['cause_id']
    df = df.groupby(group_cols, as_index=False)['deaths'].sum()

    # need each location-year-age-sex to have all the causes
    causes = df[['cause_id']].drop_duplicates()
    causes['join_key'] = 1
    demographics = df[dem_cols].drop_duplicates()
    demographics['join_key'] = 1
    demog_causes = demographics.merge(causes, on='join_key', how='outer')
    df = demog_causes.merge(df, how='left')
    df['deaths'] = df['deaths'].fillna(0)

    # add age group that fixed effects will be based on
    df = add_age_group(df, reg_spec)

    # add year windows, currently just for cancer for dealing
    # ICD coding changes
    df = add_year_window(df)

    # correct implausible diabetes input data by moving
    # these implausible deaths to unspecified garbage
    # if reg_spec["name_short"] == "unspec_diab":
    #    df = move_implausible_deaths(df)

    df['package_plus_target_deaths'] = df.groupby(
        ['location_id', 'year_id', 'age_group_id', 'sex_id']
    )['deaths'].transform(np.sum)

    df['package_plus_target_deaths_bigage'] = df.groupby(
        ['location_id', 'year_id', 'age', 'sex_id']
    )['deaths'].transform(np.sum)

    df['deaths_bigage'] = df.groupby(
        ['location_id', 'year_id', 'age', 'sex_id', 'cause_id']
    )['deaths'].transform(np.sum)

    # Adding section for running regressions will all years of data, but not
    # with year-specific results
    df['package_plus_target_deaths_all_years'] = df.groupby(
        ['location_id', 'age_group_id', 'sex_id']
    )['deaths'].transform(np.sum)

    df['deaths_all_years'] = df.groupby(
        ['location_id', 'age_group_id', 'sex_id', 'cause_id']
    )['deaths'].transform(np.sum)

    # Deaths columns for time windows
    df['package_plus_target_deaths_year_window'] = df.groupby(
        ['location_id', 'age_group_id', 'sex_id', 'year_window']
    )['deaths'].transform(np.sum)

    df['deaths_year_window'] = df.groupby(
        ['location_id', 'age_group_id', 'sex_id', 'cause_id', 'year_window']
    )['deaths'].transform(np.sum)

    df['prop_pkgtarg'] = df['deaths'] / df['package_plus_target_deaths']
    df['prop_pkgtarg_bigage'] = df['deaths_bigage'] / df['package_plus_target_deaths_bigage']
    df['prop_pkgtarg_all_years'] = df['deaths_all_years'] / df['package_plus_target_deaths_all_years']
    df['prop_pkgtarg_year_window'] = df['deaths_year_window'] / df['package_plus_target_deaths_year_window']

    # reshape such that each row has cf_target and cf_garbage
    df = reshape_wide_to_garbage_target(df, reg_spec)

    # add location data that can be used in regression
    df = add_reg_location_metadata(df, location_hierarchy)

    # add model group (units of analysis for stata)
    df = add_model_group(df)

    # add backup proportion
    # df = add_proportional_prop(df, "{}_target".format('cf'))

    # add covariates
    if reg_spec['name_short'] == "unspec_diab":
        # age-standardized diabetes prevalence
        df = add_covariate(df, 29, "diabetes_prev")
        # under-15 age-standardized death rate due to diabetes
        df = add_covariate(df, 1249, "asdr_dm_015")
        # haqi
        df = add_covariate(df, 1099, "haqi", by_sex=False)
        # age-specific obesity
        df = add_covariate(df, 455, "prev_obesity_agestd")
        # age-specific type-1 diabetes prevalence
        df = add_covariate(df, -4, "prev_type1")

    if reg_spec['name_short'] == "unspec_stroke":
        # cholesterol
        df = add_covariate(df, 69, "cholesterol")
        # blood pressure
        df = add_covariate(df, 70, "blood_pressure")
        # age-specific LDL
        df = add_covariate(df, -1, "ldl_age_specific")
        # age-specific blood pressure
        df = add_covariate(df, -2, "blood_pressure_age_specific")
        # stroke 28 day survivorship
        df = add_covariate(df, -3, "percent_survivor")
        # stroke 28 day survivorship
        df = add_covariate(df, -5, "percent_incidence")
    if "cancer" in reg_spec["name_short"]:
        # haqi
        df = add_covariate(df, 1099, "haqi", by_sex=False)
        # tobacco (cigarettes per capita)
        df = add_covariate(df, 19, "cigarettes_pc", by_sex=False)

        df = add_covariate(df, 881, "sdi", by_sex=False)
    return df

def prepare_square_df(df, location_hierarchy, reg_spec):
    locations = location_hierarchy.query('level == 3')[['location_id']].drop_duplicates()
    ages = df[['age']].drop_duplicates()
    sexes = df[['sex_id']].drop_duplicates()
    targets = df[['cause_id']].drop_duplicates()
    years = df[['year_id']].drop_duplicates()

    join_dfs = [locations, years, ages, sexes, targets]
    square_df = join_dfs[0]
    square_df['join_key'] = 1
    for join_df in join_dfs[1:]:
        join_df['join_key'] = 1
        square_df = square_df.merge(join_df, on='join_key', how='left')
    square_df = square_df.drop('join_key', axis=1)

    square_df = square_df.rename(columns={'location_id': 'country'})

    # will predict based on prop unspecified, but within 15-year age groups
    unspec_props_id_cols = ['country', 'year_id', 'age', 'sex_id']
    unspec_props = df[
        unspec_props_id_cols + ['prop_garbage_bigage']
    ].drop_duplicates()
    report_duplicates(unspec_props, unspec_props_id_cols)

    # fill with age-sex median garbage unspecified where we don't have the
    # location-year
    median_unspec_age_sex = unspec_props.groupby(
        ['age', 'sex_id'], as_index=False
    )['prop_garbage_bigage'].median()
    median_unspec_age_sex = median_unspec_age_sex.rename(
        columns={'prop_garbage_bigage': 'agesex_median_prop_garbage'}
    )

    # add proportion unspecified by location-year where possible
    square_df = square_df.merge(
        unspec_props, on=unspec_props_id_cols, how='left'
    )
    # use age-sex where necessary
    square_df = square_df.merge(
        median_unspec_age_sex, on=['age', 'sex_id'], how='left'
    )

    # this should never have missings
    report_if_merge_fail(square_df, 'agesex_median_prop_garbage', ['age', 'sex_id'])

    square_df['prop_garbage'] = square_df['prop_garbage_bigage'].fillna(
        square_df['agesex_median_prop_garbage']
    )

    # create all-year proportion garbage column
    square_df['prop_garbage_all_years'] = square_df.groupby(
        ['country', 'age', 'sex_id', 'cause_id']
    )['prop_garbage'].transform(np.mean)

    # create year window proportion garbage col
    square_df = add_year_window(square_df)
    square_df['prop_garbage_year_window'] = square_df.groupby(
        ['country', 'age', 'sex_id', 'cause_id', 'year_window']
    )['prop_garbage'].transform(np.mean)

    square_df = square_df.rename(columns={'country': 'location_id'})
    square_df = add_location_metadata(
        square_df, ['region_id', 'super_region_id'],
        location_meta_df=location_hierarchy
    )

    square_df = square_df.rename(columns={
        'super_region_id': 'super_region',
        'region_id': 'region',
        'location_id': 'country',
    })
    square_df['super_region'] = square_df['super_region'].astype(int)
    square_df['region'] = square_df['region'].astype(int)
    square_df['global'] = 1
    square_df = square_df[
        ['global', 'super_region', 'region', 'country', 'year_id',
         'year_window', 'age', 'sex_id', 'cause_id',
         'prop_garbage', 'prop_garbage_all_years', 'prop_garbage_year_window']
    ]
    square_df = add_model_group(square_df)
    square_df = square_df.sort_values(
        by=['global', 'super_region', 'region', 'country', 'year_id', 'age', 'sex_id', 'cause_id']
    )

    if reg_spec['name_short'] == "unspec_diab":
        # haqi
        print_log_message("Adding haqi")
        square_df = add_covariate(square_df, 1099, "haqi", by_sex=False)
        # age-standardized diabetes prevalence
        print_log_message("Adding age-standardized diabetes prevalence")
        square_df = add_covariate(square_df, 29, "diabetes_prev")
        # under-15 age-standardized death rate due to diabetes
        print_log_message("Adding asdr due to diabetes under 15")
        square_df = add_covariate(square_df, 1249, "asdr_dm_015")
        # age-specific obesity
        print_log_message("Adding age-standardized prevalence of obesity")
        square_df = add_covariate(square_df, 455, "prev_obesity_agestd")
        # age-specific type-1 diabetes prevalence
        print_log_message("Adding type 1 diabetes prevalence")
        square_df = add_covariate(square_df, -4, "prev_type1")

    if reg_spec['name_short'] == "unspec_stroke":
        # cholesterol
        print_log_message("Adding cholesterol")
        square_df = add_covariate(square_df, 69, "cholesterol")
        # blood pressure
        print_log_message("Adding blood pressure")
        square_df = add_covariate(square_df, 70, "blood_pressure")
        # age-specific LDL
        print_log_message("Adding age-specific LDL")
        square_df = add_covariate(square_df, -1, "ldl_age_specific")
        # age-specific blood pressure
        print_log_message("Adding age-specific blood pressure")
        square_df = add_covariate(square_df, -2, "blood_pressure_age_specific")
        # stroke 28 day survivorship
        print_log_message("Adding percent survivor")
        square_df = add_covariate(square_df, -3, "percent_survivor")
        # stroke percent incidence by target group
        print_log_message("Adding percent incidence")
        square_df = add_covariate(square_df, -5, "percent_incidence")
    if "cancer" in reg_spec["name_short"]:
        # haqi
        print_log_message('Adding HAQI')
        square_df = add_covariate(square_df, 1099, "haqi", by_sex=False)
        # tobacco (cigarettes per capita)
        print_log_message('Adding cigarettes per capita')
        square_df = add_covariate(square_df, 19, "cigarettes_pc", by_sex=False)
        print_log_message('Adding SDI')
        square_df = add_covariate(square_df, 881, "sdi", by_sex=False)

    return square_df

def get_regression_specification(shared_package_id):
    """ Current methodology to get cause ids:
    from save_proportions_for_tableau import SharedPackage
    sp = SharedPackage(4)
    sp.get_target_cause_ids()
    """
    print_log_message("Getting shared package target cause ids: ")
    sp = SharedPackage(shared_package_id)

    if shared_package_id == 15:
        reg_spec = {
            'package_descriptions': {
                1: "Unspecified Stroke",
                6: "Unspecified Stroke"
            },
            'name_short': 'unspec_stroke'
        }
    elif shared_package_id == 2614:
        reg_spec = {
            'package_descriptions': {
                1: "Diabetes unspecified type",
                6: "Diabetes unspecified type"
            },
            'name_short': 'unspec_diab'
        }
    elif shared_package_id == 14:
        reg_spec = {
            'package_descriptions': {
                1: "Left Hf",
                6: "Left Hf"
            },
            'name_short': 'hf'
        }
    elif shared_package_id == 1:
        reg_spec = {
            'package_descriptions': {
                1: "Unspecified Oropharynx C",
                6: "Unspecified Oropharynx C"
            },
            'name_short': 'cancer_oropharynx'
        }
    elif shared_package_id == 2:
        reg_spec = {
            'package_descriptions': {
                1: "Unspecified Gi C",
                6: "Unspecified Gi C"
            },
            'name_short': 'cancer_gi'
        }
    elif shared_package_id == 4:
        reg_spec = {
            'package_descriptions': {
                1: "Unspecified Uterus  C",
                6: "Unspecified Uterus  C"
            },
            'name_short': 'cancer_uterus'
        }
    elif shared_package_id == 9:
        reg_spec = {
            'package_descriptions': {
                1: "Unspecified Site  C",
                6: "Unspecified Site  C"
            },
            'name_short': 'cancer_site_unspecified'
        }
    else:
        raise ValueError(
            "Unsupported shared package id: {}. If it should exist, feel "
            "free to add it above!".format(shared_package_id)
        )

    # unfortunate hack to deal with fact we need new causes added GBD 2019 for this
    # to run regression, but need new regression to be able to pull new causes
    if shared_package_id == 9:
        targets = pd.read_csv("FILEPATH".format(RDP_REG_DIR)).cause_id.tolist()
        reg_spec['target_cause_ids'] = targets
    else:
        reg_spec['target_cause_ids'] = sp.get_target_cause_ids()

    reg_spec['shared_package_id'] = shared_package_id
    print_log_message(reg_spec['target_cause_ids'])

    return reg_spec


def run_proportions_prep(shared_package_id, outdir, vr_pull_timestamp, data_id,
                         test=False):

    location_set_version = CONF.get_id('location_set_version')

    location_hierarchy = get_current_location_hierarchy(
        # location_set_version_id=location_set_version,
        gbd_round_id=5  # FIXME: change this when covars have all loc values!
    )

    reg_spec = get_regression_specification(shared_package_id)

    input_data_path = "FILEPATH".format(outdir, data_id)

    print_log_message("Running input data prep")
    df = pull_vr_data_for_rdp_reg(
        reg_spec, location_hierarchy, vr_pull_timestamp=vr_pull_timestamp,
        data_id=data_id, small_test=test
    )
    print_log_message("Formatting regression input")
    df = format_reg_data_for_modeling(
        df, reg_spec, location_hierarchy
    )
    # df = df.rename(columns={
    #     'prop_pkgtarg_target': 'cf_target',
    #     'prop_pkgtarg_garbage': 'cf_garbage'
    # })
    print_log_message("Writing regression input")
    df.to_csv(input_data_path, index=False)

    # # make square df
    print_log_message("Writing square dataset")
    square_df = prepare_square_df(df, location_hierarchy, reg_spec)
    square_df = add_model_group(square_df, with_age=False)
    square_df.to_csv("FILEPATH".format(outdir, data_id),
                     index=False)

    print_log_message("Done")


def copy_data_from_version(version, copy_version, outdir):
    """Copy input data and square df from another version.

    This is useful if nothing about the input data has changed, but you
    want to play with something in the model and create a new version to
    reflect that.
    """
    old_version_outdir = outdir.replace(version, copy_version)
    old_version_input_path = "FILEPATH".format(old_version_outdir)
    input_data_path = "FILEPATH".format(outdir)

    old_version_square_path = "FILEPATH".format(old_version_outdir)
    square_data_path = "FILEPATH".format(outdir)

    shutil.copy2(old_version_input_path, input_data_path)
    shutil.copy2(old_version_square_path, square_data_path)


def main(shared_package_id, data_id, copy_version=None,
         skip_input_data_prep=False, vr_pull_timestamp=None,
         test=False):

    rdp_reg_dir = CONF.get_directory('rdp_regressions')

    outdir = "FILEPATH".format(
        rdp_reg_dir, shared_package_id
    )
    makedirs_safely(outdir)
    makedirs_safely("FILEPATH".format(outdir))

    if copy_version is not None:
        # sometimes all you want is a new folder to run a different model
        # on the same data
        copy_data_from_version(version, copy_version, outdir)
    else:
        run_proportions_prep(
            shared_package_id, outdir,
            vr_pull_timestamp, data_id=data_id, test=test
        )


if __name__ == "__main__":
    shared_package_id = int(sys.argv[1])
    # version 2018_04_18
    # version = sys.argv[2]
    data_id = sys.argv[2]
    test = sys.argv[3]
    # copy_version = sys.argv[3]
    # you probably need to look this up - see this directory and choose a timestamp
    # (or pass none and it will download VR again, about a 1-2 hour process)
    # vr_pull_timestamp = sys.argv[4]

    # if copy_version.lower() == "none":
    #    copy_version = None

    # if vr_pull_timestamp.lower() == "none":
    #    vr_pull_timestamp = None

    main(
        shared_package_id, data_id, test=test
    )
