import sys
import os

import pandas as pd
import numpy as np

from metric_functions import calc_sample_size, convert_to_cause_fractions
from cod_prep.claude.cf_adjustments import (
    MaternalHIVRemover,
    SampleSizeCauseRemover,
    AnemiaAdjuster,
    RTIAdjuster
)
from cod_prep.claude.aggregators import (
    CauseAggregator,
    LocationAggregator
)
from parent_garbage import ParentMappedAggregatedGarbageAdder
from cod_prep.claude.count_adjustments import EnvelopeLocationSplitter
from claude_io import write_phase_output, get_phase_output
from cod_prep.claude.configurator import Configurator
from cod_prep.downloaders.ages import get_ages
from cod_prep.downloaders.causes import get_current_cause_hierarchy
from cod_prep.downloaders.locations import (
    get_current_location_hierarchy, add_location_metadata
)
from cod_prep.downloaders.nids import get_value_from_nid, add_survey_type
from cod_prep.downloaders.population import get_env
from cod_prep.downloaders.engine_room import (
    get_cause_map, get_package_map, get_cause_package_hierarchy
)
from cod_prep.claude.hiv_maternal_pafs import HIVMatPAFs
from cod_prep.utils import (
    print_log_message, report_duplicates, report_if_merge_fail
)
from cod_prep.claude.squaring import Squarer
from cod_prep.claude.redistribution_variance import (
    dataset_has_redistribution_variance,
    RedistributionVarianceEstimator
)

VA_DATA_TYPE = 8

POLICE_SURVEY_DATA_TYPE = [4, 5, 6, 7]

POLICE_DATA_TYPE  = 4

CC_CODE = 919

CONF = Configurator('standard')

MATERNAL_SQUARED = [
    "Mexico_BIRMM", "SUSENAS",
    "China_MMS", "China_Child", "Other_Maternal"
]


def intify_cols(df):
    """Convert ids that should be integers."""
    df['cause_id'] = df['cause_id'].astype(int)
    df['age_group_id'] = df['age_group_id'].astype(int)
    df['nid'] = df['nid'].astype(int)
    return df


def drop_cc_code(df):
    """Remove cc code from the dataframe."""
    df = df.query('cause_id != {}'.format(CC_CODE))
    return df


def conform_one_like_cf_to_one(df, cf_cols=['cf', 'cf_raw', 'cf_corr', 'cf_rd']):
    """Make any cf that is basically 1 (np.isclose(cf, 1)) equal to 1."""
    for cf_col in cf_cols:
        df.loc[(df[cf_col] > 1) & (np.isclose(df[cf_col], 1)), cf_col] = 1
    return df


def assert_valid_cause_fractions(df, cf_cols=['cf']):
    """Ensure cause fractions can be used by noise reduction."""
    assert df[cf_cols].notnull().values.all()
    assert (df[cf_cols] >= 0).values.all()
    assert (df[cf_cols] <= 1).values.all()


def log_statistic(df):
    """Log deaths and cause fractions."""
    if 'cf' in df.columns:
        vcols = [x for x in df.columns if x.startswith('cf')]
    elif 'deaths' in df.columns:
        vcols = [x for x in df.columns if x.startswith('deaths')]
    else:
        return "rows={}".format(len(df))
    value_dict = {}
    for vcol in vcols:
        value_dict[vcol] = "min={}, max={}".format(
            round(df[vcol].min(), 3),
            round(df[vcol].max(), 3)
        )
    value_dict["rows"] = len(df)
    return value_dict


def prune_cancer_registry_data(df, location_meta_df):
    """ A bit of a hacky place to remove non-detailed locations

    if these are not removed, then there will be data in the national aggregate
    locations that comes from aggregating with site id 2, as well as data
    from the non-aggregated output with whatever original site id was in
    there. This causes problems, one of which is cause fractions over 1

    """

    # Ukraine data was supposed to be without Crimea Sevastapol
    ukraine_nid_extract = (df['nid'] == 284465) & (df['extract_type_id'] == 53)
    assert (df[ukraine_nid_extract]['location_id'] == 63).all(), \
        "Now ukraine data has more than just ukraine national, and code " \
        "should be changed"
    df.loc[ukraine_nid_extract, 'location_id'] = 50559

    df = add_location_metadata(
        df, ['most_detailed'], location_meta_df=location_meta_df
    )
    report_if_merge_fail(df, 'most_detailed', 'location_id')
    # make exception for Ukraine data that gets split up later
    df = df.query('most_detailed == 1')
    df = df.drop('most_detailed', axis=1)
    return df


def square_maternal_sources(df, cause_meta_df, age_meta_df):
    """Square certain maternal data sources.

    Note: this step was in noise reduction, however, it
    needs to be here BEFORE cc_code is dropped.
    """
    squarer = Squarer(cause_meta_df, age_meta_df)
    df = add_survey_type(df)
    dhs = df['survey_type'] == "DHS"
    if len(df[dhs]) > 0:
        non_dhs = df[~dhs]
        dhs = df[dhs]
        dhs = squarer.get_computed_dataframe(dhs)
        dhs['nid'].fillna(nid, inplace=True)
        dhs['extract_type_id'].fillna(extract_type_id, inplace=True)
        df = pd.concat([dhs, non_dhs], ignore_index=True)
    else:
        df = squarer.get_computed_dataframe(df)
        df = df.query("sample_size > 0")

    # cleanup
    df.drop("survey_type", axis=1, inplace=True)
    df = drop_cc_code(df)

    # checks
    assert df.notnull().values.any()
    report_duplicates(df, ['year_id', 'sex_id', 'location_id', 'cause_id',
                           'age_group_id', 'nid', 'extract_type_id', 'site_id'])
    return df


def run_phase(df, cause_set_version_id, location_set_version_id,
              data_type_id, env_run_id, source, nid, extract_type_id,
              remove_decimal, code_map_version_id, iso3):
    """Run the full pipeline, chaining together CodProcesses."""
    configurator = Configurator('standard')
    cache_dir = configurator.get_directory('db_cache')
    cache_options = {
        'block_rerun': True,
        'cache_dir': cache_dir,
        'force_rerun': False,
        'cache_results': False
    }

    # get cause hierarchy
    cause_meta_df = get_current_cause_hierarchy(
        cause_set_version_id=cause_set_version_id,
        **cache_options
    )

    # get location hierarchy
    location_meta_df = get_current_location_hierarchy(
        location_set_version_id=location_set_version_id,
        **cache_options
    )

    # get envelope
    env_meta_df = get_env(env_run_id=env_run_id, **cache_options)

    # get env with HIV
    env_hiv_meta_df = get_env(
        env_run_id=env_run_id, with_hiv=True, **cache_options
    )

    # get age groups
    age_meta_df = get_ages(**cache_options)

    code_system_id = int(get_value_from_nid(nid, 'code_system_id',
                                            extract_type_id=extract_type_id))

    cause_map = get_cause_map(
        code_map_version_id=code_map_version_id, **cache_options
    )

    package_map = get_package_map(
        code_system_id=code_system_id, **cache_options
    )

    disagg_df = get_phase_output("disaggregation", nid, extract_type_id)
    misdc_df = get_phase_output("misdiagnosiscorrection", nid, extract_type_id)

    cause_package_hierarchy = get_cause_package_hierarchy(code_system_id, **cache_options)

    if source == "Cancer_Registry":
        df = prune_cancer_registry_data(df, location_meta_df)

    # aggregate location
    # defaults to simple location -> national aggregation
    # running full aggregation for India Survey data
    print_log_message("Aggregating location to country level")
    location_aggregator = LocationAggregator(df, location_meta_df)
    if (data_type_id == 7) & (iso3 == 'IND'):
        df = location_aggregator.get_computed_dataframe('full')
    else:
        df = location_aggregator.get_computed_dataframe()

    if data_type_id in POLICE_SURVEY_DATA_TYPE:
        # special step to remove HIV from maternal data
        print_log_message("Removing HIV from cc_code for maternal data.")
        maternal_hiv_remover = MaternalHIVRemover(
            df, env_meta_df, env_hiv_meta_df, source, nid
        )
        df = maternal_hiv_remover.get_computed_dataframe()

    print_log_message("Calculating sample size")
    df = calc_sample_size(df)
    print_log_message(log_statistic(df))

    print_log_message("Converting to cause fractions")
    df = df.loc[df['sample_size'] > 0]
    df = convert_to_cause_fractions(
        df, ['deaths', 'deaths_rd', 'deaths_corr', 'deaths_raw']
    )
    print_log_message(log_statistic(df))

    if data_type_id == VA_DATA_TYPE:
        # run VA anemia adjusment
        print_log_message("Running VA Anemia adjustment")
        va_anemia_adjuster = AnemiaAdjuster()
        df = va_anemia_adjuster.get_computed_dataframe(df)

    if data_type_id == POLICE_DATA_TYPE:
        if source == 'Various_RTI':
            rti_adjuster = RTIAdjuster(df, cause_meta_df, age_meta_df,
                                       location_meta_df)
            df = rti_adjuster.get_computed_dataframe()

    if data_type_id in POLICE_SURVEY_DATA_TYPE:
        # issue: rows with > 0 sample size are dropped
        # most common in maternal data, but relevant anywhere
        # we have only cc_code and one other cause and there
        # are 0 deaths for the other cause for a given age/sex
        cause_list = df.cause_id.unique()
        square_me = (len(cause_list) == 2) & (CC_CODE in cause_list)
        if (source in MATERNAL_SQUARED) or square_me:
            print_log_message("Squaring maternal data")
            df = square_maternal_sources(df, cause_meta_df, age_meta_df)

    print_log_message("Dropping cc code")
    df = drop_cc_code(df)
    print_log_message(log_statistic(df))

    print_log_message("Splitting locations.")
    env_loc_splitter = EnvelopeLocationSplitter(df, env_meta_df, source)
    df = env_loc_splitter.get_computed_dataframe()
    print_log_message(log_statistic(df))

    # aggregate causes
    print_log_message("Aggregating causes")
    cause_aggregator = CauseAggregator(df, cause_meta_df, source)
    df = cause_aggregator.get_computed_dataframe()
    print_log_message(log_statistic(df))

    print_log_message("Adding parnt-mapped garbage to aggregated causes")
    parent_gbg_adder = ParentMappedAggregatedGarbageAdder(
        nid, extract_type_id, source, cause_package_hierarchy, cause_meta_df,
        package_map, cause_map, remove_decimal, disagg_df,
        misdc_df
    )
    df = parent_gbg_adder.get_computed_dataframe(df)

    print_log_message(
        "Applying hiv-prevalance in pregnancy adjustment to "
        "maternal deaths"
    )
    hmp = HIVMatPAFs()
    df = hmp.get_computed_dataframe(
        df, cause_meta_df, location_meta_df)
    print_log_message(log_statistic(df))

    # TO DO
    # ** In the recode step for BTL some cancer deaths were moved to the
    # cancer parent. The squaring step created 0's. Get rid of the 0's in
    # country-years the recode was previously applied to.

    print_log_message(
        "Removing HIV and shocks from cause fraction denominator")
    hiv_shock_remover = SampleSizeCauseRemover(cause_meta_df)
    df = hiv_shock_remover.get_computed_dataframe(df)
    print_log_message(log_statistic(df))

    # not sure why we do this, but could use a comment of some kind.
    df = conform_one_like_cf_to_one(df)

    print_log_message("Verifying cause fractions not null between 0 and 1")
    assert_valid_cause_fractions(df)

    if dataset_has_redistribution_variance(data_type_id, source):
        # Determine the redistribution variance
        rdvar = RedistributionVarianceEstimator(
            nid, extract_type_id, cause_meta_df, remove_decimal,
            code_system_id, cause_map, package_map, code_map_version_id=code_map_version_id
        )
        df = rdvar.get_computed_dataframe(df, **cache_options)

    return df


def main(nid, extract_type_id, remove_decimal, code_map_version_id,
         launch_set_id):
    print_log_message("Reading corrections data")
    df = get_phase_output(
        'corrections', nid=nid, extract_type_id=extract_type_id
    )

    data_type_id = get_value_from_nid(
        nid, 'data_type_id',
        extract_type_id=extract_type_id)

    cause_set_version_id = CONF.get_id('cause_set_version')
    location_set_version_id = CONF.get_id('location_set_version')
    env_run_id = CONF.get_id('env_run')

    source = get_value_from_nid(nid, 'source',
                                extract_type_id=extract_type_id)

    iso3 = get_value_from_nid(nid, 'iso3',
                                extract_type_id=extract_type_id)

    # run the pipeline
    df = run_phase(df, cause_set_version_id, location_set_version_id,
                   data_type_id, env_run_id, source, nid, extract_type_id,
                   remove_decimal, code_map_version_id, iso3)

    # upload to database
    print_log_message(
        "Writing {n} rows of output for launch set {ls}, nid {nid}, extract "
        "{e}".format(n=len(df), ls=launch_set_id, e=extract_type_id, nid=nid)
    )
    df = intify_cols(df)
    write_phase_output(df, 'aggregation', nid, extract_type_id, launch_set_id)


if __name__ == "__main__":
    nid = int(sys.argv[1])
    extract_type_id = int(sys.argv[2])
    # these arguments are only used for redistribution variance
    remove_decimal = sys.argv[3]
    assert remove_decimal in ["True", "False"], \
        "invalid remove_decimal: {}".format(remove_decimal)
    remove_decimal = (remove_decimal == "True")
    code_map_version_id = int(sys.argv[4])
    launch_set_id = int(sys.argv[5])
    main(nid, extract_type_id, remove_decimal, code_map_version_id,
         launch_set_id)
