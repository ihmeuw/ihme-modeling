"""Final step before uploading data.

This phase includes raking, age aggregation, creating upper/lower bounds
for CoDViz, and calculating variances for CODEm
"""

import sys
import os

from cod_prep.utils import (
    print_log_message,
    enforce_asr
)
from cod_prep.claude.cf_adjustments import Raker
from cod_prep.claude.aggregators import AgeAggregator
from configurator import Configurator
from cod_prep.claude.redistribution_variance import (
    dataset_has_redistribution_variance,
    RedistributionVarianceEstimator
)
from cod_prep.claude.rate_adjustments import NonZeroFloorer

from claude_io import (
    get_claude_data,
    write_phase_output,
    get_datasets
)

from cod_prep.downloaders import (
    get_pop,
    get_env,
    get_current_location_hierarchy,
    get_value_from_nid,
    get_age_weights,
    get_current_cause_hierarchy,
    get_ages
)

CONF = Configurator('standard')
PHASE_ANTECEDENT = 'noisereduction'
PHASE_NAME = 'finalclean'
# sources that are noise reduced, but not raked
NOT_RAKED_SOURCES = [
    "Maternal_report", "SUSENAS", "China_MMS", "China_Child",
]
MATERNAL_NR_SOURCES = [
    "Mexico_BIRMM", "Maternal_report", "SUSENAS",
    "China_MMS", "China_Child",
]


def run_phase(df, nid, extract_type_id, env_run_id,
              pop_run_id, location_set_version_id, cause_set_version_id):
    cache_dir = CONF.get_directory('db_cache')
    source = get_value_from_nid(
        nid, 'source', extract_type_id=extract_type_id,
        location_set_version_id=location_set_version_id
    )
    data_type_id = get_value_from_nid(
        nid, 'data_type_id', extract_type_id=extract_type_id,
        location_set_version_id=location_set_version_id
    )
    iso3 = get_value_from_nid(nid, 'iso3', extract_type_id=extract_type_id,
        location_set_version_id=location_set_version_id
    )
    standard_cache_options = {
        'force_rerun': False,
        'block_rerun': True,
        'cache_dir': cache_dir,
        'cache_results': False
    }

# ************************************************************
# Get cached metadata
# ************************************************************
    print_log_message("Getting cached db resources")
    location_hierarchy = get_current_location_hierarchy(
        location_set_version_id=location_set_version_id,
        **standard_cache_options
    )
    pop_df = get_pop(pop_run_id=pop_run_id,
                     **standard_cache_options)
    env_df = get_env(env_run_id=env_run_id,
                     **standard_cache_options)
    age_weight_df = get_age_weights(**standard_cache_options)
    cause_meta_df = get_current_cause_hierarchy(
        cause_set_version_id=cause_set_version_id,
        **standard_cache_options)
    age_meta_df = get_ages(**standard_cache_options)

# ************************************************************
# RAKING
# ************************************************************
    # Rake if appropriate based on this logic
    if ((data_type_id in [8, 9, 10] and (source != 'Other_Maternal')) or
            source in MATERNAL_NR_SOURCES):
        if source not in NOT_RAKED_SOURCES:
            print_log_message("Raking sub national estimates")
            raker = Raker(df, source)
            df = raker.get_computed_dataframe(location_hierarchy)

    # for the Other_Maternal source we only rake household surveys
    elif source == "Other_Maternal":
        model_groups = get_datasets(
            nid, extract_type_id, block_rerun=True,
            force_rerun=False
        ).model_group.unique()
        assert len(model_groups) == 1
        model_group = model_groups[0]

        if "HH_SURVEYS" in model_group:
            if model_group == 'MATERNAL-HH_SURVEYS-IND':
                print_log_message("Raking sub national estimates," \
                    " applying double raking for India Maternal"
                )
                raker = Raker(df, source, double=True)
                df = raker.get_computed_dataframe(location_hierarchy)
            else:
                print_log_message("Raking sub national estimates")
                raker = Raker(df, source)
                df = raker.get_computed_dataframe(location_hierarchy)

# ************************************************************
# DROP ZERO SAMPLE SIZE AND RESTRICTED AGE/SEX DATA
# ************************************************************

    # data with zero sample size is almost certaintly some anomolous result
    # of a program generating data it shouldn't have, and it shouldn't be
    # included in codem models. Was probably already dropped, anyway, before
    # running noise reduction.
    df = df.query('sample_size != 0')

    # uploading data before 1980 is a waste of space because neither codem
    # nor codviz use it
    df = df.loc[df['year_id'] >= 1980]

    print_log_message("Enforcing age sex restrictions")
    # this actually drops data from the dataframe if it violates age/sex
    # restrictions (e.g. male maternity disorders)
    df = enforce_asr(df, cause_meta_df, age_meta_df)

# ************************************************************
# FIT EACH DRAW TO NON-ZERO FLOOR
# ************************************************************

    print_log_message("Fitting to non-zero floor...")
    nonzero_floorer = NonZeroFloorer(df)
    df = nonzero_floorer.get_computed_dataframe(pop_df, env_df, cause_meta_df)

# ************************************************************
# AGE AGGREGATION
# ************************************************************

    print_log_message("Creating age standardized and all ages groups")
    age_aggregator = AgeAggregator(df, pop_df, env_df, age_weight_df)
    df = age_aggregator.get_computed_dataframe()

# ************************************************************
# Make CODEm and CoDViz metrics for uncertainty
# ************************************************************
    # columns that should be present in the phase output
    final_cols = [
        'age_group_id', 'cause_id', 'cf_corr', 'cf_final', 'cf_raw', 'cf_rd',
        'extract_type_id', 'location_id', 'nid', 'sample_size',
        'sex_id', 'site_id', 'year_id'
    ]
    # Use draws to make metrics for uncertainty to
    # be used by CODEm and CoDViz
    # also creates cf_final from mean of draws
    print_log_message("Making metrics for CODEm and CoDViz")
    if dataset_has_redistribution_variance(data_type_id, source):
        df = RedistributionVarianceEstimator.make_codem_codviz_metrics(
            df, pop_df)
        final_cols += ['cf_final_high_rd', 'cf_final_low_rd',
                       'variance_rd_log_dr', 'variance_rd_logit_cf']

    # we did this in the old code-- no cfs over 1 nor below 0
    for cf_col in ['cf_final', 'cf_rd', 'cf_raw', 'cf_corr']:
        df.loc[df[cf_col] > 1, cf_col] = 1
        df.loc[df[cf_col] < 0, cf_col] = 0

    df = df[final_cols]

    return df


def main(nid, extract_type_id, launch_set_id):
    """Read the data, run the phase, write the output."""
    print_log_message("Reading {} data".format(PHASE_ANTECEDENT))
    df = get_claude_data(
        PHASE_ANTECEDENT, nid=nid, extract_type_id=extract_type_id
    )

    env_run_id = int(CONF.get_id('env_run'))
    pop_run_id = int(CONF.get_id('pop_run'))
    location_set_version_id = int(CONF.get_id('location_set_version'))
    cause_set_version_id = int(CONF.get_id('cause_set_version'))

    df = df.rename(columns={'cf': 'cf_final'})

    df = run_phase(df, nid, extract_type_id, env_run_id,
                   pop_run_id, location_set_version_id, cause_set_version_id)

    print_log_message(
        "Writing {n} rows of output for launch set {ls}, nid {nid}, extract "
        "{e}".format(n=len(df), ls=launch_set_id, nid=nid, e=extract_type_id)
    )
    write_phase_output(df, PHASE_NAME, nid,
                       extract_type_id, launch_set_id)


if __name__ == "__main__":
    nid = int(sys.argv[1])
    extract_type_id = int(sys.argv[2])
    launch_set_id = int(sys.argv[3])
    main(nid, extract_type_id, launch_set_id)
