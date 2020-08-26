"""This phase includes various computational elements that change deaths data.

Including: HIV correction, sub national scaling, dropping data,
implementing bridge map, recoding causes, etc.
"""

import sys
import os
import pandas as pd
from cod_prep.downloaders import (
    get_current_cause_hierarchy,
    get_current_location_hierarchy,
    get_pop,
    get_value_from_nid,
    get_ages,
    get_code_system_from_id
)
from cod_prep.utils import print_log_message
from cod_prep.claude.configurator import Configurator
from cod_prep.claude.hiv_correction import HIVCorrector
from cod_prep.claude.count_adjustments import InjuryRedistributor, LRIRedistributor
from recode import Recoder
from mapping import BridgeMapper
from cod_prep.claude.claude_io import (
    write_phase_output,
    get_phase_output
)
from rescale import ChinaHospitalUrbanicityRescaler


"""

take source as an argument

run hiv correction for post redistribution results

optionally run subnational scaling

drop bad data

run recodes

run bridge map (maybe move to post-redistribution)

run count-based adjustments:
    rti adjustment

upload to corrections database

"""

CONF = Configurator('standard')


def skip_hiv_correction(source):
    """Determine if you want to skip HIV correction for a source"""
    source_list_path = CONF.get_resource('hiv_correction_skip_sources')
    sources_to_skip = pd.read_csv(source_list_path)
    return source in list(sources_to_skip['VR_list'].unique())


def needs_subnational_rescale(source):
    """Determine if China subnational scaling is necessary."""
    return source in [
        'China_2004_2012', 'China_DSP_prov_ICD10', 'China_1991_2002'
    ]


def needs_strata_collapse(source):
    """Determine if strata level detail should be collapsed to province."""
    return source in [
       "China_MMS", "China_Child",
    ]


def needs_rti_adjustment(source):
    """Determine if RTI adjustment necessary."""
    return False


def needs_injury_redistribution(source):
    """Apply injury proportions to South Africa data."""
    return source == "South_Africa_by_province"


def combine_with_rd_raw(df, nid, extract_type_id, location_set_version_id):
    merge_cols = [
        'nid', 'extract_type_id', 'location_id', 'year_id',
        'age_group_id', 'sex_id', 'cause_id', 'site_id'
    ]
    val_cols = ['deaths']

    raw_df = get_phase_output(
        "disaggregation", nid=nid, extract_type_id=extract_type_id
    )
    raw_df = raw_df.groupby(merge_cols, as_index=False)[val_cols].sum()
    raw_df = raw_df.rename(columns={'deaths': 'deaths_raw'})

    corr_df = get_phase_output(
        "misdiagnosiscorrection", nid=nid, extract_type_id=extract_type_id
    )
    corr_df = corr_df.groupby(merge_cols, as_index=False)[val_cols].sum()
    corr_df = corr_df.rename(columns={'deaths': 'deaths_corr'})

    rd_df = get_phase_output(
        "redistribution", nid=nid, extract_type_id=extract_type_id
    )
    rd_df = rd_df.groupby(merge_cols, as_index=False)[val_cols].sum()
    rd_df = rd_df.rename(columns={'deaths': 'deaths_rd'})

    df = df.groupby(merge_cols, as_index=False)[val_cols].sum()
    df = df.merge(rd_df, how='left', on=merge_cols)
    df = df.merge(corr_df, how='left', on=merge_cols)
    df = df.merge(raw_df, how='left', on=merge_cols)

    # null values can occur where rows were created in redistribution
    for val_col in ['deaths_rd', 'deaths_corr', 'deaths_raw']:
        df[val_col] = df[val_col].fillna(0)

    return df


def run_phase(df, nid, extract_type_id, pop_run_id,
              cause_set_version_id, location_set_version_id):
    """Run the full phase, chaining together computational elements."""
    # get filepaths
    cache_dir = CONF.get_directory('db_cache')

    orig_deaths = df['deaths'].sum()

    standard_cache_options = {
        'force_rerun': False,
        'block_rerun': True,
        'cache_dir': cache_dir,
        'cache_results': False
    }

    code_system_id = get_value_from_nid(nid, 'code_system_id',
                                        extract_type_id=extract_type_id)

    code_system = get_code_system_from_id(code_system_id, **standard_cache_options)

    source = get_value_from_nid(nid, 'source',
                                extract_type_id=extract_type_id)

    data_type_id = get_value_from_nid(
        nid, 'data_type_id', extract_type_id=extract_type_id,
        location_set_version_id=location_set_version_id
    )

    # get cause hierarchy
    cause_meta_df = get_current_cause_hierarchy(
        cause_set_version_id=cause_set_version_id,
        **standard_cache_options
    )

    is_vr = data_type_id in [9, 10]

    # run hiv correction on VR, but not Other_Maternal
    # countries to correct will be further pruned by the master cause
    # selections csv in the hiv corrector class
    if not skip_hiv_correction(source) and is_vr:

        # get location hierarchy
        loc_meta_df = get_current_location_hierarchy(
            location_set_version_id=location_set_version_id,
            **standard_cache_options
        )

        # get population
        pop_meta_df = get_pop(
            pop_run_id=pop_run_id,
            **standard_cache_options
        )

        # get age metadata
        age_meta_df = get_ages(**standard_cache_options)

        # get the country
        iso3 = get_value_from_nid(
            nid, 'iso3', extract_type_id=extract_type_id,
            location_set_version_id=location_set_version_id
        )
        assert pd.notnull(iso3), "Could not find iso3 for nid {}, " \
            "extract_type_id {}".format(nid, extract_type_id)

        hiv_corrector = HIVCorrector(df,
                                     iso3,
                                     code_system_id,
                                     pop_meta_df,
                                     cause_meta_df,
                                     loc_meta_df,
                                     age_meta_df,
                                     correct_garbage=False)
        print_log_message("Running hiv correction for iso3 {}".format(iso3))
        df = hiv_corrector.get_computed_dataframe()

    if needs_injury_redistribution(source):
        print_log_message("Correcting injuries")
        if not 'loc_meta_df'in vars():
            # get location hierarchy
            loc_meta_df = get_current_location_hierarchy(
                location_set_version_id=location_set_version_id,
                **standard_cache_options
            )
        injury_redistributor = InjuryRedistributor(df, loc_meta_df, cause_meta_df)
        df = injury_redistributor.get_computed_dataframe()

    # apply redistribution of LRI to tb in under 15, non-neonatal ages based
    # on location/year specific proportions
    print_log_message("Applying special redistribution of LRI to TB in under 15")
    lri_tb_redistributor = LRIRedistributor(df, cause_meta_df)
    df = lri_tb_redistributor.get_computed_dataframe()

    # merge in raw and rd here because recodes and bridge mapping should
    # also apply to the causes that are in previous phases (raw deaths for
    # secret codes need to be moved up to their parent cause, for example)
    df = combine_with_rd_raw(df, nid, extract_type_id, location_set_version_id)

    val_cols = ['deaths', 'deaths_raw', 'deaths_corr', 'deaths_rd']

    # run china VR rescaling
    if needs_subnational_rescale(source):
        china_rescaler = ChinaHospitalUrbanicityRescaler()
        df = china_rescaler.get_computed_dataframe(df)

    if needs_strata_collapse(source):
        # set site id to blank site id and collapse
        df['site_id'] = 2
        group_cols = list(set(df.columns) - set(val_cols))
        df = df.groupby(group_cols, as_index=False)[val_cols].sum()

    if is_vr:
        # drop if deaths are 0 across all current deaths columns
        df = df.loc[df[val_cols].sum(axis=1) != 0]

    # restrict causes based on code system
    print_log_message("Running bridge mapper")
    bridge_mapper = BridgeMapper(source, cause_meta_df, code_system)
    df = bridge_mapper.get_computed_dataframe(df)

    # run recodes based on expert opinion
    print_log_message("Enforcing some very hard priors (expert opinion)")
    expert_opinion_recoder = Recoder(cause_meta_df, source,
                                     code_system_id, data_type_id)
    df = expert_opinion_recoder.get_computed_dataframe(df)

    end_deaths = df['deaths'].sum()

    print_log_message("Checking no large loss or gain of deaths")
    if abs(orig_deaths - end_deaths) >= (.1 * end_deaths):
        diff = round(abs(orig_deaths - end_deaths), 2)
        old = round(abs(orig_deaths))
        new = round(abs(end_deaths))
        raise AssertionError(
            "Change of {} deaths [{}] to [{}]".format(diff, old, new)
        )

    return df


def main(nid, extract_type_id, launch_set_id):
    """Read the data, run the phase, write the output."""
    print_log_message("Reading redistribution data..")
    df = get_phase_output('redistribution', nid=nid,
                          extract_type_id=extract_type_id)

    cause_set_version_id = int(CONF.get_id('cause_set_version'))
    pop_run_id = int(CONF.get_id('pop_run'))
    location_set_version_id = int(CONF.get_id('location_set_version'))

    # run the phase
    df = run_phase(df, nid, extract_type_id, pop_run_id,
                   cause_set_version_id, location_set_version_id)

    # upload to database
    print_log_message(
        "Writing {n} rows of output for launch set {ls}, nid {nid}, extract "
        "{e}".format(n=len(df), ls=launch_set_id, nid=nid, e=extract_type_id)
    )
    write_phase_output(df, 'corrections', nid,
                       extract_type_id, launch_set_id)


if __name__ == "__main__":
    nid = int(sys.argv[1])
    extract_type_id = int(sys.argv[2])
    launch_set_id = int(sys.argv[3])
    main(nid, extract_type_id, launch_set_id)
