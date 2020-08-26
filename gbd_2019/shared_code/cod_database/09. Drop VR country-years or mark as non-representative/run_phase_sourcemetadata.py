"""Compile relevant source metadata for pre redisribution CoD data.

uses include:
deaths by garbage type/level

"""
import sys
import os
import pandas as pd
from cod_prep.claude.configurator import Configurator
from cod_prep.utils import print_log_message, report_if_merge_fail
from cod_prep.claude.claude_io import write_phase_output, get_phase_output
from cod_prep.downloaders import (
    get_value_from_nid, add_nid_ghdx_metadata,
    get_map_to_package_metadata
)
pd.options.mode.chained_assignment = None


def merge_with_detail_map(df, detail_map):
    """Merge incoming data with the detail map."""
    assert 'detail_level' not in df.columns
    detail_map = detail_map[["code_id", "detail_level_id"]]
    df = df.merge(detail_map, on=['code_id'], how='left')
    report_if_merge_fail(df, 'detail_level_id', 'code_id')
    return df


def assign_nationally_representative(df, source, representative_id,
                                     data_type_id, cache_options):
    """Assign indicator of national representativeness.

    For VA, the condition is:
        GHDx calls it national (coverage = "Country") & We didn't make
        source_type something like "VA Subnational"
        OR
        GHDx has no information & We called source_type something
        like "VA National"

        and SCD cannot be called national, per USERNAME.

    For VR, the condition is looser:
        We set the national variable to 1

        OR, if national = 1:

        We didnt mark it as "VR Subnational" AND GHDx coverage
        does not mark coverage as "Subnational"
    """
    # create columns for ease of logic interpretation
    df["source"] = source
    df["data_type_id"] = data_type_id
    df["representative_id"] = representative_id

    orig_columns = df.columns

    # add ghdx_coverage
    df = add_nid_ghdx_metadata(df, "ghdx_coverage", **cache_options)

    # develop national condition
    # combine ghdx coverage and our coverage statement
    ghdx_national = df['ghdx_coverage'] == "Country"
    ghdx_subnat = df['ghdx_coverage'] == "Subnational"
    ghdx_empty = df['ghdx_coverage'].isnull()
    we_called_nat = df['representative_id'] == 1
    # treat CHAMPS as "VA" for purposes of determining representativeness
    is_va = df['data_type_id'].isin([8, 12])
    is_vr = df['data_type_id'].isin([9, 10])

    # make this a prevailing condition because it will have huge impact and
    # USERNAME directly said that SCD should not be called country coverage
    # because it is only rural.
    not_scd = df['source'] != "India_SCD_states_rural"

    # VR exceptions for places where completeness would be higher if we
    # had the right envelope (e.g. VR is only from certain place)
    # All this Palestine VR is subnational
    not_pse = ~(df['location_id'] == 149)
    # Zimbabwe VR is subnational (in the event Zimbabwe gets a full VR
    # system this exception should be removed)
    not_zwe = ~(df['location_id'] == 198)
    # Nigeria_VR should not be dropped either, but make it subnational
    not_ngavr = ~(df['nid'] == 43383)
    not_vr_exception = not_pse & not_zwe & not_ngavr

    # Final condition to call a VA study national
    va_national_condition = ((ghdx_national & we_called_nat) | (
        we_called_nat & ghdx_empty)) & not_scd & is_va
    # VR condition is less conservative
    vr_national_condition = (
        is_vr &
        (we_called_nat | (~ghdx_subnat)) &
        not_vr_exception
    )

    # final national condition combines the two
    is_national = va_national_condition | vr_national_condition

    # double check india SRS & indonesia SRS counts
    srs_sources = ["India_SRS_states_report", "Indonesia_SRS_province",
                   "Indonesia_SRS_2014"]
    is_srs = df['source'].isin(srs_sources)
    assert len(df[is_srs & ~is_national]) == 0

    # assign indicator
    df['nationally_representative'] = 1 * is_national

    final_columns = list(orig_columns)
    final_columns.append('nationally_representative')
    df = df[final_columns]
    return df


def run_phase(df, nid, extract_type_id, data_type_id, source,
              representative_id, code_system_id):
    """Prep source data by location, year, age, sex, and garbage level."""
    # set caching
    configurator = Configurator('standard')
    cache_dir = configurator.get_directory('db_cache')
    cache_options = {
        'block_rerun': True,
        'cache_dir': cache_dir,
        'force_rerun': False,
        'cache_results': False
    }

    print_log_message("Pulling map from cause to detail level")
    detail_level_map = get_map_to_package_metadata(code_system_id)

    # merge incoming data with cause detail level df
    print("Merging detail level onto data")
    df = merge_with_detail_map(df, detail_level_map)

    print_log_message("Determining national coverage")
    # get national reprsentativeness
    # in stata we used whether or not there was national
    # or subnational in the source_type
    # but since we no longer have that then the best we can
    # do here is just use "representative_id"
    df = assign_nationally_representative(
        df, source, representative_id, data_type_id, cache_options
    )

    print_log_message("Collapsing data.")
    df = df.groupby(["location_id", "year_id", "nid", "extract_type_id",
                     "source", "data_type_id", "detail_level_id",
                     "nationally_representative", "age_group_id",
                     "sex_id"], as_index=False)["deaths"].sum()

    return df


def main(nid, extract_type_id, code_system_id, launch_set_id):
    """Collect source metadata."""
    print_log_message("Reading disaggregation data for source metadata phase.")
    # in stata, this was pulled using corrections phase output (step 3)
    # use data after all garbage codes have been set (step 3 produces ZZZ)
    # but before any redistribution (including HIV redistribution)
    df = get_phase_output("disaggregation", nid=nid,
                          extract_type_id=extract_type_id)

    data_type_id = get_value_from_nid(nid, "data_type_id", extract_type_id)

    source = get_value_from_nid(nid, "source", extract_type_id)

    representative_id = get_value_from_nid(nid, "representative_id",
                                           extract_type_id)

    df = run_phase(df, nid, extract_type_id, data_type_id, source,
                   representative_id, code_system_id)

    print_log_message(
        "Writing {n} rows of output for launch set {ls}, nid {nid}, extract "
        "{e}".format(n=len(df), ls=launch_set_id, e=extract_type_id, nid=nid)
    )
    write_phase_output(df, 'sourcemetadata', nid,
                       extract_type_id, launch_set_id)


if __name__ == "__main__":
    nid = int(sys.argv[1])
    extract_type_id = int(sys.argv[2])
    code_system_id = int(sys.argv[3])
    launch_set_id = int(sys.argv[4])
    main(nid, extract_type_id, code_system_id, launch_set_id)
