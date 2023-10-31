import sys

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
    """
    # create columns for ease of logic interpretation
    df["source"] = source
    df["data_type_id"] = data_type_id
    df["representative_id"] = representative_id

    orig_columns = df.columns

    df = add_nid_ghdx_metadata(df, "ghdx_coverage", **cache_options)

    # develop national condition
    # combine ghdx coverage and our coverage statement
    ghdx_national = df['ghdx_coverage'] == "Country"
    ghdx_subnat = df['ghdx_coverage'] == "Subnational"
    ghdx_empty = df['ghdx_coverage'].isnull()
    we_called_nat = df['representative_id'] == 1
    is_va = df['data_type_id'].isin([8, 12])
    is_vr = df['data_type_id'].isin([9, 10])

    # make this a prevailing condition
    not_scd = df['source'] != "India_SCD_states_rural"

    not_pse = ~(df['location_id'] == 149)
    not_zwe = ~(df['location_id'] == 198)
    not_ngavr = ~(df['nid'] == 43383)
    not_vr_exception = not_pse & not_zwe & not_ngavr

    va_national_condition = ((ghdx_national & we_called_nat) | (
        we_called_nat & ghdx_empty)) & not_scd & is_va
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

    # get garbage levels and merge them onto the df
    print_log_message("Pulling map from cause to detail level")
    detail_level_map = get_map_to_package_metadata(code_system_id,
        force_rerun=False, block_rerun=True
    )

    # merge incoming data with cause detail level df
    print("Merging detail level onto data")
    df = merge_with_detail_map(df, detail_level_map)

    print_log_message("Determining national coverage")
    # get national reprsentativeness
    df = assign_nationally_representative(
        df, source, representative_id, data_type_id, cache_options
    )

    print_log_message("Collapsing data.")
    df = df.groupby(["location_id", "year_id", "nid", "extract_type_id",
                     "source", "data_type_id", "detail_level_id",
                     "nationally_representative", "age_group_id",
                     "sex_id"], as_index=False)["deaths"].sum()

    return df


def main(nid, extract_type_id, project_id, code_system_id, launch_set_id):
    """Collect source metadata."""
    print_log_message("Reading disaggregation data for source metadata phase.")
    df = get_claude_data("disaggregation", nid=nid,
                          extract_type_id=extract_type_id, project_id=project_id)

    data_type_id = get_value_from_nid(nid, "data_type_id",
        extract_type_id=extract_type_id, project_id=project_id)

    source = get_value_from_nid(nid, "source",
        extract_type_id=extract_type_id, project_id=project_id)

    representative_id = get_value_from_nid(nid, "representative_id",
        extract_type_id=extract_type_id, project_id=project_id)

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
    project_id = int(sys.argv[3])
    code_system_id = int(sys.argv[4])
    launch_set_id = int(sys.argv[5])
    main(nid, extract_type_id, project_id, code_system_id, launch_set_id)
