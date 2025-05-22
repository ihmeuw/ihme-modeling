import sys

import pandas as pd

from cod_prep.claude.claude_io import get_input_launch_set_id, get_phase_output, write_phase_output
from cod_prep.claude.configurator import Configurator
from cod_prep.downloaders.engine_room import add_code_metadata, get_cause_map, get_map_version
from cod_prep.downloaders.garbage_levels import get_map_to_package_metadata
from cod_prep.downloaders.nids import add_nid_ghdx_metadata, get_values_from_nid
from cod_prep.utils import report_if_merge_fail

pd.options.mode.chained_assignment = None

CONF = Configurator()


def merge_with_detail_map(df, detail_map, code_system_id):
    assert "detail_level" not in df.columns
    detail_map = detail_map[["code_id", "detail_level_id"]]
    df = df.merge(detail_map, on=["code_id"], how="left")
    if df.detail_level_id.isnull().any() and code_system_id == 1:
        cm = get_cause_map(code_system_id, force_rerun=False, block_rerun=True)
        df = add_code_metadata(df, "value", code_map=cm)
        df.loc[df["value"].str.len() >= 6, "value"] = df["value"].str.slice(0, 5)
        df.drop("code_id", axis=1, inplace=True)
        df = add_code_metadata(df, "code_id", code_map=cm, merge_col="value")
        df = df.merge(detail_map, on="code_id", how="left", suffixes=("", "_retry4dig"))
        df.loc[df.detail_level_id.isnull(), "detail_level_id"] = df["detail_level_id_retry4dig"]
        df.drop("detail_level_id_retry4dig", axis=1, inplace=True)
    report_if_merge_fail(df, "detail_level_id", "code_id")
    return df


def assign_nationally_representative(df, source, representative_id, data_type_id, cache_options):
    df["source"] = source
    df["data_type_id"] = data_type_id
    df["representative_id"] = representative_id

    orig_columns = df.columns

    df = add_nid_ghdx_metadata(df, "ghdx_coverage", **cache_options)

    ghdx_national = df["ghdx_coverage"] == "Country"
    ghdx_subnat = df["ghdx_coverage"] == "Subnational"
    ghdx_empty = df["ghdx_coverage"].isnull()
    we_called_nat = df["representative_id"] == 1
    is_va = df["data_type_id"].isin([8, 12])
    is_vr = df["data_type_id"].isin([9, 10])

    not_scd = df["source"] != "India_SCD_states_rural"

    not_pse = ~(df["location_id"] == 149)
    not_zwe = ~(df["location_id"] == 198)
    not_ngavr = ~(df["nid"] == 43383)
    not_vr_exception = not_pse & not_zwe & not_ngavr

    va_national_condition = (
        ((ghdx_national & we_called_nat) | (we_called_nat & ghdx_empty)) & not_scd & is_va
    )
    vr_national_condition = is_vr & (we_called_nat | (~ghdx_subnat)) & not_vr_exception

    is_national = va_national_condition | vr_national_condition

    srs_sources = ["India_SRS_states_report", "Indonesia_SRS_province", "Indonesia_SRS_2014"]
    is_srs = df["source"].isin(srs_sources)
    assert len(df[is_srs & ~is_national]) == 0

    df["nationally_representative"] = 1 * is_national

    final_columns = list(orig_columns)
    final_columns.append("nationally_representative")
    df = df[final_columns]
    return df


def run_phase(df: pd.DataFrame, nid: int, extract_type_id: int) -> pd.DataFrame:
    cache_kwargs = {
        "force_rerun": False,
        "block_rerun": True,
        "cache_results": False,
        "cache_dir": CONF.get_directory("db_cache"),
        "verbose": False,
    }
    code_system_id, data_type_id, source, representative_id = get_values_from_nid(
        nid,
        extract_type_id,
        values=["code_system_id", "data_type_id", "source", "representative_id"],
        **cache_kwargs,
    )

    code_map_version_id = get_map_version(code_system_id, **cache_kwargs)
    detail_level_map = get_map_to_package_metadata(
        code_system_id, code_map_version_id, force_rerun=False, block_rerun=True
    )

    df = merge_with_detail_map(df, detail_level_map, code_system_id)

    df = assign_nationally_representative(df, source, representative_id, data_type_id, cache_kwargs)

    df = df.groupby(
        [
            "location_id",
            "year_id",
            "nid",
            "extract_type_id",
            "source",
            "data_type_id",
            "detail_level_id",
            "nationally_representative",
            "age_group_id",
            "sex_id",
        ],
        as_index=False,
    )["deaths"].sum()

    return df


def main(nid: int, extract_type_id: int, launch_set_id: int) -> pd.DataFrame:
    df = get_phase_output(
        "disaggregation",
        nid,
        extract_type_id,
        get_input_launch_set_id(nid, extract_type_id, launch_set_id, "disaggregation"),
    )
    df = run_phase(df, nid, extract_type_id)

    write_phase_output(df, "sourcemetadata", nid, extract_type_id, launch_set_id)
    return df


if __name__ == "__main__":
    main(
        nid=int(sys.argv[1]),
        extract_type_id=int(sys.argv[2]),
        launch_set_id=int(sys.argv[3]),
    )
