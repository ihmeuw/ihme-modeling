import sys

import numpy as np
import pandas as pd

from cod_prep.claude.claude_io import (
    get_input_launch_set_id,
    get_input_launch_set_ids,
    get_phase_output,
    write_phase_output,
)
from cod_prep.claude.configurator import Configurator
from cod_prep.claude.count_adjustments import InjuryRedistributor, LRIRedistributor
from cod_prep.claude.hiv_correction import HIVCorrector
from cod_prep.claude.mapping import BridgeMapper
from cod_prep.claude.recode import Recoder
from cod_prep.claude.rescale import ChinaHospitalUrbanicityRescaler
from cod_prep.downloaders import (
    get_ages,
    get_cod_ages,
    get_code_system_from_id,
    get_current_cause_hierarchy,
    get_current_location_hierarchy,
    get_pop,
    get_values_from_nid,
)
from cod_prep.utils import CodSchema

CONF = Configurator("standard")


def skip_hiv_correction(source):
    source_list_path = CONF.get_resource("hiv_correction_skip_sources")
    sources_to_skip = pd.read_csv(source_list_path)
    return source in list(sources_to_skip["VR_list"].unique())


def needs_subnational_rescale(source, nid):
    scaling_sources = source in ["China_2004_2012", "China_DSP_prov_ICD10", "China_1991_2002"]
    dont_exclude = nid not in [469163, 469165, 528380, 528381]
    return scaling_sources and dont_exclude


def needs_strata_collapse(source):
    return source in [
        "China_MMS",
        "China_Child",
    ]


def needs_rti_adjustment(source):
    return False


def needs_injury_redistribution(source):
    return source == "South_Africa_by_province"


def needs_disjoint_crosswalking(iso3):
    countries = (
        pd.read_excel(CONF.get_resource("cod_disjoint_crosswalk_country_causes"))
        .iso3.unique()
        .tolist()
    )
    return iso3 in countries


def combine_with_prev_phases(df, nid, extract_type_id, launch_set_id):
    schema = CodSchema.infer_from_data(df)
    merge_cols = schema.id_cols
    val_cols = schema.value_cols

    disagg_lsid, msdc_lsid, rd_lsid, cov_lsid = get_input_launch_set_ids(
        nid,
        extract_type_id,
        launch_set_id,
        ["disaggregation", "misdiagnosiscorrection", "redistribution", "covidcorrection"],
    )
    raw_df = get_phase_output(
        "disaggregation", nid=nid, extract_type_id=extract_type_id, launch_set_id=disagg_lsid
    )
    raw_df = raw_df.groupby(merge_cols, as_index=False)[val_cols].sum()
    raw_df = raw_df.rename(columns={"deaths": "deaths_raw"})

    corr_df = get_phase_output(
        "misdiagnosiscorrection",
        nid=nid,
        extract_type_id=extract_type_id,
        launch_set_id=msdc_lsid,
    )
    corr_df = corr_df.groupby(merge_cols, as_index=False)[val_cols].sum()
    corr_df = corr_df.rename(columns={"deaths": "deaths_corr"})

    rd_df = get_phase_output(
        "redistribution", nid=nid, extract_type_id=extract_type_id, launch_set_id=rd_lsid
    )
    rd_df = rd_df.groupby(merge_cols, as_index=False)[val_cols].sum()
    rd_df = rd_df.rename(columns={"deaths": "deaths_rd"})

    cov_df = get_phase_output(
        "covidcorrection", nid=nid, extract_type_id=extract_type_id, launch_set_id=cov_lsid
    )
    cov_df = cov_df.groupby(merge_cols, as_index=False)[val_cols].sum()
    cov_df = cov_df.rename(columns={"deaths": "deaths_cov"})

    df = df.groupby(merge_cols, as_index=False)[val_cols].sum()
    df = df.merge(cov_df, how='left', on=merge_cols)
    df = df.merge(rd_df, how="left", on=merge_cols)
    df = df.merge(corr_df, how="left", on=merge_cols)
    df = df.merge(raw_df, how="left", on=merge_cols)

    for val_col in ["deaths_rd", "deaths_corr", "deaths_cov", "deaths_raw"]:
        df[val_col] = df[val_col].fillna(0)

    return df


def apply_cw_results(df):
    cw = pd.read_csv(CONF.get_resource("cod_disjoint_crosswalk"))

    schema = CodSchema.infer_from_data(df)
    assert len(schema.value_cols) == 1
    assert all(col in cw.columns for col in schema.id_cols)
    value_col = schema.value_cols[0]
    start_deaths = df.groupby(schema.demo_cols, as_index=False)[value_col].sum()
    df = df.merge(
        cw.loc[:, schema.id_cols + ["change_in_deaths"]],
        on=schema.id_cols,
        how="left",
        validate="one_to_one",
    )

    df["change_in_deaths"].fillna(0, inplace=True)
    df["deaths"] = df["deaths"] + df["change_in_deaths"]
    df.drop(columns=["change_in_deaths"], inplace=True)
    assert (df["deaths"] >= 0).values.all()

    deaths_diff = pd.merge(
        start_deaths,
        df.groupby(schema.demo_cols, as_index=False)[value_col].sum(),
        on=schema.demo_cols,
        how="outer",
        suffixes=("_start", "_end"),
    )
    assert np.allclose(deaths_diff.eval("deaths_start - deaths_end"), 0)

    return df


def run_phase(df: pd.DataFrame, nid: int, extract_type_id: int, launch_set_id: int) -> pd.DataFrame:
    cache_kwargs = {
        "force_rerun": False,
        "block_rerun": True,
        "cache_results": False,
        "cache_dir": CONF.get_directory("db_cache"),
        "verbose": False,
    }
    project_id = CONF.get_id("project")
    location_set_version_id = CONF.get_id("location_set_version")
    cause_set_version_id = CONF.get_id("cause_set_version")
    pop_run_id = CONF.get_id("pop_run")
    code_system_id, source, data_type_id, iso3 = get_values_from_nid(
        nid,
        extract_type_id,
        values=["code_system_id", "source", "data_type_id", "iso3"],
        **cache_kwargs,
    )
    code_system = get_code_system_from_id(code_system_id, **cache_kwargs)
    loc_meta_df = get_current_location_hierarchy(
        location_set_version_id=location_set_version_id, **cache_kwargs
    )
    cause_meta_df = get_current_cause_hierarchy(
        cause_set_version_id=cause_set_version_id,
        **cache_kwargs,
    )
    is_vr = data_type_id in [9, 10]

    orig_deaths = df["deaths"].sum()

    if needs_disjoint_crosswalking(iso3):
        df = apply_cw_results(df)

    if not skip_hiv_correction(source) and is_vr:
        pop_meta_df = get_pop(pop_run_id=pop_run_id, **cache_kwargs)
        age_meta_df = get_ages(**cache_kwargs)
        assert pd.notnull(
            iso3
        )

        hiv_corrector = HIVCorrector(
            df,
            iso3,
            code_system_id,
            pop_meta_df,
            cause_meta_df,
            loc_meta_df,
            age_meta_df,
            correct_garbage=False,
        )
        df = hiv_corrector.get_computed_dataframe()

    if needs_injury_redistribution(source):
        injury_redistributor = InjuryRedistributor(df, loc_meta_df, cause_meta_df)
        df = injury_redistributor.get_computed_dataframe()

    cod_age_meta_df = get_cod_ages(**cache_kwargs)
    lri_tb_redistributor = LRIRedistributor(df, cause_meta_df, cod_age_meta_df)
    df = lri_tb_redistributor.get_computed_dataframe()

    df = combine_with_prev_phases(df, nid, extract_type_id, launch_set_id)

    val_cols = ["deaths", "deaths_raw", "deaths_corr", "deaths_rd", "deaths_cov"]

    if needs_subnational_rescale(source, nid):
        china_rescaler = ChinaHospitalUrbanicityRescaler()
        df = china_rescaler.get_computed_dataframe(df)

    if needs_strata_collapse(source):
        df["site_id"] = 2
        group_cols = list(set(df.columns) - set(val_cols))
        df = df.groupby(group_cols, as_index=False)[val_cols].sum()

    if is_vr:
        df = df.loc[df[val_cols].sum(axis=1) != 0]

    bridge_mapper = BridgeMapper(source, cause_meta_df, code_system, project_id)
    df = bridge_mapper.get_computed_dataframe(df)

    expert_opinion_recoder = Recoder(
        cause_meta_df, loc_meta_df, source, code_system_id, data_type_id
    )
    df = expert_opinion_recoder.get_computed_dataframe(df)

    end_deaths = df["deaths"].sum()

    if abs(orig_deaths - end_deaths) >= (0.1 * end_deaths):
        diff = round(abs(orig_deaths - end_deaths), 2)
        old = round(abs(orig_deaths))
        new = round(abs(end_deaths))
        raise AssertionError(f"Change of {diff} deaths [{old}] to [{new}]")

    return df


def main(nid: int, extract_type_id: int, launch_set_id: int) -> pd.DataFrame:
    df = get_phase_output(
        "covidcorrection",
        nid,
        extract_type_id,
        get_input_launch_set_id(nid, extract_type_id, launch_set_id, "covidcorrection"),
    )
    df = run_phase(df, nid, extract_type_id, launch_set_id)
    write_phase_output(df, "corrections", nid, extract_type_id, launch_set_id)
    return df


if __name__ == "__main__":
    main(
        nid=int(sys.argv[1]),
        extract_type_id=int(sys.argv[2]),
        launch_set_id=int(sys.argv[3]),
    )
