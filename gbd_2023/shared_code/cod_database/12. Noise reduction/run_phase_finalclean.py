import sys

import pandas as pd

from cod_prep.claude.aggregators import AgeAggregator
from cod_prep.claude.cf_adjustments import Raker
from cod_prep.claude.claude_io import get_input_launch_set_id, get_phase_output, write_phase_output
from cod_prep.claude.configurator import Configurator
from cod_prep.claude.rate_adjustments import NonZeroFloorer
from cod_prep.claude.recode import FinalRecoder
from cod_prep.claude.redistribution_variance import (
    RedistributionVarianceEstimator,
    dataset_has_redistribution_variance,
)
from cod_prep.downloaders import (
    get_age_weights,
    get_ages,
    get_current_cause_hierarchy,
    get_current_location_hierarchy,
    get_env,
    get_pop,
    get_values_from_nid,
)
from cod_prep.utils import drop_unmodeled_asc

CONF = Configurator("standard")
TRACK_FLOORING = False
NOT_RAKED_SOURCES = [
    "Maternal_report",
    "SUSENAS",
    "China_MMS",
    "China_Child",
]
MATERNAL_NR_SOURCES = [
    "Mexico_BIRMM",
    "Maternal_report",
    "SUSENAS",
    "China_MMS",
    "China_Child",
]


def run_phase(df: pd.DataFrame, nid: int, extract_type_id: int) -> pd.DataFrame:
    cache_kwargs = {
        "force_rerun": False,
        "block_rerun": True,
        "cache_results": False,
        "cache_dir": CONF.get_directory("db_cache"),
        "verbose": False,
    }
    location_set_version_id = CONF.get_id("location_set_version")
    cause_set_version_id = CONF.get_id("cause_set_version")
    code_system_id, iso3, data_type_id, source, model_group = get_values_from_nid(
        nid,
        extract_type_id,
        values=["code_system_id", "iso3", "data_type_id", "source", "model_group"],
        **cache_kwargs,
    )
    location_hierarchy = get_current_location_hierarchy(
        location_set_version_id=location_set_version_id, **cache_kwargs
    )
    age_meta_df = get_ages(**cache_kwargs)
    cause_meta_df = get_current_cause_hierarchy(
        cause_set_version_id=cause_set_version_id,
        **cache_kwargs,
    )
    env_df = get_env(env_run_id=CONF.get_id("env_run"), **cache_kwargs)
    pop_df = get_pop(pop_run_id=CONF.get_id("pop_run"), **cache_kwargs)
    age_weight_df = get_age_weights(**cache_kwargs)

    df = df.rename(columns={"cf": "cf_final"})
    if (
        data_type_id in [8, 9, 10] and (source != "Other_Maternal")
    ) or source in MATERNAL_NR_SOURCES:
        if source not in NOT_RAKED_SOURCES:
            raker = Raker(df, source)
            df = raker.get_computed_dataframe(location_hierarchy)

    elif source in ["Other_Maternal", "DHS_maternal"]:
        assert isinstance(
            model_group, str
        )
        if "HH_SURVEYS" in model_group:
            if model_group == "MATERNAL-HH_SURVEYS-IND":
                raker = Raker(df, source, double=True)
                df = raker.get_computed_dataframe(location_hierarchy)
            else:
                raker = Raker(df, source)
                df = raker.get_computed_dataframe(location_hierarchy)
    df = df.query("sample_size != 0")

    df = df.loc[df["year_id"] >= 1980]

    df = drop_unmodeled_asc(df, cause_meta_df, age_meta_df)
    nonzero_floorer = NonZeroFloorer(df, track_flooring=TRACK_FLOORING)
    df = nonzero_floorer.get_computed_dataframe(pop_df, env_df, cause_meta_df, data_type_id)

    final_recoder = FinalRecoder(
        cause_meta_df, location_hierarchy, source, code_system_id, data_type_id
    )
    df = final_recoder.get_computed_dataframe(df)

    age_aggregator = AgeAggregator(df, pop_df, env_df, age_weight_df)
    df = age_aggregator.get_computed_dataframe()
    final_cols = [
        "age_group_id",
        "cause_id",
        "cf_corr",
        "cf_final",
        "cf_raw",
        "cf_cov",
        "cf_rd",
        "cf_agg",
        "extract_type_id",
        "location_id",
        "nid",
        "sample_size",
        "sex_id",
        "site_id",
        "year_id",
    ]
    if TRACK_FLOORING:
        final_cols += ["cf_final_pre_floor", "floor_flag"]
    if dataset_has_redistribution_variance(data_type_id, source):
        df = RedistributionVarianceEstimator.make_codem_codviz_metrics(df, pop_df)
        final_cols += [
            "cf_final_high_rd",
            "cf_final_low_rd",
            "variance_rd_log_dr",
            "variance_rd_logit_cf",
            "variance_rd_cf",
        ]

    for cf_col in ["cf_final", "cf_agg", "cf_rd", "cf_cov", "cf_raw", "cf_corr"]:
        df.loc[df[cf_col] > 1, cf_col] = 1
        df.loc[df[cf_col] < 0, cf_col] = 0

    df = df[final_cols]

    return df


def main(nid: int, extract_type_id: int, launch_set_id: int) -> pd.DataFrame:
    if TRACK_FLOORING:
        assert launch_set_id == 0
    df = get_phase_output(
        "noisereduction",
        nid,
        extract_type_id,
        get_input_launch_set_id(nid, extract_type_id, launch_set_id, "noisereduction"),
    )
    df = run_phase(df, nid, extract_type_id)
    write_phase_output(df, "finalclean", nid, extract_type_id, launch_set_id)
    return df


if __name__ == "__main__":
    main(
        nid=int(sys.argv[1]),
        extract_type_id=int(sys.argv[2]),
        launch_set_id=int(sys.argv[3]),
    )
