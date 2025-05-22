import sys

import pandas as pd

from cod_prep.claude.claude_io import get_input_launch_set_id, get_phase_output, write_phase_output
from cod_prep.claude.configurator import Configurator
from cod_prep.claude.noise_reduction import NoiseReducer
from cod_prep.claude.run_phase_nrmodel import get_model_data_path, model_group_is_run_by_cause
from cod_prep.downloaders import get_values_from_nid
from cod_prep.utils import (
    just_keep_trying,
    read_df,
    report_duplicates,
    report_if_merge_fail,
    wrap,
)
from cod_prep.utils.nr_helpers import get_fallback_model_group

pd.options.mode.chained_assignment = None
CONF = Configurator("standard")
MATERNAL_NR_SOURCES = [
    "Mexico_BIRMM",
    "Maternal_report",
    "SUSENAS",
    "China_MMS",
    "China_Child",
]

NR_DIR = CONF.get_directory("nr_process_data")


def get_model_result_data(model_group, launch_set_id, cause_id=None):
    out_path = get_model_data_path(
        is_input_file=False,
        model_group=model_group,
        launch_set_id=launch_set_id,
        cause_id=cause_id,
    )
    return read_df(out_path)


def get_malaria_noise_reduction_model_result(malaria_model_group, launch_set_id):
    malaria_dfs = []
    for model_group in malaria_model_group:
        if model_group != "NO_NR":
            df = just_keep_trying(
                get_model_result_data,
                args=[model_group, launch_set_id],
                max_tries=100,
                seconds_between_tries=6,
                verbose=True,
            )
            malaria_dfs.append(df)
    malaria_results = pd.concat(malaria_dfs)
    return malaria_results


def get_model_cols(df, model_group):
    if model_group.startswith("VA-") or (model_group == "CHAMPS"):
        return ["cf", "cf_pred", "se_pred", "cf_post", "var_post"]
    elif (
        model_group.startswith("VR-")
        or model_group.startswith("MATERNAL")
        or model_group.startswith("POLICE_VIOLENCE")
    ):
        return [
            col
            for col in df.columns
            if col.startswith("pred")
            or col.startswith("std_err")
            or (col == "average_prior_sample_size")
        ]
    else:
        raise AssertionError("ERROR")


def stitch_fallback_model_results(df, fallback_model_group, launch_set_id):
    if not df["fallback_flag"].any():
        return df

    merge_cols = [
        "nid",
        "extract_type_id",
        "location_id",
        "year_id",
        "age_group_id",
        "sex_id",
        "cause_id",
        "site_id",
    ]
    model_cols = get_model_cols(df, fallback_model_group)
    if model_group_is_run_by_cause(fallback_model_group):
        fallback_model = pd.concat(
            [
                get_model_result_data(fallback_model_group, launch_set_id, cause_id=cause_id)
                for cause_id in df.query("fallback_flag == 1").cause_id.unique().tolist()
            ]
        )
    else:
        fallback_model = get_model_result_data(fallback_model_group, launch_set_id)
    fallback_model = fallback_model[merge_cols + model_cols]

    df = df.merge(fallback_model, how="left", on=merge_cols, suffixes=("", "_fallback"))
    check_col = model_cols[0] + "_fallback"
    if df.loc[df.fallback_flag == 1][check_col].isnull().any():
        failed_merges = df.loc[(df.fallback_flag == 1) & (df[check_col].isnull())]
        failed_merges = failed_merges[merge_cols].drop_duplicates()
        fallback_model = fallback_model.merge(
            failed_merges, on=merge_cols, how="outer", indicator=True
        )
        assert fallback_model["_merge"].isin(["left_only", "right_only"]).all()
        df = df.loc[~((df.fallback_flag == 1) & (df[check_col].isnull()))]
    report_if_merge_fail(df.query("fallback_flag == 1"), check_col, merge_cols)

    for col in model_cols:
        df.loc[df.fallback_flag == 1, col] = df[col + "_fallback"]
        df = df.drop(col + "_fallback", axis=1)
    df = df.drop("fallback_flag", axis=1)
    return df


def get_noise_reduction_model_result(
    nid, extract_type_id, launch_set_id, iso3, model_group, data_type_id, cause_id=None
):
    results = just_keep_trying(
        get_model_result_data,
        args=[model_group, launch_set_id, cause_id],
        max_tries=100,
        seconds_between_tries=6,
        verbose=True,
    )

    if len(results) == 0 and cause_id is not None:
        return results

    results = results[(results["nid"] == nid) & (results["extract_type_id"] == extract_type_id)]

    fallback_model_group = get_fallback_model_group(model_group, iso3)
    if fallback_model_group != "NO_NR":
        results = stitch_fallback_model_results(
            results, fallback_model_group=fallback_model_group, launch_set_id=launch_set_id
        )

    drop_columns = [
        col
        for col in [
            "country_id",
            "subnat_id",
            "year_bin",
            "is_loc_agg",
            "fallback_flag",
            "iso3",
            "code_system_id",
        ]
        if col in results.columns
    ]
    results = results.drop(drop_columns, axis=1)

    if "VA-" not in model_group and "CHAMPS" not in model_group:
        assert not results.isnull().values.any()

    return results


def add_malaria_model_group_result(df, nid, extract_type_id, launch_set_id, malaria_model_group):
    keep_cols = df.columns.tolist()
    malaria_results = get_malaria_noise_reduction_model_result(malaria_model_group, launch_set_id)
    malaria_results = malaria_results.query(f"nid == {nid} & extract_type_id == {extract_type_id}")
    if len(malaria_results) > 0:
        malaria = df["cause_id"] == 345
        df = df.loc[~malaria]
    df = pd.concat([df, malaria_results], ignore_index=True)
    report_duplicates(
        df,
        [
            "nid",
            "extract_type_id",
            "site_id",
            "cause_id",
            "sex_id",
            "age_group_id",
            "location_id",
            "year_id",
        ],
    )
    df = df[keep_cols]
    return df


def run_phase(nid: int, extract_type_id: int, launch_set_id: int) -> pd.DataFrame:
    iso3, source, data_type_id, model_group, malaria_model_group = get_values_from_nid(
        nid,
        extract_type_id,
        values=["iso3", "source", "data_type_id", "model_group", "malaria_model_group"],
        force_rerun=False,
        block_rerun=True,
        cache_results=False,
        verbose=False,
        cache_dir=CONF.get_directory("db_cache"),
    )
    malaria_model_group = wrap(malaria_model_group)
    run_noise_reduction = True
    run_by_cause = False

    if model_group == "NO_NR":
        run_noise_reduction = False

    if model_group_is_run_by_cause(model_group):
        run_by_cause = True

    if run_noise_reduction:
        if run_by_cause:

            filepath = "FILEPATH"
            causes = sorted(list(pd.read_csv(filepath, dtype=int)["cause_id"].unique()))
        else:
            causes = [None]

        dfs = []
        for cause_id in causes:
            df = get_noise_reduction_model_result(
                nid,
                extract_type_id,
                launch_set_id,
                iso3,
                model_group,
                data_type_id,
                cause_id=cause_id,
            )
            df = df.drop(
                columns=[
                    "source",
                    "subreg",
                    "cf_pred",
                    "cf_post",
                    "var_post",
                    "std_error",
                    "se_pred",
                    "deaths",
                    "Unnamed: 0",
                ],
                errors="ignore",
            )
            dfs.append(df)
        df = pd.concat(dfs, ignore_index=True)

        if "NO_NR" not in malaria_model_group:
            df = add_malaria_model_group_result(
                df, nid, extract_type_id, launch_set_id, malaria_model_group
            )

        noise_reducer = NoiseReducer(data_type_id, source)
        df = noise_reducer.get_computed_dataframe(df)

    else:
        df = get_phase_output(
            "aggregation",
            nid,
            extract_type_id,
            get_input_launch_set_id(nid, extract_type_id, launch_set_id, "aggregation"),
        )
    return df


def main(nid: int, extract_type_id: int, launch_set_id: int) -> pd.DataFrame:
    df = run_phase(nid, extract_type_id, launch_set_id)
    write_phase_output(df, "noisereduction", nid, extract_type_id, launch_set_id)
    return df


if __name__ == "__main__":
    main(
        nid=int(sys.argv[1]),
        extract_type_id=int(sys.argv[2]),
        launch_set_id=int(sys.argv[3]),
    )
