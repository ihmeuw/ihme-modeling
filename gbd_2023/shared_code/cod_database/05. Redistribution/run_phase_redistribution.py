import sys
from pathlib import Path
from typing import Any, Dict, Optional

import numpy as np
import pandas as pd

from cod_prep.claude.claude_io import get_input_launch_set_id, get_phase_output, write_phase_output
from cod_prep.claude.configurator import Configurator
from cod_prep.claude.hiv_correction import HIVCorrector
from cod_prep.claude.redistribution import Redistributor
from cod_prep.downloaders import (
    get_ages,
    get_cause_map,
    get_current_cause_hierarchy,
    get_current_location_hierarchy,
    get_map_version,
    get_pop,
    get_remove_decimal,
    get_values_from_nid,
)
from cod_prep.utils import CodSchema, print_log_message

CONF = Configurator()


def needs_garbage_correction(iso3, data_type_id):
    return iso3 in ["ZAF", "MOZ", "ZWE"] and data_type_id == 9


def cause_map_post_rd(
    df: pd.DataFrame,
    cause_map: pd.DataFrame,
    col_meta: Optional[Dict[str, Dict[str, Any]]] = None,
) -> pd.DataFrame:
    df = df.merge(cause_map.loc[:, ["code_id", "cause_id"]], on="code_id")
    is_garbage = df["cause_id"] == 743
    if is_garbage.any():
        with pd.option_context("display.max_columns", 50, "display.max_rows", 100):
            raise RuntimeError(f"Some garbage remained in the data:\n{df.loc[is_garbage, :]}")
    id_cols = [
        c for c in CodSchema.infer_from_data(df, metadata=col_meta).id_cols if c != "code_id"
    ]
    return df.groupby(id_cols, observed=True, as_index=False).agg({"deaths": sum})


def run_phase(
    df: pd.DataFrame,
    nid: int,
    extract_type_id: int,
    write_diagnostics: bool,
) -> pd.DataFrame:
    cache_kwargs = {
        "force_rerun": False,
        "block_rerun": True,
        "cache_results": False,
        "cache_dir": CONF.get_directory("db_cache"),
        "verbose": False,
    }
    cause_set_version_id = CONF.get_id("cause_set_version")
    location_set_version_id = CONF.get_id("location_set_version")
    pop_run_id = CONF.get_id("pop_run")
    code_system_id, data_type_id, iso3 = get_values_from_nid(
        nid,
        extract_type_id,
        values=["code_system_id", "data_type_id", "iso3"],
        **cache_kwargs,
    )
    remove_decimal = get_remove_decimal(code_system_id, **cache_kwargs)
    code_map_version_id = get_map_version(code_system_id, **cache_kwargs)
    loc_meta_df = get_current_location_hierarchy(
        location_set_version_id=location_set_version_id,
        **cache_kwargs,
    )
    age_meta_df = get_ages(**cache_kwargs)
    cause_meta_df = get_current_cause_hierarchy(
        cause_set_version_id=cause_set_version_id,
        **cache_kwargs,
    )
    cause_map = get_cause_map(
        code_map_version_id=code_map_version_id,
        **cache_kwargs,
    )

    if cause_map["code_system_id"].drop_duplicates().item() != code_system_id:
        raise ValueError("Code system of NID/extract does not match the code map version ID.")

    orig_deaths_sum = df["deaths"].sum()

    if needs_garbage_correction(iso3, data_type_id):
        print_log_message(f"Correcting Garbage for {iso3}")
        orig_gc_sum = df.query("cause_id == 743")["deaths"].sum()

        pop_meta_df = get_pop(pop_run_id=pop_run_id, **cache_kwargs)
        hiv_corrector = HIVCorrector(
            df,
            iso3,
            code_system_id,
            pop_meta_df,
            cause_meta_df,
            loc_meta_df,
            age_meta_df,
            correct_garbage=True,
        )
        df = hiv_corrector.get_computed_dataframe()
        after_gc_sum = df.query("cause_id == 743")["deaths"].sum()
        after_deaths_sum = df["deaths"].sum()
        print_log_message(
            f"Stage [gc deaths / total deaths]\n"
            f"Before GC correction [{orig_gc_sum} / {orig_deaths_sum}]\n"
            f"After GC correction [{after_gc_sum} / {after_deaths_sum}]"
        )

    df.drop(columns="cause_id", inplace=True)
    proportional_cols = {{"location_id": "admin1_or_above_id"}.get(c, c) for c in df.columns} & set(
        CONF.get_id("potential_rd_proportional_cols")
    )
    df = Redistributor(
        conf=CONF,
        code_system_id=code_system_id,
        proportional_cols=list(proportional_cols),
        loc_meta_df=loc_meta_df,
        age_meta_df=age_meta_df,
        cause_meta_df=cause_meta_df,
        cause_map=cause_map,
        remove_decimal=remove_decimal,
        diagnostics_path=(
            Path(CONF.get_directory("rd_process_data"), str(nid), str(extract_type_id))
            if write_diagnostics
            else None
        ),
    ).run_redistribution(df)
    df = cause_map_post_rd(df, cause_map)

    after_deaths_sum = df["deaths"].sum()
    before_after_text = (
        f"Before GC redistribution: {orig_deaths_sum}\n"
        f"After GC redistribution: {after_deaths_sum}"
    )
    if not np.isclose(orig_deaths_sum, after_deaths_sum):
        raise AssertionError(f"Deaths not close.\n{before_after_text}")
    else:
        print_log_message(before_after_text)

    return df


def main(nid: int, extract_type_id: int, launch_set_id: int) -> pd.DataFrame:
    df = get_phase_output(
        "misdiagnosiscorrection",
        nid,
        extract_type_id,
        get_input_launch_set_id(nid, extract_type_id, launch_set_id, "misdiagnosiscorrection"),
    )
    if df.eval("cause_id == 743").any():
        print_log_message("Running redistribution")
        df = run_phase(df, nid, extract_type_id, write_diagnostics=launch_set_id != 0)
    else:
        print_log_message("No redistribution to do.")
        val_cols = ["deaths"]
        group_cols = list(set(df.columns) - set(["code_id"] + val_cols))
        df = df.groupby(group_cols, as_index=False)[val_cols].sum()

    write_phase_output(df, "redistribution", nid, extract_type_id, launch_set_id)
    return df


if __name__ == "__main__":
    main(
        nid=int(sys.argv[1]),
        extract_type_id=int(sys.argv[2]),
        launch_set_id=int(sys.argv[3]),
    )
