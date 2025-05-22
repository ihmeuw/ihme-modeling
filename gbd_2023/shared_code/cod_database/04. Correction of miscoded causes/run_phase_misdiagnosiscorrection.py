"""Correct misdiagnosis of Alzheimer's.

Inputs called 'misdiagnosis_probabilities' are used and need to be re-generated
when there are substantial changes to ICD10 mapping, especially to which
packages garbage codes are redistributed in.
"""
import sys
import time
from pathlib import Path

import pandas as pd

from cod_prep.claude.cause_reallocation import (
    PoliceConflictCorrector,
    manually_reallocate_gbdcauses,
)
from cod_prep.claude.claude_io import get_input_launch_set_id, get_phase_output, write_phase_output
from cod_prep.claude.configurator import Configurator
from cod_prep.claude.misdiagnosis_correction import MisdiagnosisCorrector
from cod_prep.downloaders import (
    get_cod_ages,
    get_map_version,
    get_remove_decimal,
    get_values_from_nid,
)
from cod_prep.utils import print_log_message

CONF = Configurator("standard")

AVAILABLE_CODE_SYSTEMS = [1, 2, 4, 6, 7, 8, 9, 40, 73, 74]


def clear_intermediate_data(mc_process_dir, adjust_id, nid, extract_type_id):
    """Clear intermediate files as soon as run starts"""
    for subdir in ["ui", "pct", "orig"]:
        Path(mc_process_dir, str(adjust_id), subdir, f"{nid}_{extract_type_id}.csv").unlink(
            missing_ok=True
        )


def get_above_forty(**cache_kwargs):
    """Returns a list of ages 40 and older. Msdc only affects deaths in those ages"""
    cod_ages = get_cod_ages(**cache_kwargs)
    above_forty = list(cod_ages.loc[cod_ages["age_group_years_start"] >= 40, "age_group_id"])
    return above_forty


def run_pipeline(df: pd.DataFrame, nid: int, extract_type_id: int) -> pd.DataFrame:
    """Run full pipeline"""
    cache_kwargs = {
        "force_rerun": False,
        "block_rerun": True,
        "cache_results": False,
        "cache_dir": CONF.get_directory("db_cache"),
        "verbose": False,
    }
    code_system_id, data_type_id, iso3 = get_values_from_nid(
        nid,
        extract_type_id,
        values=["code_system_id", "data_type_id", "iso3"],
        **cache_kwargs,
    )
    code_map_version_id = get_map_version(code_system_id, **cache_kwargs)
    remove_decimal = get_remove_decimal(code_system_id, **cache_kwargs)

    over_40 = get_above_forty(**cache_kwargs)
    has_over_40 = df["age_group_id"].isin(over_40).values.any()
    if code_system_id in AVAILABLE_CODE_SYSTEMS and has_over_40:
        adjust_ids = [543, 544, 500]
        for adjust_id in adjust_ids:
            print_log_message("Clearing intermediate files.")
            clear_intermediate_data(
                CONF.get_directory("mc_process_data"), adjust_id, nid, extract_type_id
            )
            print_log_message("Running misdiagnosis correction for cause {}".format(adjust_id))
            misdiagnosis_corrector = MisdiagnosisCorrector(
                nid,
                extract_type_id,
                code_system_id,
                code_map_version_id,
                adjust_id,
                remove_decimal,
            )
            df = misdiagnosis_corrector.get_computed_dataframe(df)

    if code_system_id == 1 and data_type_id == 9 and iso3 == "ZAF":
        print_log_message("Special correction step for ICD10 South Africa VR.")
        df = manually_reallocate_gbdcauses(
            df, code_map_version_id, force_rerun=False, block_rerun=True
        )

    if data_type_id == 9 and iso3 == "USA":
        print_log_message("Special correction step for Police Violence in United States.")
        pcc = PoliceConflictCorrector(
            # TODO: find other sources from which to draw deaths to account for this shortage
            tolerance=0.01,  # maximum shortage from the crosswalk as a percent of total expected per year
            code_map_version_id=code_map_version_id,
            force_rerun=False,
            block_rerun=True,
        )
        df = pcc.get_computed_dataframe(df)
    return df


def main(nid: int, extract_type_id: int, launch_set_id: int) -> pd.DataFrame:
    start_time = time.time()
    df = get_phase_output(
        "disaggregation",
        nid,
        extract_type_id,
        get_input_launch_set_id(nid, extract_type_id, launch_set_id, "disaggregation"),
    )
    df = run_pipeline(df, nid, extract_type_id)
    run_time = time.time() - start_time
    print_log_message(f"Finished in {run_time:.2f} seconds")

    write_phase_output(df, "misdiagnosiscorrection", nid, extract_type_id, launch_set_id)
    return df


if __name__ == "__main__":
    main(
        nid=int(sys.argv[1]),
        extract_type_id=int(sys.argv[2]),
        launch_set_id=int(sys.argv[3]),
    )
