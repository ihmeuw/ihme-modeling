import sys
import time
from pathlib import Path

import pandas as pd
import numpy as np

from cod_prep.claude.claude_io import (
    get_claude_data,
    get_input_launch_set_id,
    get_phase_output,
    write_phase_output,
)
from cod_prep.claude.configurator import Configurator
from cod_prep.claude.covid_correction import CovidCorrector
from cod_prep.downloaders.nids import get_value_from_nid

CONF = Configurator("standard")

AVAILABLE_CODE_SYSTEMS = [1, 74]
AVAILABLE_YEARS = [2020, 2021, 2022]
EXCLUDE_ISOS = ['CHN', 'SAU']


def run_pipeline(df, nid, extract_type_id, launch_set_id):
    iso3 = get_value_from_nid(
        nid, extract_type_id, value="iso3", force_rerun=False, block_rerun=True
    )
    code_system_id = get_value_from_nid(
        nid, extract_type_id, value="code_system_id", force_rerun=False, block_rerun=True
    )
    year_id = get_value_from_nid(
        nid, extract_type_id, value="year_id", force_rerun=False, block_rerun=True
    )
    if type(year_id) == np.ndarray:
        year_id = int(max(year_id))
    data_type_id = get_value_from_nid(
        nid, extract_type_id, value="data_type_id", force_rerun=False, block_rerun=True
    )

    if (code_system_id in AVAILABLE_CODE_SYSTEMS) and (year_id in AVAILABLE_YEARS) and (iso3 not in EXCLUDE_ISOS) and (data_type_id == 9):
        ccorrector = CovidCorrector(
            nid, extract_type_id, iso3, year_id, code_system_id, as_launch_set_id=launch_set_id
        )
        df = ccorrector.get_computed_dataframe(df)
    return df


def main(nid: int, extract_type_id: int, launch_set_id: int) -> pd.DataFrame:
    start_time = time.time()
    as_launch_set_id = get_input_launch_set_id(
        nid, extract_type_id, launch_set_id, "redistribution"
    )
    df = get_phase_output(
        "redistribution",
        nid,
        extract_type_id,
        as_launch_set_id,
    )

    df = run_pipeline(df, nid, extract_type_id, launch_set_id)
    run_time = time.time() - start_time

    write_phase_output(df, "covidcorrection", nid, extract_type_id, launch_set_id)
    return df


if __name__ == "__main__":
    main(
        nid=int(sys.argv[1]),
        extract_type_id=int(sys.argv[2]),
        launch_set_id=int(sys.argv[3]),
    )
