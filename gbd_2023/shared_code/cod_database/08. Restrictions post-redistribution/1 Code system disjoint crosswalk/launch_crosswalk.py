import pandas as pd
import numpy as np

from pathlib import Path
from cod_prep.claude.configurator import Configurator
from datetime import date
from crosswalk_helper_functions import *
from crosswalk_main_functions import *
from crosswalk_vetting_functions import *

CONF = Configurator()
RUN_ID = date.today().strftime("%m_%d_%Y")
OVERLAP = 3

ROOT_DIR = Path(FILEPATH)

WORK_DIR = Path(FILEPATH)
CACHE_FILE_DATA_ORIG = WORK_DIR / FILEPATH
CACHE_FILE_CROSSWALK_IN = WORK_DIR / FILEPATH
OUT_FILE = WORK_DIR / FILEPATH
CROSSWALK_OUT = WORK_DIR / FILEPATH
R_CROSSWALK_PATH = ROOT_DIR / FILEPATH


if __name__ == '__main__':
    make_run_directory(WORK_DIR)

    format_disjoint_demographics(ROOT_DIR, WORK_DIR)
    years_to_use = get_years_to_use(OVERLAP, CONF)
    demographics_to_run = get_demographics_to_run(WORK_DIR)
    nid_loc_years = get_nid_loc_years(years_to_use, demographics_to_run)
    df = pull_claude_redistribution_data(
        CONF, CACHE_FILE_DATA_ORIG, OUT_FILE, 
        nid_loc_years, demographics_to_run
        )
    df.to_parquet(CACHE_FILE_DATA_ORIG)
    launch_crosswalk_R_code(
        df, nid_loc_years, demographics_to_run, RUN_ID,
        R_CROSSWALK_PATH, CACHE_FILE_CROSSWALK_IN, CROSSWALK_OUT, CACHE_FILE_DATA_ORIG
        )
    final = apply_and_rake_results(df, WORK_DIR)
    final = save_final_results(final, RUN_ID, ROOT_DIR)
    create_ICD_10_vetting_files(CONF, WORK_DIR)
    generate_plots(ROOT_DIR, WORK_DIR)
