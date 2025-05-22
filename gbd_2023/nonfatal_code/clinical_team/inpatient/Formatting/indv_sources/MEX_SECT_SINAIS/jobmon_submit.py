"""
Jobmon submit & data compilation script for
MEX_SECT_SINAIS formatting, parallelized by year
@author: USERNAME

Years of data captured: 2004-2020

"""

import getpass
import glob
import os
from pathlib import Path

import pandas as pd

from crosscutting_functions import *
from crosscutting_functions.ci_jobmon import ezjobmon

# set env variables
user = getpass.getuser()
root = f"FILEPATH{user}"
base = f"{root}FILEPATH"


def get_mex_hosp_files():
    """
    Function to get only relevant hospital discharge data file for MEX from
    specified GHDx record folder

    """

    # get all MEX files in list
    input_dir = "FILEPATH"
    all_files = glob.glob(
        os.path.join(input_dir, "MEX_*INSTITUTIONS_HEALTH_SECTOR_HOSP_DIS*_*.CSV")
    )
    # we don't want below extraneous files
    cat_files = glob.glob(
        os.path.join(input_dir, "MEX_*INSTITUTIONS_HEALTH_SECTOR_HOSP_DIS*_*_CAT*.CSV")
    )
    gbd_files = glob.glob(
        os.path.join(input_dir, "MEX_*INSTITUTIONS_HEALTH_SECTOR_HOSP_DIS*_*_GBD*.CSV")
    )
    xtr_files = cat_files + gbd_files
    # select to only data files
    data_files = [f for f in all_files if f not in xtr_files]

    return data_files


def compile_and_write(keep_indv_files=False):
    """
    Function to concatenate the separate formatted years of MEX hospital data

    Arg -
        keep_indv_files (bool): Specify if we want to keep individual year's
        file from jobmon output

    """

    out_base = "FILEPATH"
    out_dir = Path(f"{out_base}/individual_year/")
    write_path = f"{out_base}/data/intermediate/formatted_MEX_SECT_SINAIS.H5"

    # compile all years' files
    full_df = pd.concat(
        pd.read_parquet(parquet_file) for parquet_file in out_dir.glob("*.parquet")
    )

    hosp_prep.write_hosp_file(full_df, write_path, backup=True)

    if not keep_indv_files:
        # remove individual files once concat file is wrote out
        for f in glob.glob(f"{out_dir}/*.parquet"):
            os.remove(f)

    print(f">> All years of data successfully formatted and written out in {write_path}")


if __name__ == "__main__":
    SCRIPT_PATH = f"{base}/MEX_SECT_SINAIS/01_format_MEX_SECT_SINAIS.py"
    LOG_PATH = "FILEPATH"

    files = get_mex_hosp_files()
    workflow_status = ezjobmon.submit_batch(
        name="MEX_SECT_format_by_yrs",
        script_path=SCRIPT_PATH,
        log_path=LOG_PATH,
        inputs=files,
        ram=8,
    )

    if workflow_status == "D":
        compile_and_write()
    else:
        raise ValueError(">> Workflow was not sucessful")
