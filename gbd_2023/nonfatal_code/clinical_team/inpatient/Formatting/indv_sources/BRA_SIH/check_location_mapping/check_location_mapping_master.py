"""
Check location mapping. This script has three functions. One to qsub, one to
run all the qsubs, and one to read the results of the worker scripts. The
results are not collected automatically. These functions are meant to be
imported and ran interactively.

"""

import getpass
import os
import subprocess

import pandas as pd


def check_loc_submit_brazil_jobs(years):
    """
    qsubs jobs by year for all years.

    Returns
    -------
    None
    """

    for year in years:

        qsub = f"""
        qsub -N BRA_loc_map_{year} -P proj_hospital
        -o FILEPATH/{getpass.getuser()}/output/
        -e FILEPATH/{getpass.getuser()}/errors/
        ADDRESS
        FILEPATH/{getpass.getuser()}/FILEPATH/python_shell_clinical_env.sh
        FILEPATH/{getpass.getuser()}/FILEPATH/check_location_mapping.py
        {year}"""
        qsub = " ".join(qsub.split())
        subprocess.call(qsub, shell=True)
        print(f"Submitted year {year}.")


def submit():
    """
    Submits qsub for each year

    Returns
    -------
    None
    """

    years = list(range(1992, 2014 + 1))
    check_loc_submit_brazil_jobs(years)


def collect_results():
    """
    Collects, processes, and prints results

    Returns
    -------
    Pandas Dataframe with columns "Location of Residence",
    "Location of Hospital", "count"
    """

    base_dir = (
        "FILEPATH"
        "FILEPATH"
    )

    files = os.listdir(base_dir)
    files = [f"{base_dir}/{f}" for f in files]

    df_list = []
    for file in files:
        df = pd.read_csv(file)
        df_list.append(df)

    df = pd.concat(df_list, ignore_index=True, sort=False)

    df_list = []
    for loc in df.location_name.unique():
        tmp = df[df.location_name == loc].state_name.value_counts().reset_index()
        tmp = tmp.head()
        tmp["location_name"] = loc
        df_list.append(tmp)

    result = pd.concat(df_list, ignore_index=False, sort=True)

    # set columns
    result.columns = ["Location of Hospital", "Location of Residence", "count"]

    # re-order columns
    result = result[["Location of Residence", "Location of Hospital", "count"]]

    print(result)

    return result
