"""
Re making numerators and denominators for the outpatient correction factor.

The current claims process does _something_ wrong, not sure what. It is faster
and easier to make this script and run it on the output files from one step
prior in claims.
"""

import numpy as np
import subprocess
import getpass
import os
import pandas as pd


def submit_qsubs(run_id):
    username = getpass.getuser()
    repo = "FILEPATH".format(username)
    groups = 350
    jobs = np.arange(0, groups, 1)
    for group in jobs:
        qsub = f"""qsub """
        qsub = " ".join(qsub.split())

        subprocess.call(qsub, shell=True)
        if group == 1:
            print(qsub)


def collect_results(run_id):

    base_dir = ("FILEPATH"
                "FILENAME".format(run_id))

    files_list = os.listdir(base_dir)

    assert len(files_list) == 350

    assert len(set(files_list)) == 350

    df = pd.read_csv("FILEPATH".format(base_dir, files_list[0]))

    counter = 1
    for f in files_list[1:]:
        temp_df = pd.read_csv("FILEPATH".format(base_dir, f))

        if temp_df.isnull().any().any():
            print("{} has nulls".format(f))

        df = pd.concat([df, temp_df], ignore_index=True, sort=False)
        counter += 1

        df = df.groupby(df.drop(['val'], axis=1).columns.tolist()).agg(
            {'val': 'sum'}).reset_index()

        print(counter)
    print(counter)

    return df


def save_otp_cf(df, run_id):
    base_dir = (f"FILEPATH"
                "FILENAME")
    filepath = "FILEPATH".format(base_dir)
    print("Saving to {}...".format(filepath))
    df.to_csv(filepath, index=False)
    print("Saved.")


def check_claims_otp_cf_process(run_id):

    otp_cf_base_dir = (f"FILEPATH"
                       "FILENAME")
    claims_process_filepath = (
        f"FILEPATH")

    df_claims = pd.read_csv(claims_process_filepath)

    ad_hoc_base_dir = ("FILEPATH"
                       "FILENAME".format(run_id))
    ad_hoc_filepath = "FILEPATH".format(
        ad_hoc_base_dir)

    df_ad_hoc = pd.read_csv(ad_hoc_filepath)

    assert df_ad_hoc.equals(df_claims)
