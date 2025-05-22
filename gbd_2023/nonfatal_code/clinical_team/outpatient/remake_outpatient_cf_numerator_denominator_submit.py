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
import warnings


def submit_qsubs(run_id):
    username = getpass.getuser()
    repo = FILEPATH
    groups = 350
    jobs = np.arange(0, groups, 1)
    for group in jobs:
        qsub = QSUB
        qsub = " ".join(qsub.split())  # clean up spaces and newline chars

        subprocess.call(qsub, shell=True)
        if group == 1:
            print(qsub)


def collect_results(run_id, groups=350):

    base_dir = FILEPATH

    files_list = os.listdir(base_dir)
    files_list = [f"{base_dir}/{f}" for f in files_list]

    expected_files = []
    for i in range(groups):
        expected_files.append(
            f"{base_dir}{FILEPATH}{i}")
    if len(files_list) != groups:
        warnings.warn("The number of files we read back in doesn't match "
                      "the number of jobs sent out")
        missing_files = set(expected_files) - set(files_list)
        raise ValueError("Look to the logs for failed jobs. These files are"
                         f"missing: {missing_files}")

    assert len(files_list) == 350

    assert len(set(files_list)) == 350

    df = pd.read_csv(files_list[0])

    counter = 1
    for f in files_list[1:]:
        temp_df = pd.read_csv(f)

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
    base_dir = FILEPATH
    filepath = FILEPATH.format(base_dir)
    print("Saving to {}...".format(filepath))
    df.to_csv(filepath, index=False)
    print("Saved.")


def check_claims_otp_cf_process(run_id):

    otp_cf_base_dir = FILEPATH
    claims_process_filepath = (
        f"{otp_cf_base_dir}/FILEPATH")

    df_claims = pd.read_csv(claims_process_filepath)

    ad_hoc_base_dir = FILEPATH
    ad_hoc_filepath = FILEPATH

    df_ad_hoc = pd.read_csv(ad_hoc_filepath)

    assert df_ad_hoc.equals(df_claims)
