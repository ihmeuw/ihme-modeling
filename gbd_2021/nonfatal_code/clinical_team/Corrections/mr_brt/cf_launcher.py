import subprocess
import time
import sys
import os
import shutil
import getpass
import itertools
import pandas as pd
import numpy as np
import datetime
from db_queries import get_age_metadata

from clinical_info.Functions import hosp_prep
from clinical_info.Corrections.correction_inputs.correction_factor_prep_master import create_non_ms_cf_inputs
from clinical_info.Mapping.clinical_mapping import get_clinical_process_data
from clinical_info.Functions.bundle_check import get_bundle_estimate_for_upload


def expand_grid(data_dict):
    """
    Create a dataframe from every combination of given values.
    """
    rows = itertools.product(*data_dict.values())
    return pd.DataFrame.from_records(rows, columns=data_dict.keys())

def get_expected_bundles(method, filename):
    if method=='run_all':
        #To check against all bundles:
        bundle_icg = get_clinical_process_data('icg_bundle', prod=True)
        bundles = bundle_icg.bundle_id.unique()
        bundles = expand_grid({
            'bundle_id': bundles,
            'cf' : ['cf1','cf2','cf3']
            })
    elif (method=='run_custom') or (method=='pred_only_custom'):
        bundles = pd.read_csv(filename)
    else:
        bundles = get_bundle_estimate_for_upload()
        est_cfs = {2: 'cf1', 3: 'cf2', 4: 'cf3', 7: 'cf1', 8: 'cf2', 9: 'cf3'}
        bundles['cf'] = bundles['estimate_id'].map(est_cfs)

        bundles = pd.DataFrame(bundles)
        bundles = bundles[['bundle_id', 'cf']]
        bundles = bundles[bundles.cf.notnull()]
    return bundles

def copyDirectory(src, dest, pred):
    try:
        if pred:
            shutil.copytree(src, dest, ignore=shutil.ignore_patterns('model_summaries.csv','model_draws.csv','prediction_newdata.csv'))
        else:
            shutil.copytree(src, dest)            
    # Directories are the same
    except shutil.Error as e:
        print('Directory not copied. Error: %s' % e)
    # Any error saying that the directory doesn't exist
    except OSError as e:
        print('Directory not copied. Error: %s' % e)

def model_copier(old_vers, folder):
    print("Copying directories from previous models:")
    t = [x[0] for x in os.walk(FILEPATH]
    for dirs in t:
        if 'mrbrt_' in dirs:
            mdir = dirs.split('/')[7]
            print(mdir)
            dest = f'{folder}by_cfbundle/{mdir}'
            copyDirectory(dirs, dest, True)
    return

def check_cf_inputs(prep_vers, method='run_active_only', filename=[], prod=True):
    print(f"Checking input files to correction factors in prep {prep_vers}")
    bundles = get_expected_bundles(method=method, filename=filename)

    failures = pd.DataFrame()
    for index,row in bundles.iterrows():
        bun = row['bundle_id']
        cf = row['cf']
        if not os.path.isfile(FILEPATH):
            err = "input file doesn't exist"
            message = f"Bundle {bun} {cf} {err}"
            failures=failures.append({'bundle_id': bun, 'cf': cf, 'Error': err}, ignore_index=True)
            print(message)
        elif pd.read_csv(FILEPATH).shape[0]==0:
            err = "input file is empty"
            message = f"Bundle {bun} {cf} {err}"
            print(message)
            failures=failures.append({'bundle_id': bun, 'cf': cf, 'Error': err}, ignore_index=True)
        else:
            failures = pd.DataFrame(columns=['bundle_id','cf','Error'])
    if len(failures.index)>=1:
        if prod:
            assert False, f"{len(failures.index)} tests failed on the input file. Fix the bundles or re-run in non-prod mode to continue."
        else:
            print(f"{len(failures.index)} tests failed on the input file.")
    return failures

def run_cf_models(vers, prep_vers, method, trim=0.2, knots='manual', bundle_file=[]):
    """
    This runs mr_brt models for provided cf/bundle combos
    """
    user = getpass.getuser()
    qsub = QSUB

    print("Running mr_brt_parent")
    subprocess.call(qsub, shell=True)

    time.sleep(120)
    # hold for master script and for worker jobs
    hosp_prep.job_holder(job_name='MASTER_COFA', sleep_time=10)
    hosp_prep.job_holder(job_name='cofa_', sleep_time=30)

    return

def check_cf_models(vers, prep_vers, method='run_active_only', filename=[], log=False, prod=True):
    
    if (method != 'pred_only') & (method != 'pred_only_custom'):
        print("checking input data")
        failures=check_cf_inputs(prep_vers=prep_vers, method=method, filename=filename, prod=False)
    else: 
        print("not running new models - input data won't be checked.")
        failures = pd.DataFrame(columns=['bundle_id','cf','Error'])

    print(f"checking model folders for cf version {vers}")
    bundles = get_expected_bundles(method=method, filename=filename)
    n = 0
    for index,row in bundles.iterrows():
        bun = row['bundle_id']
        cf = row['cf']
        if failures.shape[0]==0:
            inputerr = pd.DataFrame()
        else:
            inputerr = failures[(failures['bundle_id']==bun) & (failures['cf']==cf)]
        err = []
        if len(inputerr.index)==0:
            if not os.path.isfile(FILEPATH):
                err = "model was never summarized"
            elif not os.path.isfile(FILEPATH):
                err = "model predictions never ran or weren't written"
            elif not os.path.isfile(FILEPATH):
                err = "model predictions failed at formatting"
            if err:
                message = f"Bundle {bun} {cf} {err}"
                n += 1
                failures=failures.append({'bundle_id': bun, 'cf': cf, 'Error': err}, ignore_index=True)
    if n>=1:
        if prod:
            assert False, f"{n} CF models failed. Re-launch models or check error files for details."
        else:
            print(f"{n} CF models failed. Re-launch models or check error files for details.")
    else:
        print("All bundle-cf combos with input data ran successfully. Congrats!")

    d=datetime.datetime.now()
    
    failures['bundle_id'] = failures['bundle_id'].astype(int)
    if log:
        failures.to_csv(FILEPATH, index=False)
    return failures

def agesex_files(folder):
    # Compile all separate files into age/sex csvs
    df = pd.DataFrame()
    for filename in os.listdir(f"{folder}split/by_agesex/"):
        if filename.endswith(".csv"):
            df = df.append(pd.read_csv(f"{folder}split/by_agesex/{filename}"))

    for i, (name, group) in enumerate(df.groupby(['age_group_id', 'sex_id'])):
        age = name[0]
        sex = name[1]
        group.to_csv(f"{folder}/by_agesex/{age}_{sex}.csv", index=False)

    cffile = pd.DataFrame()
    for filename in os.listdir(f"{folder}split/cf_estimates/"):
        if filename.endswith(".csv"):
            cffile = cffile.append(pd.read_csv(f"{folder}split/cf_estimates/{filename}"))
    cffile.to_csv(f"{folder}cf_estimates.csv", index=False)

def cf_runner(vers, prep_vers, folder, method, prod, trim, knots, bundle_file = []):
    """
    1. Run all bundle/CF combinations.
    2. Run tests against active bundles in clinical.active_bundle_metadata
    3. Relaunch failures.
    4. Retest. Relaunch & retest again for 3 tries
    5. Test against all cf/bundle combinations. 
        Write a file with all failed bun/CFs and whether it's in clinical.active_bundle_metadata

    """
    run_cf_models(vers, prep_vers, method, trim, knots, bundle_file)

    if method == 'run_all':
        failures = check_cf_models(vers=vers, prep_vers=prep_vers, method='run_active_only', filename=[], log=False, prod=False)
    else: 
        failures = check_cf_models(vers=vers, prep_vers=prep_vers, method=method, filename=bundle_file, log=False, prod=False)

    tries = 0
    print("Relaunching failed models")
    while tries < 3 and failures.shape[0] > 0:
        retries = failures.loc[(failures['Error'] != "input file doesn't exist") & (failures['Error'] != "input file is empty")].copy()
        retries.drop('Error', axis=1, inplace=True)
        retries.to_csv(FILEPATH)
        run_cf_models(vers, prep_vers, method="run_custom", trim=trim, knots=knots, bundle_file=FILEPATH)
        if method == 'run_all':
            failures = check_cf_models(vers=vers, prep_vers=prep_vers, method='run_active_only', filename = [], log=False, prod=False)
        else: 
            failures = check_cf_models(vers=vers, prep_vers=prep_vers, method=method, filename = bundle_file, log=False, prod=False)
        tries += 1

    failures = check_cf_models(vers=vers, prep_vers=prep_vers, method=method, filename = bundle_file, log=True, prod=False)

if __name__ == '__main__':
    vers = sys.argv[1]
    data_folder = FILEPATH

    prep_vers = sys.argv[2]
    method = sys.argv[3]
    old_vers = sys.argv[4]
    if method == 'pred_only':
        model_copier(old_vers, data_folder)

    cf_runner(vers=vers, prep_vers=prep_vers, folder=data_folder, method=method, prod=False, bundle_file = bundle_file, trim = trim, knots=knots)
    agesex_files(data_folder)
