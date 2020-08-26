"""
Centrally prep CF inputs, run MR-BRT models, and validate file output.
Sub-processes kicked off by this script:
	correction_factor_prep_master.py - creates non-Marketscan CF inputs
	prep_all_cfs.R - generates raw CF values and formats input to MR-BRT
	mr_brt_parent.R - sends jobs to run MR-BRT models by bundle-CF combo
		trim_worker.R - runs each MR-BRT model
"""

import subprocess
import time
import sys
import os
import getpass
import itertools
import pandas as pd
import numpy as np
import datetime
from db_queries import get_age_metadata

from clinical_info.Functions import hosp_prep
from clinical_info.Corrections.correction_factor_prep_master import create_non_ms_cf_inputs
from clinical_info.Mapping.clinical_mapping import get_clinical_process_data
from clinical_info.Functions.bundle_check import get_bundle_estimate_for_upload

def expand_grid(data_dict):
    """
    Create a dataframe from every combination of given values.
    See https://pandas.pydata.org/pandas-docs/version/0.17.1/cookbook.html#creating-example-data
    """
    rows = itertools.product(*data_dict.values())
    return pd.DataFrame.from_records(rows, columns=data_dict.keys())

def get_expected_bundles(method, folder, filename):
	if method=='active_only':
		bundles = get_bundle_estimate_for_upload()
		est_cfs = {2: 'cf1', 3: 'cf2', 4: 'cf3', 7: 'cf1', 8: 'cf2', 9: 'cf3'}
		bundles['cf'] = bundles['estimate_id'].map(est_cfs)

		bundles = pd.DataFrame(bundles)
		#bundles['cf'] = np.select(estimates, cfs, default = 'no_cf')
		bundles = bundles[['bundle_id', 'cf']]
		bundles = bundles[bundles.cf.notnull()]
	elif method=='file':
		bundles = pd.read_csv('{fol}{fname}'.format(fol=folder, fname=filename))
	else:
		#To check against all bundles:
		bundle_icg = get_clinical_process_data('icg_bundle', prod=True)
		bundles = bundle_icg.bundle_id.unique()
		bundles = expand_grid({
			'bundle_id': bundles,
			'cf' : ['cf1','cf2','cf3']
			})
	return bundles

"""
1. Kick off correction factor prep master
2. Wait until complete, confirm success
3. Kick off prep cfs script
4. Wait until complete, confirm success
5. Kick off mr_brt_parent
"""

def prep_cf_inputs(run_id):
	subprocess.call('prep_all_cfs.R {r}'\
		.format(u=user, r=run_id), shell=True)
	return

"""
Right now this checks bundle-cf input files against what's in clinical.bundle. Is that what it should be doing? Alternative is to check against all bundles but that'll make a lot of noise
"""
def check_cf_inputs(folder, prod=True, method = 'active_only', filename=np.nan):
	print("Checking input files to correction factors in {}".format(folder))
	bundles = get_expected_bundles(folder = folder, method=method, filename = filename)

	failures = pd.DataFrame()
	for index,row in bundles.iterrows():
		bun = row['bundle_id']
		cf = row['cf']
		if os.path.isfile('{f}prep_data/{cf}/{bundle}.csv'.format(f=folder,cf=cf,bundle=bun))==False:
			err = "input file doesn't exist"
			message = "Bundle {b} {cf} is active but {e}".format(b=bun, cf=cf, e=err)
			failures=failures.append({'Bundle': bun, 'CF': cf, 'Error': err}, ignore_index=True)
			print(message)
		elif os.path.getsize('{f}prep_data/{cf}/{bundle}.csv'.format(f=folder,cf=cf,bundle=bun))==0:
			err = "input file is empty"
			message = "Bundle {b} {cf} is active but {e}".format(b=bun, cf=cf, e=err)
			print(message)
			failures=failures.append({'Bundle': bun, 'CF': cf, 'Error': err}, ignore_index=True)
	if len(failures.index)>=1:
		if prod:
			assert False, "{l} tests failed on the input file. Fix the bundles or re-run in non-prod mode to continue.".format(l=len(failures.index))
		else:
			print("{l} tests failed on the input file.".format(l=len(failures.index)))
	return failures




def run_cf_models(run_id):
	"""
	This runs mr_brt models for all possible cf/bundle combos. To test or run specific combinations 
	"""
	qsub = QSUB

	print("Running mr_brt_parent")
	subprocess.call(qsub, shell=True)

	time.sleep(120)
	# hold for master script and for worker jobs
	hops_prep.job_holder(job_name='MASTER_COFA', sleep_time=50)
	hosp_prep.job_holder(job_name='cofa_', sleep_time=200)
	return

def check_cf_models(run_id, folder, prod=True, method='active_only', filename=np.nan):
	print("checking input data")
	failures=check_cf_inputs(folder=data_folder, prod=False, method=method)

	print("checking model folders")
	bundles = get_expected_bundles(folder = folder, method=method, filename=filename)
	n = 0
	for index,row in bundles.iterrows():
		bun = row['bundle_id']
		cf = row['cf']
		inputerr = failures[(failures['Bundle']==bun) & (failures['CF']==cf)]
		if len(inputerr.index)==0:
			if os.path.isdir('{f}by_cfbundle/mrbrt_{bundle}_{cf}'.format(f=folder,cf=cf,bundle=bun))==False:
				err = "model never ran"
				message = "Bundle {b} {cf} {e}".format(b=bun, cf=cf, e=err)
				n += 1
				print(message)
				failures=failures.append({'Bundle': bun, 'CF': cf, 'Error': err}, ignore_index=True)

	print("checking age-sex combos")
	ages = get_age_metadata(age_group_set_id=12)
	ages = ages[['age_group_id']]
	agesexes = expand_grid({
		'age_group_id': ages.age_group_id,
		'sex': [1,2]
		})

	for index,row in agesexes.iterrows():
		age = row['age_group_id']
		sex = row['sex']
		if os.path.isfile('{f}by_agesex/{age}_{sex}.csv'.format(f=folder,age=age, sex=sex))==False:
			message = "Age-sex file {age}_{sex} is expected but missing".format(age=age, sex=sex)
			print(message)

	if n>=1:
		if prod:
			assert False, "{l} CF models failed. Re-launch models or check error files for details.".format(l=n)
		else:
			print("{l} CF models failed. Re-launch models or check error files for details.".format(l=n))
	else:
		print("All bundle-cf combos with input data ran successfully.")

	d=datetime.datetime.now()
	failures.to_csv('{f}runerrors_{dt}.csv'.format(f=folder,dt=d.strftime("%Y-%m-%d_%H%M")), index=False)
	return

if __name__ == '__main__':
    run_id = sys.argv[1]
    data_folder = FILEPATH
    create_non_ms_cf_inputs(run_id, user)
    prep_cf_inputs(run_id)
    run_cf_models(run_id)
    check_cf_models(run_id, folder=data_folder, prod=False, method='active_only')