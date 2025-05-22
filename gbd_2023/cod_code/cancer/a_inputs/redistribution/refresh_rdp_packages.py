'''
Filename: refresh_rdp_packages.py

Description: Copies the RDP packages from CoD's directory to our Cancer team's local directory.
             These packages are not sourced in our pipeline and are just used to explore if we have questions about package behavior.

Contributors: INDIVIDUAL NAME
'''

import pandas as pd
import os
import numpy as np
import shutil
from cancer_estimation._database import cdb_utils
from cancer_estimation.py_utils import(
    common_utils as utils
)
# PACKAGE PATH

fp_rdp_packages_cod = utils.get_path(key = 'CoD_packages', process = 'mi_dataset')
fp_rdp_packages_cancer = utils.get_path('mi_dataset_resources', process="mi_dataset") + "/redistribution/"
dict_icd_package_set_id = {'ICD10':utils.get_gbd_parameter('icd10_package_set_id'),
						'ICD9_detail':utils.get_gbd_parameter('icd9_detail_package_set_id')}

def refresh_rdp_packages():
	""" Copies RDP packages from CoD's RDP package directory to the Cancer team's local RDP package directory

		ICD10 (package_set_id = 1) and ICD9_detail (package_set_id = 6)'s RDP packages are copied.
	"""
	for icd_coding_system, icd_package_set_id in dict_icd_package_set_id.items():
		fp_icd_package_set_cod = fp_rdp_packages_cod + str(icd_package_set_id) + "/"
		fp_icd_package_set_cancer = fp_rdp_packages_cancer + str(icd_package_set_id) + "/"

		if os.path.isdir(fp_icd_package_set_cancer):
			print("Destination folder already exists, deleting existing folder in the cancer directory {} and attempting to copy RDP packages again".format(fp_icd_package_set_cancer))
			shutil.rmtree(path = fp_icd_package_set_cancer)
			shutil.copytree(src = fp_icd_package_set_cod, dst = fp_icd_package_set_cancer)
		else:
			shutil.copytree(src = fp_icd_package_set_cod, dst = fp_icd_package_set_cancer)
		print("Copied RDP package directory from {} to {}".format(fp_icd_package_set_cod, fp_icd_package_set_cancer))

		# assert number of files in the cancer team directory matches the number of files in CoD's directory
		file_count_cod = sum([len(files) for r, d, files in os.walk(fp_icd_package_set_cod)])
		file_count_cancer = sum([len(files) for r, d, files in os.walk(fp_icd_package_set_cancer)])
		assert file_count_cod == file_count_cancer, "File count mismatch between CoD's RPD directory for {} and the what was copied into the cancer RPD directory {}".format(fp_icd_package_set_cod, fp_icd_package_set_cancer)

		print("Copied coding system: {}'s RDP packages from CoD's RDP directory to the Cancer team's RDP directory successfully".format(icd_coding_system))

if __name__ == '__main__':
	""" Runs the refresh_rdp_packages() function.
	"""
	refresh_rdp_packages()