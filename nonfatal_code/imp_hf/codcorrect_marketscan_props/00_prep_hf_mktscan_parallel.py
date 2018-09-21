
"""
Merges Market-Scan data etiology counts with heart failure and CODcorrect 
death-data for each HF etiology to produce the proportion of total heart
Failure due to each etiology.

The 6 most significant of the etiologies are uploaded to the Epi-database 
such that they can be used as inputs for Dismod
"""

# custom, local directories
from upload_hf_mktscan_prep import add_columns_and_upload
from slacker import Slacker
from hf_processes import MarketScan, CoDcorrect, MarketScan_DeathData
from hf_functions import wait, timestamp, make_custom_causes
from diagnostic_data import setup_for_shiny
import submitter

# standard and installed libraries
import datetime
import time
import subprocess
import pandas as pd
import numpy as np
import os
import sys
import shutil
import smtplib

# shared functions libraries
from db_queries import get_location_metadata


def save_comp_causes(prefix):
	"""
	Organizes and saves the custom-composite (aggregate) causes in a CSV, where
	sub-processes can retrive them -- better to keep it in a central location in
	case it changes.
	"""
	# Aggregate causes
	other_resp = {509, 510, 511, 512, 513, 514, 515, 516}
	other_resp_code = 520

	# Make dataframe
	other_resp_df = pd.DataFrame({'cause_id':list(other_resp)})
	other_resp_df['composite_id'] = other_resp_code

	# all but ihd, cmp, htn, rhd and (copd/interstitial/pnuem)
	other_heart_failure = {390, 503, 614, 616, 618, 619, 643, 507, 388}
	other_hf_code = 385

	# Make dataframe
	other_hf_df = pd.DataFrame({'cause_id':list(other_heart_failure)})
	other_hf_df['composite_id'] = other_hf_code

	# alcoholic cardiomyopathy, myocarditis, and other cardiomyopathy
	other_cmp = {938, 942, 944}
	other_cmp_code = 499
	
	# Make dataframe
	other_cmp_df = pd.DataFrame({'cause_id':list(other_cmp)})
	other_cmp_df['composite_id'] = other_cmp_code
	
	# append these and save them to a CSV
	comp_causes = other_resp_df.append(other_hf_df)\
							   .append(other_cmp_df)\
							   .reset_index()\
							   .drop('index', axis=1)
	comp_causes.to_csv("{}composite_cause_list.csv".format(prefix))
	
	return comp_causes


if __name__ == '__main__':
	""" MAIN """

	# The bundle IDs associated with the significant HF etiologies that will be
	# uploaded to the database 
	BUNDLE_IDS = [285, 286, 287, 288, 289, 290]
	
	# file paths
	prefix = "FILEPATH"
	metadata_path="FILEPATH"

	mktscan_in_path = "FILEPATH"
	mktscan_out_path = "FILEPATH"
	out_dir = "FILEPATH"
	upload_dir = "FILEPATH"
	
	#### change the below to YOUR username ####
	user_name = raw_input("What's your username (for qsub errors/outputs? ")
	user_name= str(user_name)
	
	status_dir = "FILEPATH"
	viz_path = ("FILEPATH{}/imp_hf/input_diagnostics/").format(user_name)
	
	# qsub diagnostic file path
	qsub_output = "FILEPATH{}/HF/output".format(user_name)
	qsub_errors = "FILEPATH{}/HF/errors".format(user_name)
	
	print "qsub errors and outputs will be written to:"
	print "   *", qsub_errors
	print "   *", qsub_output
	print "respectivily\n"
	#### change the above to YOUR username ####
	
	# columns to merge on
	index_cols = ['age_group_id', 'sex_id', 'cause_id']
	
	# Columns used by "adjust_std_error_with_limit"
	group_cols = ['location_id', 'age_group_id', 'sex_id']
	
	if not os.path.exists(prefix):
		os.makedirs(prefix)
	if not os.path.exists(status_dir):
		os.makedirs(status_dir)
	if not os.path.exists(viz_path):
		os.makedirs(viz_path)
		
	# composite cause and sub-cause IDs 
	comp_causes = save_comp_causes(prefix)
	
	# Ask the user if the want the prog to send a Slack when the program is 
	# done.
	send_Slack = raw_input("Do you want to Slack a person/channel when the prog"
	" finishes: ")
	send_Slack = str(send_Slack).upper()
	if send_Slack == "YES":
		token = raw_input("Slack API token: ")
		channel = raw_input("#channel or @person to send message: ")
		token = str(token)
		channel = str(channel)
		try:
			slack = Slacker(token)
			worked = True
		except:
			worked = False
		
		assert worked, "Token not recognized"
	
	else:
		slack = None
		channel = None
		token = None
	
	# Ask the user if they want to upload or first validate the upload
	re_query = raw_input("Do you need to get new CODcorrect data: ")
	re_query = str(re_query).upper()
	
	# Ask the user if they want to upload or first validate the upload
	upload = raw_input("Enter 'upload' to upload the data or 'validate' to " 
					   "validate the data for upload: ")
	upload = str(upload).upper()
	
	# Get locations (only countries and subnationals)
	locations_df = get_location_metadata(location_set_id=35,
										 gbd_round_id=4).query('level>=3')
	LOCATION_IDS = locations_df.location_id.unique()
	
	# Get MarketScan data with uncertainty
	marketscan = MarketScan(in_path=mktscan_in_path,
							out_path=mktscan_out_path,
							metadata_path=metadata_path,
							group_cols=['age_group_id',
										'sex_id'],
							measure_cols=['cases',
										  'cases_wHF',
										  'sample_size'],
							AGE_RESTRICTED_CAUSES={938, 512, 509, 513, 498, 
												   516, 493, 514, 492, 511},
							comp_causes,
							success_cases_col='cases_wHF',
							sample_size_col='cases',
							collapse=True,
							seed=1325)
	marketscan.get_marketscan()
	
	if send_Slack and marketscan.mktscan[index_cols].duplicated().any():
		message = 'there are duplicate location-age-sex-causes in MarketScan.'
		slack.chat.post_message(channel, message)
	
	# download cod correct data for causes in marketscan
	COD_CAUSE_IDS = marketscan.mktscan.query('cause_id not in {}'.format(
					list(comp_causes.composite_id.unique()))).cause_id.unique()
	
	# list including custom cause IDs
	ALL_CAUSE_IDS = marketscan.mktscan.cause_id.unique()
	
	# get death data
	if re_query == "YES":
		codcorrect = CoDcorrect(prefix=prefix,
								qsub_errors=qsub_errors,
								qsub_output=qsub_output,
								CAUSE_IDS=COD_CAUSE_IDS,
								LOCATION_IDS=LOCATION_IDS,
								comp_causes,
								mean_col='mean_death', 
								upper_col='upper_death', 
								lower_col='lower_death',
								send_Slack=send_Slack,
								slack=slack,
								token=token,
								channel=channel)
								
		codcorrect.get_death_data()
		cod_correct = codcorrect.death_data
		cod_correct.to_csv('{}codcorrect_results.csv'.format(prefix),
						   index=False)
	else:
		cod_correct = pd.read_csv('{}codcorrect_results.csv'.format(prefix))
	
	# calculate MS*deaths and coefficient of variation for each
	marketscan_deathdata = MarketScan_DeathData(mktscan=marketscan.mktscan,
											   codcorr=cod_correct,
											   prop_hf_col='prop_hf',
											   cc_death_col='mean_death',
											   mkt_cv_col='coeff_var_mkt',
											   cc_cv_col='coeff_var',
											   hf_deaths_col='hf_deaths', 
											   cv_col='mkt_cc_cv',
											   group_cols=group_cols,
											   mkt_coeff_var='coeff_var_mkt',
											   cod_coeff_var='coeff_var',
											   hf_prop_var='hf_target_prop',
											   limit=0.25)
								   
	marketscan_deathdata.get_inputs()
	
	df = marketscan_deathdata.mktscan_codcorr
	
	# verify that there are no duplicates
	id_cols = ['location_id', 'age_group_id', 'sex_id', 'cause_id']
	assert not df[id_cols].duplicated().any(), \
		   'there are duplicate location-age-sex-causes'
	
	# verify that all the HF etiologies proportion sum up to 1
	assert np.allclose(df.groupby(group_cols)['hf_target_prop'].sum(), 1.0),\
		   "proportions don't sum to 1"
	
	# drop all irrelevant columns
	df_out = df[index_cols + 
	            ['location_id'] + 
			    ['hf_target_prop', 'std_err_adj']]
	
	setup_for_shiny(df_out, viz_path)
	
	splits = df_out.query("cause_id not in {}".format(
						  list(comp_causes.composite_id.unique()))).copy()
	splits = make_custom_causes(splits,
								group_cols = group_cols,
								measure_cols = ['hf_target_prop',
												'std_err_adj'])
		
	#### For debugging ####
	write = True
	#write = False
	#### For debugging ####
	
	if write:
		
		# Make a latest run time stamp
		time_stamp = timestamp()
		
		# save files for DisMod inputs
		df_out.to_csv(("{out_dir}/heart_failure_target_props_subnat_"
		               "{time_stamp}.csv").format(out_dir=out_dir,
												  time_stamp=time_stamp),
					  index=False)
		df_out.to_csv("{out_dir}/heart_failure_target_props_subnat.csv".format(\
					                                           out_dir=out_dir),
					  index=False)
		
		# Save files for splitting composites out into individual etiologies
		splits.to_csv(("{out_dir}/heart_failure_SPLIT_AGGs_subnat_{time_stamp}"
		               ".csv").format(out_dir=out_dir, 
					                  time_stamp=time_stamp),
					  index=False)
		splits.to_csv("{out_dir}/heart_failure_SPLIT_AGGs_subnat.csv".format(\
		                                                       out_dir=out_dir),
			          index=False)
		
		# The message emailed when the main script has finished everything
		# but the upload.
		message = ("Hi,\nThe Heart Failure Market-Scan/CODcorrect code has "
				   "finished running.\n The inputs will begin uploading soon.")
		
		if send_Slack == "YES":
			slack.chat.post_message(channel, message)
		else:
			channel = None
			token = None
			slack = None
		
		# prepare inputs for diagnostic (a shiny app)
		
		# Upload data to the database.
		if upload == 'UPLOAD' or upload == 'VALIDATE':
			add_columns_and_upload(bundle_ids, upload_dir, status_dir, out_dir)
		
		# Upload data to the database.
		if upload == 'UPLOAD' or upload == 'VALIDATE':
			# errors
			if not os.path.exists(qsub_errors):
				os.makedirs(qsub_errors)
			else:
				shutil.rmtree(qsub_errors)
				os.makedirs(qsub_errors)
			
			# output
			if not os.path.exists(qsub_output):
				os.makedirs(qsub_output)
			else:
				shutil.rmtree(qsub_output)
				os.makedirs(qsub_output)
				
			#exit()
				

			# run upload script
			for bundle_id in BUNDLE_IDS:
				call = ('qsub -cwd -N "upload_{bundle_id}" '
				        '-P proj_custom_models '
						'-o {qsub_output} '
						'-e {qsub_errors} '
						'-l mem_free=40G -pe multi_slot 10 cluster_shell.sh '
						'upload_it.py '
						'{bundle_id} '
						'{upload} '
						'{out_dir} '
						'{status_dir} '
						'{send_Slack} '
						'{channel} '
						'{token}'.format(qsub_output=qsub_output, 
										 qsub_errors=qsub_errors,
										 bundle_id=str(bundle_id),
										 upload=str(upload),
										 out_dir=str(upload_dir),
										 status_dir=str(status_dir),
										 send_Slack=str(send_Slack),
										 channel=str(channel),
										 token=str(token)))									
				subprocess.call(call, shell=True)	
			wait('upload', 100)
			
			# Upload data to the database.
			if upload == 'UPLOAD':
				message = ("Inputs for {BUNDLE_IDS} have been uploaded to the "
				           "Database").format(BUNDLE_IDS=BUNDLE_IDS)
			else:
				message = ("Inputs for {BUNDLE_IDS} have been validated for "
				           "upload to the Database").format(\
						                                  BUNDLE_IDS=BUNDLE_IDS)
				
			if send_Slack == "YES":	
				slack.chat.post_message(channel, message)
		