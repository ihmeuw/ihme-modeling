
# local/custom libraries
import submitter
from slacker import Slacker
from hf_functions import wait

# standard libraries
import os
import sys
import time
import subprocess
import pandas as pd
import shutil

# remote libraries
from db_queries import get_ids


def submit_save_results_jobs(write_hdf,
							 save_Results,
							 in_path,
							 save_path,
							 qsub_output,
							 qsub_errors,
							 send_Slack,
							 slack,
							 token, 
							 channel):
	"""submit jobs to use save_result"""
	
	if not os.path.exists(save_path):
		os.makedirs(save_path)	
	if not os.path.exists(qsub_output):
		os.makedirs(qsub_output)
	else:
		shutil.rmtree(qsub_output)
		os.makedirs(qsub_output)
	if not os.path.exists(qsub_errors):
		os.makedirs(qsub_errors)
	else:
		shutil.rmtree(qsub_errors)
		os.makedirs(qsub_errors)

	IDS = pd.read_csv("{}MEs_and_Etiologies.csv".format(in_path))
	
	# not Chagas
	IDS = IDS.query('cause_id != 346')
	
	#### TEMPORARY ####
	#IDS = IDS.query('cause_id in (493, 942, 511, 503)')
	#### TEMPORARY ####
	
	CAUSE_IDS = IDS['cause_id'].unique()
	
	# Start parallelization
	cause_count = 0
	
	#### For debugging ####
	need_draws = True
	#need_draws = False
	#### For debugging ####
	
	if need_draws:
	
		for cause_id in CAUSE_IDS:
			cause_count += 1
			# get location name and convert to a string
				
			call = ('qsub -cwd -N "save_{cause_id}" -P proj_custom_models '
					'-o {qsub_output} '
					'-e {qsub_errors} '
					'-l mem_free=20G -pe multi_slot 10 cluster_shell.sh '
					'01_upload_corrected_Subprocess.py '
					'{save_path} '
					'{in_path} '
					'{cause_id} '
					'{write_hdf} '
					'{save_Results} '
					'{send_Slack} '
					'{token} '
					'{channel}'.format(qsub_output=str(qsub_output),
									   qsub_errors=str(qsub_errors),
									   save_path=str(save_path),
									   in_path=str(in_path),
									   cause_id=str(cause_id),
									   write_hdf=str(write_hdf),
									   save_Results=str(save_Results),
									   send_Slack=str(send_Slack),
									   token=str(token),
									   channel=str(channel)))	
			subprocess.call(call, shell=True)
			print ("    The corrected prevalence draws for {} etilogies are"
        		   "being uploaded.").format(cause_count)
		wait('save', 100)
			
if __name__ == '__main__':
	"""MAIN"""
	
	# Ask the user if they want to write new HDF
	write_hdf = raw_input("Do you want write a new HDF for each etiology? ")
	write_hdf = str(write_hdf).upper()
	
	# Ask the user if they want to use "save_results" to upload the HDFs
	save_Results = raw_input("Do you want to use save_results to upload the HDF"
	                         " for each etiology? ")
	save_Results = str(save_Results).upper()
	
	# Ask the user if they want the prog to send a Slack when the program is
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
	
	submit_save_results_jobs(write_hdf,
	                         save_Results,
							 send_Slack,
							 slack,
							 token,
							 channel)
	
	
	
	
	
	