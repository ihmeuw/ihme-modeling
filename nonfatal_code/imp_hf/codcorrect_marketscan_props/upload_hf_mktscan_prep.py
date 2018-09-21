
"""This script uploads (or validates for upload) the inputs produced by
	'00_prep_hf_mktscan_parallel.py'"""

# local/custom libraries	
from slacker import Slacker
import submitter
from hf_functions import wait, timestamp, sex_fix, age_fix

# standard libraries
import sys
import os
import shutil
import datetime
import time
import pandas as pd
import numpy as np
from itertools import izip
import smtplib
import subprocess

# remote/virtual environment libraries
from db_queries import get_location_metadata
from db_tools.ezfuncs import query
		

#	Prepare the inputs to merge/delete/add to the Epi database under a
#	given bundle ID and NID.
def assign_row_nums(df, bundle_id, nid, me_id):
	"""Fills in missing seqs in input dataframe

		Args:
			df (object): pandas dataframe object of input data
			engine (object): ihme_databases class instance with dUSERt
				engine set
			me_id (int): modelable_entity_id for the dataset in memory

		Returns:
			Returns a copy of the dataframe with the seqs filled in,
			increment strarting from the max of the database for the given
			modelable_entity.
		"""
	
	# variable used for indicating if Epi database row deletion is necessary
	delete_rows = False
	
	# necessary columns
	needed_cols = ['year_start',
				   'year_end',
				   'age_start',
				   'age_end',
				   'sex',
				   'location_id',
				   'mean',
				   'standard_error',
				   'measure_id',
				   'nid',
				   'bundle_id']

	index_cols = ['location_id',
				  'year_start',
				  'year_end',
				  'age_start',
				  'age_end',
				  'sex',
				  'measure_id',
				  'nid',
				  'bundle_id']
	
	# time stamp for upload metadata
	Time = timestamp()
	
	# Query that pulls the data for the unique bundle ID	
	q = ('''SELECT seq, location_id, year_start, year_end, age_start, age_end, 
	sex_id, measure_id, nid, bundle_id 
	FROM epi.bundle_dismod 
	WHERE bundle_id={bundle_id} AND nid={nid};'''.format(bundle_id=bundle_id,
               	                                         nid=nid))
	
	# execute query
	data = query(q, conn_def="epi")
	
	# get row numbers for all rows including for other NIDS
	all_seqs = data['seq']
	
	# recode sex_id to be sex names
	data.rename(columns = {'sex_id':'sex'}, inplace=True)
	sexes = data['sex']
	sexes = sexes.apply(sex_fix)
	data['sex'] = sexes
	
	# recode year to be current latest year
	data['year_end'] = 2016
	
	# get location names
	locations_df = get_location_metadata(location_set_id=35)[['location_id',
	                                                          'location_name']]
	
	# if the data pulled has zero rows, then make new row numbers
	if len(data) == 0:
		#df['seq'] = range(1,len(df)+ 1)
		df['seq'] = np.nan
		
		# drop all unneeded columns
		df = df[needed_cols + ['seq']]
		#df = df[needed_cols]
		
		# append location names
		df = df.merge(locations_df, on='location_id', how='inner')
	else:
		# make a identifier "new" for the data to check the merge
		df['new'] = 1
		
		# perform an outer merge of the new data on old data on location_id, 
		# and year_start
		df = df.merge(data, on=index_cols, how='outer')
		
		# find all the rows where "seq" is null -- these are rows that need to be inserted
		null_df = df[df['seq'].isnull()]
		
		# find all the rows where "new" is null -- these are rows that need to be deleted
		no_match = df[df['new'].isnull()]
		
		print "LENGTH", len(df) 
		
		# take all the rows where all the index columns matched (the inner merge)
		# these are the rows to be updated
		df = df[(df['seq'].notnull())&(df['mean'].notnull())&(df['new'].notnull())]
		
		# drop all unnecessary columns
		df = df[needed_cols + ['seq']]
		
		print df.seq.unique()

		# if it wasn't a perfect merge -- if there are nulls, then
		print "LENGTH", len(null_df)
		print "LENGTH", len(no_match)
		print "LENGTH", len(df) 
		if len(null_df) != 0 or len(no_match) != 0:
			# drop the null row numbers (null "seq").
			null_df = null_df[needed_cols]
			
			# Append location name.
			null_df = null_df.merge(locations_df, on='location_id', how='inner')
			
			# If the number of rows to be deleted is greater than the number of rows that need to be
			# inserted then 
			if len(no_match) > len(null_df):
				# get the row numbers of the rows that need to be deleted, the leftovers rows will be replaced by
				# those to be inserted
				replace_seqs = no_match['seq'].tolist()[:len(null_df)]
				
				for seq in replace_seqs.seq.unique():
					assert seq not in df.seqs.unique(), "seq {} is a dupliacte.".format(seq)
				
				get_rid = no_match.query('seq not in {}'.format(replace_seqs)).copy()
				
				# make all columns of get_rid of empty except seq, and bundle ID
				get_rid['bundle_id'] = np.nan
				get_rid['nid'] = np.nan
				get_rid['location_id'] = np.nan
				get_rid['sex'] = np.nan
				get_rid['mean'] = np.nan
				get_rid['standard_error'] = np.nan
				get_rid['measure_id'] = np.nan
				get_rid['year_start'] = np.nan
				get_rid['year_end'] = np.nan
				get_rid['age_start'] = np.nan
				get_rid['age_end'] = np.nan
				get_rid['unit_type'] = np.nan
				get_rid['unit_type_value'] = np.nan
				get_rid['measure_issue'] = np.nan
				get_rid['uncertainty_type'] = np.nan
				get_rid['uncertainty_type_value'] = np.nan
				get_rid['extractor'] = np.nan
				get_rid['representative_name'] = np.nan
				get_rid['urbanicity_type'] = np.nan
				get_rid['response_rate'] = np.nan
				get_rid['sampling_type'] = np.nan
				get_rid['recall_type'] = np.nan
				get_rid['recall_type_value'] = np.nan
				get_rid['case_name'] = np.nan
				get_rid['case_definition'] = np.nan
				get_rid['case_diagnostics'] = np.nan
				get_rid['note_modeler'] = np.nan
				get_rid['cv_hospital'] = np.nan
				get_rid['cv_marketscan'] = np.nan
				get_rid['cv_low_income_hosp'] = np.nan
				get_rid['cv_high_income_hosp'] = np.nan
				get_rid['is_outlier'] = np.nan
				get_rid['cases'] = np.nan
				get_rid['measure'] = np.nan
				get_rid['sample_size'] = np.nan
				get_rid['effective_sample_size'] = np.nan
				get_rid['source_type'] = np.nan
				get_rid['underlying_nid'] = np.nan
				get_rid['input_type'] = np.nan
				get_rid['design_effect'] = np.nan
				get_rid['unit_value_as_published'] = np.nan	
				get_rid['date_inserted'] = np.nan
				get_rid['last_updated'] = np.nan
				get_rid['inserted_by'] = np.nan
				get_rid['last_updated_by'] = np.nan
				get_rid['upper'] = np.nan
				get_rid['lower'] = np.nan
				
				# flip the "delete rows" indicator to True
				delete_rows = True

			# otherwise the rows to be deleted are replaced until new rows need to be inserted entirely:
			# Make the row numbers blank
			else:
				null_df['seq'] = np.nan
				null_df.reset_index(inplace=True)
				null_df.drop('index', axis=1, inplace=True)
				replace_seqs = no_match['seq'].tolist()
				null_df.loc[0:len(replace_seqs)-1, 'seq'] = replace_seqs
			
			# and append them to those being updated
			df = df.append(null_df)

	# check if row nums assigned properly
	
	print len(df[df.seq.notnull()])
	print len(df[df.seq.isnull()])
	
	assert not any(df[df.seq.notnull()].seq.duplicated()), "Duplicate row numbers assigned"

	# fill in columns required by the Epi Uploader
	df['unit_type'] = "Person"
	df['unit_type_value'] = 2.0
	df['measure_issue'] = 0.0
	df['uncertainty_type'] = "Standard error"
	#df['uncertainty_type_id'] = 1
	df['uncertainty_type_value'] = np.nan
	df['extractor'] = "USER"
	df['representative_name'] = "Nationally and subnationally representative"
	df['urbanicity_type'] = "Unknown"
	df['response_rate'] = np.nan
	df['sampling_type'] = np.nan
	df['recall_type'] = "Point"
	df['recall_type_value'] = 1.0
	df['case_name'] = np.nan
	df['case_definition'] = np.nan
	df['case_diagnostics'] = np.nan
	df['note_modeler'] = 'Proportion generated from CODEm deaths using Marketscan data'
	df['cv_hospital'] = 0
	df['cv_marketscan'] = 1
	df['cv_low_income_hosp'] = 0
	df['cv_high_income_hosp'] = 0
	df['is_outlier'] = 0
	df['cases'] = np.nan
	df['measure'] = "proportion"
	df['sample_size'] = np.nan
	df['effective_sample_size'] = np.nan
	df['source_type'] = "Mixed or estimation"
	df['underlying_nid'] = np.nan
	df['input_type'] = "extracted"
	df['design_effect'] = np.nan
	df['unit_value_as_published'] = 1
	df['date_inserted'] = Time
	df['last_updated'] = Time
	df['inserted_by'] = "USERNAME"
	df['last_updated_by'] = "USERNAME"
	df['upper'] = np.nan
	df['lower'] = np.nan
	
	# Query the Epi database for modelable entity names
	q1 = '''SELECT modelable_entity_name
			FROM epi.modelable_entity
			WHERE modelable_entity_id={};'''.format(me_id)
	me_name = str(query(q1, conn_def="epi").loc[0,'modelable_entity_name'])
	
	df['modelable_entity_id'] = me_id
	df['modelable_entity_name'] = me_name
	
	# If the "delete rows" indicator is on,
	if delete_rows:
		# then append the rows set up to be deleted: leaving only NID, bundle ID, and seq (row number)
		df = df.append(get_rid)
	
	return df


def add_columns_and_upload(bundle_ids, out_dir, status_dir, in_path):
	""" Description:
			uploads data under each respective bundle ID and NID.

		Args:
			bundle_ids (list)
	"""
	
	# organize modelable entity IDs, bundle IDs, cause IDs, and NID
	IDs = pd.DataFrame({'bundle_id':[285, 286, 287, 288, 289, 290],
						'cause_id':[493, 498, 520, 492, 499, 385],
						'nid':[250478, 250479, 250480, 250481, 250482, 250483],
						'me_id':[2414, 2415, 2416, 2417, 2418, 2419],
						})
			   
	# Find the respective cause ID, NID, and ME ID for the given bundle ID/s.
	IDs = IDs.query('bundle_id in {}'.format(bundle_ids))
	cause_ids = IDs['cause_id'].tolist()
	nids = IDs['nid'].tolist()
	me_ids = IDs['me_id'].tolist()

	if not os.path.exists(out_dir):
		os.makedirs(out_dir)
	if not os.path.exists(status_dir):
		os.makedirs(status_dir)

	# Bring in the new HF proportion inputs.
	new_df = pd.read_csv('{in_path}heart_failure_target_props_subnat.csv'.format(
														in_path=in_path))													

	# Filter out Sub-Saharan Africa.
	super_regions = get_location_metadata(location_set_id=35)[['location_id','super_region_id']]
	new_df = new_df.merge(super_regions, on='location_id', how='inner')
	new_df = new_df.query('super_region_id != 166')
	new_df.drop('super_region_id', axis=1, inplace=True)
	
	count = 0
	for bundle_id, nid, cause_id, me_id in izip(bundle_ids, nids, cause_ids, me_ids):
		
		#### For debugging ####
		fix = True
		#fix = False
		#### For debugging ####
		
		print cause_id
		print nid
		print me_id
		
		if fix:
				
			# Fill in some necessary columns for the merge/replacement of pre-existing input data.
			new_inputs = new_df.query('cause_id == {cause_id}'.format(cause_id=cause_id))
			
			new_inputs.drop('cause_id', axis=1, inplace=True)
			new_inputs['measure_id'] = 18
			new_inputs['nid'] = nid
			new_inputs['bundle_id'] = bundle_id
			new_inputs.rename(columns={'hf_target_prop':'mean', 'std_err_adj':'standard_error'}, inplace=True)
			
			# recode age groups to age range
			q0 = ('SELECT age_group_id, age_group_years_start AS age_start, '
				  'age_group_years_end AS age_end '
				  'FROM shared.age_group')
			age_df = query(q0, conn_def="shared")
			age_df['age_end'] = age_df.apply(age_fix, axis=1)
			new_inputs = new_inputs.merge(age_df, on='age_group_id', how='inner')
			new_inputs.drop('age_group_id', axis=1,
						   inplace=True)
			
			# recode sex IDs to sex names
			new_inputs.rename(columns = {'sex_id':'sex'}, inplace=True)
			sexes = new_inputs['sex']
			sexes = sexes.apply(sex_fix)
			new_inputs['sex'] = sexes
			
			# set years (put year_end as 2015 for merging purposes, recode to 2016)
			new_inputs['year_start'] = 1990
			new_inputs['year_end'] = 2016
				
			# fill in "seqs"
			new_inputs = assign_row_nums(new_inputs, bundle_id, nid, me_id)
			
			# Write the upload sheet as an Excel sheet labeled "extraction" -- this is
			# the required format by the Epi Uploader 
			writer = pd.ExcelWriter('{out_dir}new_inputs_{bundle_id}.xlsx'.format(
																	out_dir=out_dir,
																	bundle_id=bundle_id), engine='xlsxwriter')		   
			new_inputs.to_excel(writer, sheet_name='extraction', index=False, encoding='utf-8')
			writer.save()
			
			print new_inputs.shape
			count += 1
			print "{0} main/composite etiology inputs ready to to {0} bundle ID".format(count)	
	
#### MAIN ####
if __name__ == '__main__':
	
	# bundle IDs to be uploaded to
	BUNDLE_IDS = [285, 286, 287, 288, 289, 290]
	
	# Input and output directories
	in_path = "FILEPATH"
	out_dir = "FILEPATH"
	upload_dir = "FILEPATH"
	status_dir = "FILEPATH"
	
	#### change the below to YOUR username ####
	user_name = raw_input("What's your username (for qsub errors/outputs? ")
	user_name= str(user_name)
	
	# qsub diagnostic file path
	qsub_output = "FILEPATH/{}/HF/output".format(user_name)
	qsub_errors = "FILEPATH/{}/HF/errors".format(user_name)
	
	print "qsub errors and outputs will be written to:"
	print "   *", qsub_errors
	print "   *", qsub_output
	print "respectivily\n"
	#### change the above to YOUR username ####
	
	#### For debugging ####
	fix = True
	#fix = False
	#### For debugging ####
	
	if fix:
		if not os.path.exists(out_dir):
			os.makedirs(out_dir)
		else:
			shutil.rmtree(out_dir)
			os.makedirs(out_dir)
			print("Deleting and remaking an empty {}.".format(out_dir))

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
		oshutil.rmtree(qsub_output)
		os.makedirs(qsub_output)
		
	# Ask the user if the want the prog to send a Slack when the program is done.
	send_Slack = raw_input("Do you want to Slack a person/channel when the prog finishes: ")
	send_Slack = str(send_Slack).upper()
	if send_Slack == "YES":
		token = raw_input("Slack API token: ")
		token = str(token)
		try:
			slack = Slacker(token)
			worked = True
		except:
			worked = False
		
		assert worked == True, "Token not recognized"
		channel = raw_input("#channel or @person to send message: ")
		channel = str(channel)
	else:
		slack = None
		channel = None
		token = None
	
	# Ask the user for some info
	upload = raw_input("Enter upload or test to validate: ")
	upload = str(upload).upper()
		
	# upload/validate
	add_columns_and_upload(bundle_ids, out_dir, status_dir, in_path)
	
	# Upload data to the database.
	if upload == 'UPLOAD' or upload == 'VALIDATE':
		# run upload script
		for bundle_id in BUNDLE_IDS:
			call = ('qsub -cwd -N "upload_{bundle_id}" -P proj_custom_models '
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
			message = "Inputs for {BUNDLE_IDS} have been uploaded to the Database".format(BUNDLE_IDS=BUNDLE_IDS)
		else:
			message = "Inputs for {BUNDLE_IDS} have been validated for upload to the Database".format(BUNDLE_IDS=BUNDLE_IDS)
			
		if send_Slack == "YES":	
			slack.chat.post_message(channel, message)

	



	