
# local/custom libraries
import submitter
from slacker import Slacker
from make_correction_factors import Factors
from upload_corrected_Submit import submit_save_results_jobs
from hf_functions import wait

# standard libraries
import os
import shutil
import time
import subprocess
import pandas as pd
import unicodedata
from itertools import izip

# remote libraries
from db_queries import get_location_metadata, get_demographics, get_ids

			
def get_marketscan(df):	
	"""MarketScan"""
	local_index_cols = ['age_group_id', 'sex_id', 'cause_id', 'step']
	
	# add these percentiles to the dataset		
	df['proportion'] = df['mk_prop']
	df['lower'] = 0
	df['upper'] = 0
	
	# we don't care about prevalence from MarketScan 	
	df['prevalence'] = 0
	df['lower_prev'] = 0
	df['upper_prev'] = 0
	
	# ID the step of the HF modeling process
	df['step'] = "MarketScan"
	df = df[local_index_cols +
			['proportion', 'lower', 'upper',
			 'prevalence', 'lower_prev', 'upper_prev']]
			
	return df
	
def make_square_matrix(df1, df2):
	"""Make sure the matrix doesn't have any missing data."""
	
	df1['merge_col'] = 1
	
	LOCATIONS = df2[['location_id']].drop_duplicates().reset_index()
	LOCATIONS['merge_col'] = 1
	LOCATIONS.drop('index', axis=1, inplace=True)
	
	YEARS = df2[['year_id']].drop_duplicates().reset_index()
	YEARS['merge_col'] = 1

	square = LOCATIONS.merge(YEARS, on='merge_col', how='inner')
				  
	df1 = square.merge(df1, how='inner')
	df1.drop('merge_col', axis=1, inplace=True)
	
	return df1
		
def fill_missing(df, index_cols):
	"""If data isn't present fill draws as 0"""
	
	LOCATIONS = df[['location_id']].drop_duplicates().reset_index()
	LOCATIONS['merge_col'] = 1
	LOCATIONS.drop('index', axis=1, inplace=True)
	
	YEARS = df[['year_id']].drop_duplicates().reset_index()
	YEARS['merge_col'] = 1
	YEARS.drop('index', axis=1, inplace=True)
	
	CAUSES = df[['cause_id']].drop_duplicates().reset_index()
	CAUSES['merge_col'] = 1
	CAUSES.drop('index', axis=1, inplace=True)
	
	AGE_GROUPS = df[['age_group_id']].drop_duplicates().reset_index()
	AGE_GROUPS['merge_col'] = 1
	AGE_GROUPS.drop('index', axis=1, inplace=True)

	SEXES = df[['sex_id']].drop_duplicates().reset_index()
	SEXES['merge_col'] = 1
	SEXES.drop('index', axis=1, inplace=True)
	
	STEPS = df[['step']].drop_duplicates().reset_index()
	STEPS['merge_col'] = 1

	square = LOCATIONS.merge(CAUSES, on='merge_col', how='inner')\
					  .merge(YEARS, on='merge_col', how='inner')\
					  .merge(SEXES, on='merge_col', how='inner')\
					  .merge(STEPS, on='merge_col', how='inner')\
					  .merge(AGE_GROUPS, on='merge_col', how='inner')
				  
	df = df.merge(square, on=index_cols, how='right')
	df.fillna(0, inplace=True)
	
	df = df[index_cols +
			['proportion', 'lower', 'upper',
			 'prevalence', 'lower_prev', 'upper_prev']]
	
	return df
	
def get_correction_factors(info_path, in_path, out_path, draws_path):
	"""get the correction factors and write them to a CSV"""
	
	# Instantiate the process class
	factors = Factors(info_path, in_path, draws_path, out_path)
	factors.correction_factor()
	
	# save correction factors to a CSV so they are accessible by the qsubs
	factors.corrections.to_csv("{out_path}corrections_draws.csv".format(out_path=out_path),
	                           index=False,
							   encoding='utf-8')
	
	return factors.mktscan
			
def location_name_string(row):
	try:
		location_name = str(row['location_name'])
	except:
		location_name = unicodedata.normalize('NFKD', row['location_name']).encode('ascii','ignore')
	return location_name
	
def round_numbers(row_val):
	return round(row_val, 5)
	
def assert_df_is_square(df):
	"""
	Assert that the dataframe has all locations and is square on age, sex, 
	and cause.
	
	Throws:
		AssertionError if not true
	"""
	CAUSES = df[['cause_id']].drop_duplicates().reset_index()
	CAUSES['merge_col'] = 1

	AGE_GROUPS = df[['age_group_id']].drop_duplicates().reset_index()
	AGE_GROUPS['merge_col'] = 1

	SEXES = df[['sex_id']].drop_duplicates().reset_index()
	SEXES['merge_col'] = 1

	YEARS = df[['year_id']].drop_duplicates().reset_index()
	YEARS['merge_col'] = 1
	
	LOCATIONS = df[['location_id']].drop_duplicates().reset_index()
	LOCATIONS['merge_col'] = 1
	
	STEPS = df[['step']].drop_duplicates().reset_index()
	STEPS['merge_col'] = 1

	square = LOCATIONS.merge(CAUSES, on='merge_col', how='inner')\
					  .merge(YEARS, on='merge_col', how='inner')\
					  .merge(SEXES, on='merge_col', how='inner')\
					  .merge(STEPS, on='merge_col', how='inner')\
					  .merge(AGE_GROUPS, on='merge_col', how='inner')
					  
	m = square.merge(df, how='inner')
	assert len(m) == len(square), \
	       'the dataset is not square or is missing some location'

if __name__ == '__main__':
	"""  MAIN  """
	
	# All DisMOD ages
	#ALL_AGES = range(2, 21) + range(30, 33) + [235]
	
	# necessary columns
	index_cols = ['age_group_id',
				  'sex_id',
				  'cause_id',
				  'year_id',
				  'location_id',
				  'step']
	group_cols = ['age_group_id',
				  'sex_id',
				  'location_id', 
				  'year_id', 
				  'step']
	
	# file paths
	in_path = "FILEPATH"
	out_path = "FILEPATH"
	
	# info on causes
	info_path = "FILEPATH"
	
	# outpath for data viz tools
	viz_path = "FILEPATH"
	
	# file path where HDFs will be saved for upload
	save_path = "FILEPATH"
	
	# file where uncorrected, subnational-location, proportion draws are saved and read in from
	draws_path = "FILEPATH"
	
	#### change the below to YOUR username ####
	user_name = raw_input("What's your username (for qsub errors/outputs? ")
	user_name= str(user_name)
	
	# qsub diagnostic file path
	qsub_output = "FILEPATH{}/HF_correct/output".format(user_name)
	qsub_errors = "FILEPATH{}/HF_correct/errors".format(user_name)
	
	print "qsub errors and outputs will be written to "
	print "   ", qsub_errors
	print "   ", qsub_output
	print "respectivily"
	#### change the above to YOUR username ####
	
	# Ask the user if they need new data entirely, for all locations
	#re_query = raw_input("Do you want to run apply the correction factors to all locations? ")
	re_query = "YES"
	re_query = str(re_query).upper()
	
	if re_query != "YES":
		# Ask the user if they need new data for admin0 locations, for trial purposes
		#admin0_only = raw_input("Do want to run apply the correction factors to only admin0 locations? ")
		admin0_only = "NO"
		admin0_only = str(admin0_only).upper()
	else:
		admin0_only = "NO"
		
	if re_query == "YES" or admin0_only == "YES":	
		# Ask the user which causes they want corrected
		#print "Option 1: IHD, HTN, all locations"
		#print "Option 2: IHD, HTN, all locations except SSA; IHD only in SSA"
		#print "Option 3: IHD, HTN, all locations except SSA; no correction in SSA"
		#print "Option 4: IHD, all locations except SSA; no correction in SSA"
		
		#test_all = raw_input("Do you want to test all options? ")
		test_all = "NO"
		test_all = str(test_all).upper()

	# what will be corrected
	if test_all != "YES":
		#option = raw_input("Which option do you want? ")
		option = 4
		option = str(option)
		
				# Ask the user if they want to upload
		print "After the program finishes, do you want: "
			
		# Ask the user if they want to write new HDF
		#write_hdf = raw_input("    to write a new HDF for each etiology? ")
		write_hdf = "YES"
		write_hdf = str(write_hdf).upper()
		
		# Ask the user if they want to use "save_results" to upload the HDFs
		#save_Results = raw_input("    to use save_results to upload the HDF for each etiology? ")
		save_Results = "YES"
		save_Results = str(save_Results).upper()
	else:
		option = 0
	
	# Ask the user if she/he wants to confirm that all draws files exist
	check_files = raw_input("Do want to confirm draw files exist for all locations? ")
	check_files = str(check_files).upper()
		
	# Ask the user if the want the prog to send a Slack when the program is done.
	send_Slack = raw_input("Do you want to Slack a person/channel when the prog finishes? ")
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
	
	if not os.path.exists(out_path):
		os.makedirs(out_path)
	if not os.path.exists(viz_path):
		os.makedirs(viz_path)
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
		
	# Make the file paths for draws
	FILE_PATHS = [out_path + 'diagnostics/',
				  out_path + 'prevalence/']
	
	for file_path in FILE_PATHS:
		if not os.path.exists(file_path):
			os.makedirs(file_path)
	
	# Make correction factors
	mktscan_draws = get_correction_factors(info_path,
	                                       in_path,
										   out_path,
										   draws_path)
		
	# Get age group, and cause- IDs, and names:
	AGE_NAMES = get_ids('age_group')[['age_group_id', 'age_group_name']]
	CAUSE_NAMES = get_ids('cause')[['cause_id', 'cause_name']]
	
	# get the locations
	locations_df = get_location_metadata(location_set_id=9)
	
	# filter out locations not used in in Epi and non-admin0 locations
	DEMOGRAPHICS = list(get_demographics(gbd_team='epi')['location_ids'])
	
	# If admin0 only is selected then only take those 
	if admin0_only == "YES":
		LOCATION_NAMES = locations_df.query('location_type_id == 2'.format((DEMOGRAPHICS)))[['location_id', 'location_name']]
	else:
		LOCATION_NAMES = locations_df.query('location_id in {} or location_type_id == 2'.format((DEMOGRAPHICS)))[['location_id', 'location_name']]
	
	# location IDs
	LOCATION_IDS = LOCATION_NAMES.location_id.unique()
	
	super_regions = locations_df[['location_id','super_region_id']]
	ssa_df = super_regions.query('super_region_id == 166')
	SSA_LOCATIONS = ssa_df['location_id'].tolist()
	
	#### UNCOMMENT THE LINE BELOW FOR TESTING ####
	#LOCATION_IDS = [62, 180, 101, 6, 81, 93, 492, 102]
	#LOCATION_IDS = LOCATION_IDS[:5]
	#print LOCATION_IDS
	#### UNCOMMENT THE LINE ABOVE FOR TESTING ####
	
	# If user asks to confirm that draw files are present for ALL locations.
	if check_files == "YES":
		# Cause folders
		CAUSE_FOLDERS = ['myocarditis',
						 'anemia',
						 'iodine',
						 'htn',
						 'endo',
						 'cmp_other',
						 'thalass',
						 'congenital',
						 'other_anemia',
						 'pneum_other',
						 'alcoholic_cmp',
						 'copd',
						 'g6pd',
						 'valvu',
						 'ihd',
						 'other_combo',
						 'interstitial',
						 'rhd']	
		
		# break if file is missing
		check = True
					
		# loop through all nessary causes to confirm that a draw file exists
		for cause_folder in CAUSE_FOLDERS:
			cause_count = 0
			
			# Make local file path
			temp_path = "{}{}/".format(draws_path, cause_folder)
			if not os.path.exists(temp_path):
				check == False
				print "{} does not exist".format(temp_path)
				break 
			if cause_folder == "pneum_other":
				# pnuem_other has 4 causes with in it
				SUBCAUSE_FOLDERS = ['other', 
									'silicosis', 
									'coalworker', 
									'asbestosis']
				
				# for pneum_other loop through sub-causes
				for subcause_folder in SUBCAUSE_FOLDERS:
					# Make local file path
					inner_temp_path = "{}{}/".format(temp_path, subcause_folder)
					if not os.path.exists(inner_temp_path):
						check = False
						print "{} does not exist".format(inner_temp_path)
						break 
					# loop through all the locations to make sure they exist
					for location_id in LOCATION_IDS:
						csv_file = "{}5_{}.csv".format(inner_temp_path,
						                               location_id)				
						if not os.path.isfile(csv_file):
							check = False
							print "{} does not exist".format(csv_file)
							break 
				
				cause_count += 1
				print "		{} causes checked.".format(cause_count)
			
			else:
				# loop through all the locations to make sure they exist
				for location_id in LOCATION_IDS:
					csv_file = "{}5_{}.csv".format(temp_path, location_id)				
					if not os.path.isfile(csv_file):
						check = False
						print "{} does not exist".format(csv_file)
						break
				
				cause_count += 1
				print "		{} causes checked.".format(cause_count)
		
		# make sure all files exist
		if send_Slack == "YES" and not check:
			message = "Not all files exist"
		
		assert check, "Not all files exist"
	
	# Start parallelization
	if re_query == "YES" or admin0_only:
		location_count = 0
		
		#### For debugging ####
		need_draws = True
		#need_draws = False
		#### For debugging ####
		
		if need_draws:
		
			for location_id in LOCATION_IDS:
				if location_id in SSA_LOCATIONS:
					is_ssa = 1
				else:
					is_ssa = 0
					
				location_count += 1
				# get location name and convert to a string
					
				call = ('qsub -cwd -N "corrects_{location_id}" '
				        '-P proj_custom_models '
						'-o {qsub_output} '
						'-e {qsub_errors} '
						'-l mem_free=40G -pe multi_slot 10 cluster_shell.sh '
						'01_apply_corrections_subprocess.py '
						'{out_path} '
						'{in_path} '
						'{draws_path} '
						'{location_id} '
						'{is_ssa} '
						'{option} '
						'{test_all} '
						'{send_Slack} '
						'{token} '
						'{channel}'.format(qsub_output=str(qsub_output),
										   qsub_errors=str(qsub_errors),
										   out_path=str(out_path),
										   in_path=str(in_path),
										   draws_path=str(draws_path),
										   location_id=str(location_id),
										   is_ssa=str(is_ssa),
										   option=str(option),
										   test_all=str(test_all),
										   send_Slack=str(send_Slack),
										   token=str(token),
										   channel=str(channel)))	
				subprocess.call(call, shell=True)
				print "		results for {} locations being calculated.".format(location_count)
			wait('corrects', 100)
			location_count += 1
			print "		jobs for {} locations submitted.".format(location_count)
		
		
		
		# When the parallelization is all complete, append the result into a compiled	
		# dataset, with all the causes, locations, sexes, and age groups.
		location_count = 0
		corrections_list = []
		for location_id in LOCATION_IDS:
			location_count += 1
			try:
				df = pd.read_csv("{out_path}diagnostics/corr_prop_diagnostics_{location_id}.csv".format(out_path=out_path,
																										location_id=location_id))
				corrections_list.append(df)
			except:
				print "Missing location at {}".format(location_id)
				
			print "		results for {} locations appended.".format(location_count)
		corrections = pd.concat(corrections_list)
		corrections.reset_index(inplace=True)
		corrections.drop('index', axis=1, inplace=True)
		
		# get MarketScan data for diagnostic purposes
		mktscan = get_marketscan(mktscan_draws)
		
		# Make the MarketScan square
		mktscan = make_square_matrix(mktscan, corrections)
		mktscan = fill_missing(mktscan, index_cols)
		
		# append it to the main dataframe
		compare = corrections.append(mktscan)
		
		# merge locations, cause, and age group names.
		compare = compare.merge(LOCATION_NAMES, on='location_id', how='inner').reset_index()
		compare.drop('index', axis=1, inplace=True)
		compare['location_name'] = compare.apply(location_name_string, axis=1)
		
		compare = compare.merge(AGE_NAMES, on='age_group_id', how='inner').reset_index()
		compare.drop('index', axis=1, inplace=True)
		
		compare = compare.merge(CAUSE_NAMES, on='cause_id', how='inner').reset_index()
		compare.drop('index', axis=1, inplace=True)
		
		# round values to a reasonable number of digits, say 5
		for value_col in ('proportion', 'lower', 'upper', 'prevalence', 'lower_prev', 'upper_prev'): 
			compare[value_col] = compare[value_col].apply(round_numbers)
			
		# Make sure there aren't any duplicates
		assert not compare[index_cols].duplicated().any(), 'duplicates in the viz DF'
		
		# assert that the matrix is square
		assert_df_is_square(corrections)
		
		#exit()
		
		# save diagnostic CSV
		compare.to_csv("{}hf_corrected.csv".format(viz_path))
		
		# if user chooses to upload results
		if save_Results == "YES" or write_hdf == "YES":
			# file path for where draws are saved
			in_path = "FILEPATH"
			
			submit_save_results_jobs(write_hdf,
							         save_Results,
							         in_path,
							         save_path,
							         qsub_output,
							         qsub_errors,
							         send_Slack,
							         slack,
							         token, 
							         channel)


		
	if send_Slack == "YES":
		message = "The correction factors have been applied."
		slack.chat.post_message(channel, message)
		
		
