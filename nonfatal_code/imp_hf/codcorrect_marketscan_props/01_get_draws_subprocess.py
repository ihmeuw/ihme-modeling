
"""
This script is a sub-process of '00_prep_hf_mktscan_parallel.py', it takes a 
cause and a location as arguments. It is used to get CODcorrect outputs or 
all 700+ locations in parallel for one cause at a time.
"""

# local/custom
from slacker import Slacker
from hf_functions import make_square_matrix

# standard
import pandas as pd
import sys
import time

# remote env
from transmogrifier.draw_ops import get_draws

def draw_deaths(cause_id, location_id, out_path, send_Slack, slack, channel):
	"""
	draw_deaths: For a given location and Cause grab death estimates at the
	draw-level. Collapse it mean, upper, and lower.
	"""
	
	# year IDs
	YEAR_IDS = range(1990, 2017)
		
	# All ages that could be used
	ALL_AGES = range(2, 22) + [28] + range(30, 33) + [235]

	# Early and late ages of DisMod
	DISMOD_AGES = [2, 3, 4, 30, 31, 32, 235]

	# DisMod old ages
	OLD_DISMOD_AGES = [30, 31, 32, 235]

	# DisMod young ages
	YOUNG_DISMOD_AGES = [2, 3, 4]

	# Ages
	AGES = range(2, 5) + range(30, 34)

	# old ages
	OLD_AGES = range(30, 34)

	# young ages
	YOUNG_AGES = range(2, 5)

	# necessary columns
	group_cols = ['cause_id', 'sex_id', 'age_group_id', 'location_id']
	measure_cols = ['mean_death', 'upper_death', 'lower_death']

	# Draw columns
	draw_cols = (['draw_%s' % i for i in range(0, 1000)])
	hf_death_cols = (['hf_deaths_%s' % i for i in range(0, 1000)])

	# split <1 age group into early neonatal, late neonatal, and post neonatal
	young_age_groups = pd.DataFrame({'age_group_id':YOUNG_DISMOD_AGES,
									 'temp_id':1})

	# split 80+ age group into early neonatal, late neonatal, and post neonatal
	old_age_groups = pd.DataFrame({'age_group_id':OLD_DISMOD_AGES, 
								   'temp_id':1})

	# Query draws for the cause/location combo
	print cause_id, location_id
	try:
		DF = get_draws('cause_id', 
					   cause_id,
					   'codcorrect',
					   location_ids=location_id,
					   age_group_ids=ALL_AGES,
					   gbd_round_id=4,
					   #status="best",
					   status="latest",
					   #version_id=64,
					   sexes=[1,2],
					   location_set_id=35,
					   measure_ids=1,
					   numworkers=5)
		print DF['output_version_id'].unique(), " ", DF.shape
		
		DF = DF.query('year_id in {}'.format(YEAR_IDS))
		DF = DF.query('output_version_id == {}'.format(64))
		DF.fillna(0, inplace=True)
	except:
		if send_Slack == "YES":
			message = ("get_draws query FAILED for location_id={location_id} "
			           "and cause_id={cause_id}").format(\
					                                    location_id=location_id,
														cause_id=cause_id)
			slack.chat.post_message(channel, message)
		print message
			
	if send_Slack == "YES" and not len(DF):
		message = "Missing data for get_draws for location_id={location_id}" +\
				  " and cause_id={cause_id}".format(location_id=location_id,
													cause_id=cause_id)
		slack.chat.post_message(channel, message)

	# Compute 25 and 75 percentiles of the distribution for each row					
	stats = DF[draw_cols].transpose().describe(
			percentiles=[.25, .75]).transpose()[['mean', '25%', '75%']]		
	stats.rename(
			columns={'mean':'mean_death',
			         '25%': 'lower_death',
					 '75%': 'upper_death'}, inplace=True)

	# add these percentiles to the dataset		
	DF['mean_death'] = stats['mean_death']
	DF['lower_death'] = stats['lower_death']
	DF['upper_death'] = stats['upper_death']

	# To make this script robust filter by what ever the age groups are 
	# available if age group ID 235 is available go with that otherwise find 
	# another way to get/make an 80+ age group
	if pd.Series(DISMOD_AGES).isin(DF.age_group_id.unique()).all():
		ages = range(2,21) + [30, 31, 32, 235]
		DF = DF.query('age_group_id in {}'.format(ages))
		print "DISMOD_AGES"
		
	elif pd.Series(YOUNG_DISMOD_AGES).isin(DF.age_group_id.unique()).all() and \
		 not pd.Series(OLD_DISMOD_AGES).isin(DF.age_group_id.unique()).all():
		ages = range(2, 21) + range(30, 34)
		DF = DF.query('age_group_id in {}'.format(ages))
		DF['age_group_id'].replace(to_replace=33, value=235, inplace=True)
		print "YOUNG_DISMOD_AGES"
		
	elif pd.Series(OLD_DISMOD_AGES).isin(DF.age_group_id.unique()).all() and \
		not pd.Series(YOUNG_DISMOD_AGES).isin(DF.age_group_id.unique()).all():
		ages = range(5, 21) + [28] + [30, 31, 32, 235]
		DF = DF.query('age_group_id in {}'.format(ages))
		print "OLD_DISMOD_AGES"
		
	elif pd.Series(AGES).isin(DF.age_group_id.unique()).all():
		ages = range(2, 21) + range(30, 34)
		DF = DF.query('age_group_id in {}'.format(ages))
		print "AGES"
		
	elif pd.Series(OLD_AGES).isin(DF.age_group_id.unique()).all() and \
		not pd.Series(YOUNG_AGES).isin(DF.age_group_id.unique()).all():
		ages = range(5, 21) + [28] + range(30, 34)
		DF = DF.query('age_group_id in {}'.format(ages))
		print "OLD_AGES"

	elif pd.Series(YOUNG_AGES).isin(DF.age_group_id.unique()).all() and \
		not pd.Series(OLD_AGES).isin(DF.age_group_id.unique()).all():
		ages = range(2, 22)
		DF = DF.query('age_group_id in {}'.format(ages))
		print "YOUNG_AGES"

	else:
		ages = range(5, 22) + [28]
		DF = DF.query('age_group_id in {}'.format(ages))
		"A lot is missing"

	# split out neotals age groups if needed
	if pd.Series([28]).isin(DF.age_group_id.unique()).all():
		temp = DF.query('age_group_id == 28').copy()
		DF = DF.query('age_group_id != 28')
		
		# Make a temporary id to merge with age groups DataFrame
		temp.drop('age_group_id', axis=1, inplace=True)
		temp['temp_id'] = 1
		temp = temp.merge(young_age_groups, on='temp_id', how='inner')
		temp.drop('temp_id', axis=1, inplace=True)

		# Append the new df w/ age groups to the original
		# (excluding the <1 age composite).
		DF = DF.append(temp)

	# split out 80+ age groups if needed
	if pd.Series([21]).isin(DF.age_group_id.unique()).all():
		temp = DF.query('age_group_id == 21').copy()
		DF = DF.query('age_group_id != 21')
		
		# Make a temporary id to merge with age groups DataFrame
		temp.drop('age_group_id', axis=1, inplace=True)
		temp['temp_id'] = 1
		temp = temp.merge(old_age_groups, on='temp_id', how='inner')
		temp.drop('temp_id', axis=1, inplace=True)
		
		# Append the new df w/ age groups to the original
		# (excluding the 80+ age composite).
		DF = DF.append(temp)

	# The columns that are added up
	DF = DF.groupby(group_cols)[measure_cols].sum().reset_index()
	
	# fill in any missing data
	DF = make_square_matrix(DF)
	
	# Save it to a CSV on the cluster to be read back into the main script.					
	DF.to_csv('{out_path}/codcorrect_{cause}_{location}.csv'.format(\
			  out_path=out_path,
			  cause=cause_id,
			  location=location_id),
			  index=False, encoding='utf-8')
														 
if __name__ == '__main__':
	""" MAIN """
	start = time.time()
	
	# bring in args
	cause_id, \
	location_id, \
	out_path, \
	send_Slack, \
	token, \
	channel = sys.argv[1:7]
	
	# convert strings to ints
	cause_id = int(cause_id)
	location_id = int(location_id)
	
	if send_Slack == "YES":
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
	
	# convert strings to ints
	cause_id = int(cause_id)
	location_id = int(location_id)
	
	# query death data
	draw_deaths(cause_id,
				location_id,
				out_path,
				send_Slack,
				slack,
				channel)
	
	end = time.time()
	
	print end - start


