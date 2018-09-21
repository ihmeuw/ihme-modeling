
# local/custom libraries
import submitter
from slacker import Slacker

# standard libraries
import os
import sys
import datetime
import subprocess
import pandas as pd
import unicodedata
from itertools import izip

# remote libraries
from db_queries import get_location_metadata, get_demographics, get_ids
from adding_machine import agg_locations as al

class UploadToME:
	@staticmethod
	def timestamp(time=datetime.datetime.now()):
		"""Return a timestamp for the current moment in the CoD format.

		Times are made in the timezone of the computer that called it
		(likely PST)

		This is the format:
			{year}_{month}_{day}_{hourminuteseconds}

		Example:
			April 2nd, 1992, 7:12:39 PM -->
				1992_04_02_191239

		Arguments:
			time: datetime.datetime
				DUSERts to now()

		Returns:
			String, the timestamp
		"""
		return '{:%Y_%m_%d_%H%M%S}'.format(time)
	
	@staticmethod
	def upload_to_me(save_dir,
	                 in_path,
					 cause_id,
					 cause_name,
					 me_id,
					 write_hdf,
					 save_Results,
					 send_Slack,
					 token,
					 channel):
		""" upload using save_results """

		# out file name
		out_file = "5_draws.h5"
		out_hdf = save_dir + out_file

		# table name
		h5_tablename = "draws"

		# get the time
		time_stamp = UploadToME.timestamp()

		# important columns
		save_cols = ['measure_id',
					 'location_id',
					 'year_id',
					 'age_group_id',
					 'sex_id']
					 
					 
		years = range(1990, 2015, 5) + [2016]
		sexes = [1, 2]
		
		# if the user asked to write an HDF for this etiology
		
		if write_hdf == "YES":
			# draw columns			 
			draw_cols = (['draw_%s' % i for i in range(0, 1000)])

			# get the locations
			locations_df = get_location_metadata(location_set_id=9)

			# filter out locations not used in in Epi and non-admin0 locations
			DEMOGRAPHICS = list(get_demographics(gbd_team='epi')['location_ids'])
			LOCATIONS = locations_df.query('location_id in {}'.format((DEMOGRAPHICS)))[['location_id']]

			# location IDs
			LOCATION_IDS = LOCATIONS.location_id.unique()

			location_count = 0
			results_list = []
			for location_id in LOCATION_IDS:
				location_count += 1
				try:
					df = pd.read_csv("{in_path}prevalence/corr_prev_draws_{location_id}.csv".format(in_path=in_path,
																										 location_id=location_id))
					# filter to the one etiology being saved
					temp = df.query('cause_id == {}'.format(cause_id)).copy()
					
					# drop all unnecessary columns
					temp = temp[save_cols + draw_cols]
					
					# append the new df to the rest
					results_list.append(temp)
				except:
					print "Missing location at {location_id} for {cause_id}".format(location_id=location_id,
																					cause_id=cause_id)
					if send_Slack == "YES":
						message = "Missing location at {}".format(location_id)
						slack.chat.post_message(channel, message)
					
				print "		results for {} locations appended.".format(location_count)
			results = pd.concat(results_list)
			results.reset_index(inplace=True)
			results.drop('index', axis=1, inplace=True)
			
			# Make sure there aren't any negative values (This will happen where Chagas is endemic)
			results[results < 0] = 0

			# save the results to a HDF
			results.to_hdf(out_hdf,
						   h5_tablename,
						   format='table',
						   data_columns=save_cols)
		
		#if the user asked to save the HDF for this etiology using "save_results"
		if save_Results == "YES":
			description = "heart failure due to {cause_name}; new upload {time_stamp}; corrected-IHD proportions.".format(cause_name=cause_name,
																														 time_stamp=time_stamp)
			al.save_custom_results(me_id,
							description,
							save_dir,
							years=years,
							sexes=sexes,
							mark_best="best",
							in_counts=False,
							env="prod",
							custom_file_pattern=out_file,
							h5_tablename=h5_tablename,
							gbd_round=2016)
	
if __name__ == '__main__':
	"""MAIN"""
	
	# Bring in args
	save_path, in_path, cause_id, write_hdf, save_Results, send_Slack, token, channel = sys.argv[1:9]
	
	# Make sure cause ID and ME ID are integer types.
	cause_id = int(cause_id)
	
	IDS = pd.read_csv("{}MEs_and_Etiologies.csv".format(in_path))
	cause_name = str(IDS.query('cause_id == {}'.format(cause_id))['cause_name'].unique()[0])
	me_id = int(IDS.query('cause_id == {}'.format(cause_id))['modelable_entity_id'].unique()[0])
	
	# verify slack token/channel
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
	
	# file path for specific etiology
	save_dir = save_path + "cause_{cause_id}_me_{me_id}/".format(cause_id=cause_id,
																 me_id=me_id)	
	if not os.path.exists(save_dir):
		os.makedirs(save_dir)
	
	# run upload code
	UploadToME.upload_to_me(save_dir,
						    in_path,
						    cause_id,
						    cause_name,
						    me_id,
						    write_hdf,
						    save_Results,
						    send_Slack,
						    token,
						    channel)
	
	if send_Slack == "YES":
		message = "Draws for cause ID {cause_id} were uploaded to ME ID {me_id}.".format(cause_id=cause_id,
																						 me_id=me_id)
		slack.chat.post_message(channel, message)
		
