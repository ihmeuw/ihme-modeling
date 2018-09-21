
# custom/local libraries
from slacker import Slacker

# standard libraries
import os
import sys
import pandas as pd
import numpy as np
from itertools import izip

# remote libraries (used on the cluster)
from db_queries import get_ids
from transmogrifier.draw_ops import get_draws


def assert_df_is_square(df, send_Slack, slack, channel, location_id, dataset):
	"""
	Assert that the dataframe has all locations and is square on age, sex,
	and cause.
	
	Throws: AssertionError if not true
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

	square = LOCATIONS.merge(CAUSES, on='merge_col', how='inner')\
					  .merge(YEARS, on='merge_col', how='inner')\
					  .merge(SEXES, on='merge_col', how='inner')\
					  .merge(AGE_GROUPS, on='merge_col', how='inner')
					  
	m = square.merge(df, how='inner')
	
	if len(m) != len(square) and self.send_Slack == "YES":
		message = "{dataset} is not square for {location_id}".format(\
		                                                dataset=dataset,
				                                        location_id=location_id)
		slack.chat.post_message(channel, message)
	
	#assert len(m) == len(square),\
	#       'the dataset is not square or is missing some location'
	
def assert_env_df_is_square(df, send_Slack, slack, channel, location_id):
	"""
	Assert that the dataframe has all locations and is square on age, sex,
	and cause.
	
	Throws: AssertionError if not true
	"""
	AGE_GROUPS = df[['age_group_id']].drop_duplicates().reset_index()
	AGE_GROUPS['merge_col'] = 1

	SEXES = df[['sex_id']].drop_duplicates().reset_index()
	SEXES['merge_col'] = 1

	YEARS = df[['year_id']].drop_duplicates().reset_index()
	YEARS['merge_col'] = 1
	
	LOCATIONS = df[['location_id']].drop_duplicates().reset_index()
	LOCATIONS['merge_col'] = 1

	square = LOCATIONS.merge(YEARS, on='merge_col', how='inner')\
					  .merge(SEXES, on='merge_col', how='inner')\
					  .merge(AGE_GROUPS, on='merge_col', how='inner')
					  
	m = square.merge(df, how='inner')
	
	if len(m) != len(square) and send_Slack == "YES":
		message = "Envelope is not square for {location_id}".format(\
		                                                location_id=location_id)
		slack.chat.post_message(channel, message)
	
	#assert len(m) == len(square),\
	#       'the dataset is not square or is missing some location'
	
def assert_everything_is_square(df, send_Slack, slack, channel, location_id):
	"""
	Assert that the dataframe has all locations and is square on age, sex, 
	and cause.
	
	Throws: AssertionError if not true
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
	
	if len(m) != len(square) and send_Slack == "YES":
		message = "complete appended DataFrame is not square for {location_id}"\
		                                        .format(location_id=location_id)
		slack.chat.post_message(channel, message)
	
	#assert len(m) == len(square),\
	#       'the dataset is not square or is missing some location'

class Correction_Diagnostics:
	"""COrrection_Diagnostics"""
	
	def __init__(self,
	             in_path,
				 draws_path,
				 location_id,
				 is_ssa,
				 option,
				 test_all,
				 send_Slack,
				 slack,
				 channel):

		# initialized at time of instantiation
		self.AGE_GROUPS_IDS = range(2,21) + [30,31,32,235]
		self.group_cols = ['age_group_id', 
						   'sex_id',
						   'year_id',
						   'location_id']
		self.index_cols = ['age_group_id',
						   'sex_id',
						   'cause_id', 
						   'location_id',
						   'year_id']
		
		# Inputs -- Initialize input
		self.in_path = in_path
		self.location_id = location_id
		self.is_ssa = is_ssa
		self.option = option
		self.test_all = test_all
		self.draws_path = draws_path
		self.send_Slack = send_Slack
		self.slack = slack
		self.channel = channel
		
		# outputs
		self.fill_empty = None
		self.corrections = None
		self.outputs = None
		self.envelope = None
		self.uncorr_outputs = None
		self.corr_outputs = None
		self.corr_prev_draws = None
		self.compare = None
		
	def get_data(self):
		"""get_data"""
		corrections = pd.read_csv("{}corrections_draws.csv".format(\
		                                                          self.in_path))
		self.corrections = corrections
		
	def fill_missing(self):
		"""If data isn't present fill draws as 0"""
		AGE_GROUPS = self.corrections[['age_group_id']].drop_duplicates()\
		                                                          .reset_index()
		AGE_GROUPS['merge_col'] = 1
		AGE_GROUPS.drop('index', axis=1, inplace=True)

		SEXES = self.corrections[['sex_id']].drop_duplicates().reset_index()
		SEXES['merge_col'] = 1
		SEXES.drop('index', axis=1, inplace=True)

		YEARS = self.corrections[['year_id']].drop_duplicates().reset_index()
		YEARS['merge_col'] = 1
		YEARS.drop('index', axis=1, inplace=True)

		square = YEARS.merge(SEXES, on='merge_col', how='inner')\
					  .merge(AGE_GROUPS, on='merge_col', how='inner')
					  
		self.fill_empty = square

	def get_final_results(self):
		"""DESCRIPTION: Final model results from latest run"""
		
		print "get final estimates"
		
		# age groups used
		AGE_GROUPS = range(2,21) + [30, 31, 32, 235]
					
		# draw columns
		prop_cols = (['fe_prop_%s' % i for i in range(0, 1000)])
		
		# draw columns
		draw_cols = (['draw_%s' % i for i in range(0, 1000)])
		
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
		
		# etiology IDs
		CAUSE_IDS = [942, 390, 388, 498, 503, 944, 614, 643, 618, 888, 938, 509,
               		 616, 507, 493, 619, 516, 492]
		
		# get the data for all the causes for the given location
		outputs_list = []
		for cause_folder, cause_id in izip(CAUSE_FOLDERS, CAUSE_IDS):
			# Make local file path
			temp_path = "{}{}/".format(self.draws_path, cause_folder)
			if cause_folder == "pneum_other":
				# pnuem_other has 4 causes with in it
				SUBCAUSE_FOLDERS = ['other', 
									'silicosis', 
									'coalworker', 
									'asbestosis']
				
				# with these respective cause_ids
				SUBCAUSE_IDS = [514, 511, 513, 512]
				
				temp_list = []
				for subcause_folder, subcause_id in izip(SUBCAUSE_FOLDERS,
				                                         SUBCAUSE_IDS):
					# Make local file path
					inner_temp_path = "{}{}/".format(temp_path, subcause_folder)
					try:
						inner_temp = pd.read_csv("{}5_{}.csv".format(\
						                                      inner_temp_path,
															  self.location_id))
					except:
						if self.send_Slack == "YES":
							message = ("No file path available for {location_id} in {inner_temp_path}").format(location_id=self.location_id,
												                                                               inner_temp_path=inner_temp_path)
							self.slack.chat.post_message(self.channel, message)
						self.fill_missing()
						inner_temp = self.fill_empty.copy()
						inner_temp['location_id'] = self.location_id
						
						# fill draws with zero
						for i in xrange(1000):
							inner_temp['draw_'+str(i)] = 0
			
					# append a cause column
					inner_temp['cause_id'] = subcause_id
					
					# drop all unneeded columns
					inner_temp = inner_temp[self.index_cols + draw_cols]
					
					temp_list.append(inner_temp)
					temp = pd.concat(temp_list)
					temp.reset_index(inplace=True)
					
			else:
				try:
					temp = pd.read_csv("{}5_{}.csv".format(temp_path,
														   self.location_id))
				except:
					if self.send_Slack == "YES":
						message = ("No file path available for {location_id} in {temp_path}".format(location_id=self.location_id,
												                                                    temp_path=temp_path))
						self.slack.chat.post_message(self.channel, message)
					self.fill_missing()
					temp = self.fill_empty.copy()
					temp['location_id'] = self.location_id
					
					# fill draws with zero
					for i in xrange(1000):
						temp['draw_'+str(i)] = 0
					
				# append a cause column
				temp['cause_id'] = cause_id
				temp.fillna(0, inplace=True)
				
				# drop all unneeded columns
				temp = temp[self.index_cols + draw_cols]
			outputs_list.append(temp)
				
		outputs = pd.concat(outputs_list)
		outputs.reset_index(inplace=True)

		# filter out the unneeded ages 
		outputs = outputs.query('age_group_id in {}'.format((\
		                                                  self.AGE_GROUPS_IDS)))
	
		for i in xrange(1000):
			outputs.rename(columns={'draw_'+str(i):'fe_prop_'+str(i)},
			               inplace=True)
		
		# keep only columns needed and save to a CSV
		outputs = outputs[self.index_cols + 
						  prop_cols]
		
		# Make sure the matrix is square
		assert_df_is_square(outputs,
		                    self.send_Slack,
							self.slack,
							self.channel,
							self.location_id,
							"outputs")
		
		# Make sure there aren't any duplicates
		if outputs[self.index_cols].duplicated().any() and \
		   self.send_Slack == "YES":
			message = "outputs has duplicates for {location_id}".format(\
			                                            location_id=location_id)
			self.slack.chat.post_message(self.channel, message)
			
		#assert not outputs[self.index_cols].duplicated().any(),\
		#	'duplicates introduced in custom cause generation'
		
		##np.allclose(np.isclose(corr_outputs.groupby(self.group_cols + ['year_id'])[\
		##						fe_prop_cols].sum(), 1.0, atol=0.01), True), "proportions don't sum to 1"
		
		#if not np.allclose(outputs.groupby(self.group_cols)[prop_cols].sum(), 1.0) and send_Slack == "YES":
		if not np.allclose(np.isclose(outputs.groupby(self.group_cols)[\
								prop_cols].sum(), 1.0, atol=0.03), True) and self.send_Slack == "YES":
			message = "Proportions don't sum to 1 for {location_id}".format(location_id=location_id)
			self.slack.chat.post_message(self.channel, message)
			
		#assert np.allclose(outputs.groupby(self.group_cols)[prop_cols].sum(), 1.0), "proportions don't sum to 1"
						   
		self.outputs = outputs
							   
	def get_envelope(self):
		"""get the envelope"""
		
		print "get envelope"
		
		# columns
		draw_cols = (['draw_%s' % i for i in range(0, 1000)])
		env_cols = (['env_prev_%s' % i for i in range(0, 1000)])
		hf_cols = (['hf_prev_%s' % i for i in range(0, 1000)])
		
		# get overall prevalence of heart failure
		hf = get_draws('modelable_entity_id',
					   2412,
					   'dismod',
					   sex_ids=[1,2],
					   status="best",
					   measure_ids=5,
					   location_ids=self.location_id,
					   gbd_round_id=4,
					   num_workers=5)
		
		# drop unneeded columns
		hf = hf[self.group_cols + draw_cols]
		
		# drop unneeded age groups
		hf = hf.query('age_group_id in {}'.format((self.AGE_GROUPS_IDS)))
		
		# delete the prevalence due to Chagas
		chagas = get_draws('modelable_entity_id',
						   2413,
						   'dismod',
						   sex_ids=[1,2],
						   status="best",
						   measure_ids=5,
						   location_ids=self.location_id,
						   gbd_round_id=4,
						   num_workers=5)
		
		# drop unneeded columns
		chagas = chagas[self.group_cols + draw_cols]
		
		# drop unneeded age groups
		chagas = chagas.query('age_group_id in {}'.format((self.AGE_GROUPS_IDS)))
		
		# make sure the size of HF and Chagas is the same
		if len(hf) != len(chagas) and self.send_Slack == "YES":
				message = "Chagas and HF envelople have different number of rows for {location_id}"\
							.format(location_id=location_id)
				self.slack.chat.post_message(self.channel, message)
		
		#assert len(hf) == len(chagas), "matrices are not the same size."
		
		# rename HF prev draws
		for i in xrange(1000):
			hf.rename(columns={'draw_'+str(i):'env_prev_'+str(i)}, inplace=True)
			
		# merge the HF-chagas prev draws and the HF prev draws
		hf = hf.merge(chagas, on=self.group_cols, how='inner')
			
		# subtract prevalence of HF due to Chagas from total HF prevalence
		for i in xrange(1000):
			hf['nonchagas_prev_'+str(i)] = hf['env_prev_'+str(i)] - hf['draw_'+str(i)]

			hf.rename(columns={'nonchagas_prev_'+str(i):'hf_prev_'+str(i)}, inplace=True)
		
		# drop unneeded columns
		hf = hf[self.group_cols + hf_cols]
		
		hf[hf < 0] = 0
		
		# Make sure the Matrix is square
		assert_env_df_is_square(hf, self.send_Slack, self.slack, self.channel, self.location_id)
		
		# Make sure there aren't any duplicates
		if hf[self.group_cols].duplicated().any() and self.send_Slack == "YES":
			message = "The Chagas deleted HF envelope has duplicates for {location_id}"\
						.format(location_id=location_id)
			self.slack.chat.post_message(self.channel, message)
		#assert not hf[self.index_cols].duplicated().any(), 'duplicates introduced in custom cause generation'
		
		self.envelope = hf
		
	def get_uncorrected_outputs(self):
		"""Get uncorrected final estimates of HF etiology proportions"""
		
		print "get uncorrected prevalence and proportions"
		
		# draw columns
		prop_cols = (['fe_prop_%s' % i for i in range(0, 1000)])
		prev_cols = (['fe_prev_%s' % i for i in range(0, 1000)])
		
		# Uncorrected Outputs
		uncorr_outputs = self.outputs[self.index_cols +
							          prop_cols].copy()
		
		uncorr_outputs = uncorr_outputs.merge(self.envelope, on=self.group_cols, how='inner')
		
		# calculate prevalence
		for i in xrange(1000):
			uncorr_outputs['fe_prev_'+str(i)] = uncorr_outputs['hf_prev_'+str(i)] * uncorr_outputs['fe_prop_'+str(i)]
		
		# Compute 25 and 75 percentiles of proproportion distribution for each row
		prop_stats = uncorr_outputs[prop_cols].transpose().describe(
						percentiles=[.25, .75]).transpose()[['mean', '25%', '75%']]		
		prop_stats.rename(
				columns={'mean':'proportion','25%': 'lower', '75%': 'upper'}, inplace=True)	
				
		# Compute 25 and 75 percentiles of prevalence distribution for each row
		prev_stats = uncorr_outputs[prev_cols].transpose().describe(
						percentiles=[.25, .75]).transpose()[['mean', '25%', '75%']]		
		prev_stats.rename(
				columns={'mean':'prevalence','25%': 'lower_prev', '75%': 'upper_prev'}, inplace=True)

		# add these percentiles to the dataset
		# for proportion:
		uncorr_outputs['proportion'] = prop_stats['proportion']
		uncorr_outputs['lower'] = prop_stats['lower']
		uncorr_outputs['upper'] = prop_stats['upper']
		
		# for prevalence:
		uncorr_outputs['prevalence'] = prev_stats['prevalence']
		uncorr_outputs['lower_prev'] = prev_stats['lower_prev']
		uncorr_outputs['upper_prev'] = prev_stats['upper_prev']
		
		uncorr_outputs['step'] = "Uncorrected final estimates"
		
		# Make sure the matrix is square
		assert_df_is_square(uncorr_outputs, self.send_Slack, self.slack, self.channel, self.location_id, "uncorr_outputs")
		
		# Make sure there aren't any duplicates
		if uncorr_outputs[self.index_cols + ['step']].duplicated().any() \
		   and self.send_Slack == "YES":
			message = "The Uncorrected proportion df has duplicates for {location_id}"\
						.format(location_id=location_id)
			self.slack.chat.post_message(self.channel, message)
		
		#assert not uncorr_outputs[self.index_cols + ['step']].duplicated().any(),\
		#	'duplicates introduced in custom cause generation'
		
		
		# Keep only necessary columns
		self.uncorr_outputs = uncorr_outputs[self.index_cols + ['proportion', 'lower', 'upper',
																'prevalence', 'lower_prev', 'upper_prev',
																'step']]
																
	def test_all_options(self):
		""""Apply the corrections only to IHD and let everything else fight it out."""
		
		print "Test all options"
		
		# merge columns
		local_index_cols = ['age_group_id', 'sex_id', 'cause_id', 'year_id']
		
		# correction factors
		ratio_cols = (['ratio_%s' % i for i in range(0, 1000)])
		raw_prop_cols = (['raw_prop_%s' % i for i in range(0, 1000)])
		adj_prop_cols = (['adj_prop_%s' % i for i in range(0, 1000)])
		adj_prev_cols = (['adj_prev_%s' % i for i in range(0, 1000)])
		draw_cols = (['draw_%s' % i for i in range(0, 1000)])
		
		# correction factors
		factors = self.corrections[local_index_cols + ratio_cols].copy()

		# corrected ouputs
		corr_outputs = factors.merge(self.outputs, on=local_index_cols, how='right')
		corr_outputs.fillna(1, inplace=True)		
													
		# Apply corrections
		# Make copies for each option
		option_1 = corr_outputs.copy()
		option_2 = corr_outputs.copy()
		option_3 = corr_outputs.copy()
		option_4 = corr_outputs.copy()
		
		for i in xrange(1000):
			# everything besides the CAUSES the user asked to correct is kept as it is for now.
			option_1['raw_prop_'+str(i)] = option_1['fe_prop_'+str(i)]
			option_2['raw_prop_'+str(i)] = option_2['fe_prop_'+str(i)]
			option_3['raw_prop_'+str(i)] = option_3['fe_prop_'+str(i)]
			option_4['raw_prop_'+str(i)] = option_4['fe_prop_'+str(i)]
			
			# option == 1:
			# For the causes to correct, we multiply the final estimate proportion by the correction factor. 
			option_1.loc[option_1['cause_id'].isin((493, 498)), 'raw_prop_'+str(i)] = \
					option_1.loc[option_1['cause_id'].isin((493, 498)), 'fe_prop_'+str(i)] * \
					option_1.loc[option_1['cause_id'].isin((493, 498)), 'ratio_'+str(i)]
			# option == 2:
			# For the causes to correct, we multiply the final estimate proportion by the correction factor.
			if self.is_ssa:
				option_2.loc[option_2['cause_id'] == 493, 'raw_prop_'+str(i)] = \
							option_2.loc[option_2['cause_id'] == 493, 'fe_prop_'+str(i)] * \
							option_2.loc[option_2['cause_id'] == 493, 'ratio_'+str(i)]
			
			else:
				option_2.loc[option_2['cause_id'].isin((493, 498)), 'raw_prop_'+str(i)] = \
					option_2.loc[option_2['cause_id'].isin((493, 498)), 'fe_prop_'+str(i)] * \
					option_2.loc[option_2['cause_id'].isin((493, 498)), 'ratio_'+str(i)]
			
				# option == 3:
				# For the causes to correct, we multiply the final estimate proportion by the correction factor. 
				option_3.loc[option_3['cause_id'].isin((493, 498)), 'raw_prop_'+str(i)] = \
					option_3.loc[option_3['cause_id'].isin((493, 498)), 'fe_prop_'+str(i)] * \
					option_3.loc[option_3['cause_id'].isin((493, 498)), 'ratio_'+str(i)]
					
				# option == 3:
				# For the causes to correct, we multiply the final estimate proportion by the correction factor. 
				option_4.loc[option_4['cause_id'] == 493, 'raw_prop_'+str(i)] = \
					option_4.loc[option_3['cause_id'] == 493, 'fe_prop_'+str(i)] * \
					option_4.loc[option_3['cause_id'] == 493, 'ratio_'+str(i)]
				
				
		
		# Make a Unique identifier for this set of dataset 
		option_1['step'] = "Option 1: IHD, HTN, all locations"
		option_2['step'] = "Option 2: IHD, HTN, all locations except SSA; IHD only in SSA"
		option_3['step'] = "Option 3: IHD, HTN, all locations except SSA; no correction in SSA"
		option_4['step'] = "Option 4: IHD only, all locations except SSA; no correction in SSA"
		
		# append the data for the 3 options
		corr_outputs = option_1.append(option_2).append(option_3).append(option_4)
		
		# rescale the proportions to add up to one.
		rescale = corr_outputs.groupby(self.group_cols + ['step'])[raw_prop_cols].sum()
		rescale.reset_index(inplace=True)
		
		for i in xrange(1000):
			rescale.rename(columns={'raw_prop_'+str(i):'total_'+str(i)}, inplace=True)
			
		# merge the totals with the original matrix
		corr_outputs = rescale.merge(corr_outputs, on=self.group_cols + ['step'], how='inner')
		
		# calculate the rescaled corrected props at the draw level
		for i in xrange(1000):
			corr_outputs['adj_prop_'+str(i)] = corr_outputs['raw_prop_'+str(i)] / corr_outputs['total_'+str(i)]
		
		# calculate the rescaled corrected prevalence at the draw level		
		# merge envelope with corrected proportions
		corr_outputs = corr_outputs.merge(self.envelope, on=self.group_cols, how='inner')
		
		# calculate the rescaled corrected prevalence at the draw level
		for i in xrange(1000):
			corr_outputs['adj_prev_'+str(i)] = corr_outputs['hf_prev_'+str(i)] * corr_outputs['adj_prop_'+str(i)]
		
		# Compute 25 and 75 percentiles of prevalence distribution for each row
		prev_stats = corr_outputs[adj_prev_cols].transpose().describe(
						percentiles=[.25, .75]).transpose()[['mean', '25%', '75%']]		
		prev_stats.rename(
				columns={'mean':'prevalence','25%': 'lower_prev', '75%': 'upper_prev'}, inplace=True)

		# Compute 25 and 75 percentiles of proproportion distribution for each row
		prop_stats = corr_outputs[adj_prop_cols].transpose().describe(
						percentiles=[.25, .75]).transpose()[['mean', '25%', '75%']]		
		prop_stats.rename(
				columns={'mean':'proportion','25%': 'lower', '75%': 'upper'}, inplace=True)

		# add these percentiles to the dataset
		# for proportion:
		corr_outputs['proportion'] = prop_stats['proportion']
		corr_outputs['lower'] = prop_stats['lower']
		corr_outputs['upper'] = prop_stats['upper']
		
		# for prevalence:
		corr_outputs['prevalence'] = prev_stats['prevalence']
		corr_outputs['lower_prev'] = prev_stats['lower_prev']
		corr_outputs['upper_prev'] = prev_stats['upper_prev']
		
		# Make sure the proportions sum up to 1
		if not np.allclose(corr_outputs.groupby(self.group_cols + ['step'])['proportion'].sum(), 1.0) and self.send_Slack == "YES":
			message = "The corrected proportion don't sum to 1 for {location_id}".format(location_id=location_id)
			self.slack.chat.post_message(channel, message)

		#assert np.allclose(corr_outputs.groupby(self.group_cols)['proportion'].sum(), 1.0), "proportions don't sum to 1"
		
		# Make sure there aren't any duplicates
		if corr_outputs[self.index_cols + ['step']].duplicated().any() and self.send_Slack == "YES":
			message = "There are duplicates in the corrected for {location_id}".format(location_id=location_id)
			self.slack.chat.post_message(channel, message)
		
		#assert not isch_outputs[self.index_cols + ['step']].duplicated().any(),\
		#	'duplicates introduced in custom cause generation'
		
		self.corr_outputs = corr_outputs[self.index_cols + ['proportion', 'lower', 'upper',
															'prevalence', 'lower_prev', 'upper_prev',
															'step']]
															
	def get_corrected_outputs(self):
		""""Apply the corrections only to IHD and let everything else fight it out."""
		
		print "get corrected prevalence and proportions for only"
		
		# merge columns
		local_index_cols = ['age_group_id', 'sex_id', 'cause_id', 'year_id']
		
		# correction factors
		ratio_cols = (['ratio_%s' % i for i in range(0, 1000)])
		raw_prop_cols = (['raw_prop_%s' % i for i in range(0, 1000)])
		adj_prop_cols = (['adj_prop_%s' % i for i in range(0, 1000)])
		adj_prev_cols = (['adj_prev_%s' % i for i in range(0, 1000)])
		draw_cols = (['draw_%s' % i for i in range(0, 1000)])
		
		# correction factors
		factors = self.corrections[local_index_cols + ratio_cols].copy()

		# corrected ouputs
		corr_outputs = factors.merge(self.outputs, on=local_index_cols, how='right')
		corr_outputs.fillna(1, inplace=True)		
													
		# Apply corrections
		for i in xrange(1000):
			# everything besides the CAUSES the user asked to correct is kept as it is for now.
			corr_outputs['raw_prop_'+str(i)] = corr_outputs['fe_prop_'+str(i)]
			
			if option == 1:
				# For the causes to correct, we multiply the final estimate proportion by the correction factor. 
				corr_outputs.loc[corr_outputs['cause_id'].isin((493)), 'raw_prop_'+str(i)] = \
						corr_outputs.loc[corr_outputs['cause_id'].isin((493)), 'fe_prop_'+str(i)] * \
						corr_outputs.loc[corr_outputs['cause_id'].isin((493)), 'ratio_'+str(i)]
				corr_outputs['step'] = "Option 1: IHD, HTN, all locations"

			if option == 2:
				# For the causes to correct, we multiply the final estimate proportion by the correction factor. 
				if self.is_ssa:
					corr_outputs.loc[corr_outputs['cause_id'] == 493, 'raw_prop_'+str(i)] = \
								corr_outputs.loc[corr_outputs['cause_id'] == 493, 'fe_prop_'+str(i)] * \
								corr_outputs.loc[corr_outputs['cause_id'] == 493, 'ratio_'+str(i)]
				
				else:
					corr_outputs.loc[corr_outputs['cause_id'].isin((493, 498)), 'raw_prop_'+str(i)] = \
						corr_outputs.loc[corr_outputs['cause_id'].isin((493, 498)), 'fe_prop_'+str(i)] * \
						corr_outputs.loc[corr_outputs['cause_id'].isin((493, 498)), 'ratio_'+str(i)]
				corr_outputs['step'] = "Option 2: IHD, HTN, all locations except SSA; IHD only in SSA"
		
			if option == 3:
				# For the causes to correct, we multiply the final estimate proportion by the correction factor. 
				if not self.is_ssa:
					corr_outputs.loc[corr_outputs['cause_id'].isin((493, 498)), 'raw_prop_'+str(i)] = \
						corr_outputs.loc[corr_outputs['cause_id'].isin((493, 498)), 'fe_prop_'+str(i)] * \
						corr_outputs.loc[corr_outputs['cause_id'].isin((493, 498)), 'ratio_'+str(i)]
				corr_outputs['step'] = "Option 3: IHD, HTN, all locations except SSA; no correction in SSA"
				
			if option == 4:
				# For the causes to correct, we multiply the final estimate proportion by the correction factor. 
				if not self.is_ssa:
					corr_outputs.loc[corr_outputs['cause_id'] == 493, 'raw_prop_'+str(i)] = \
						corr_outputs.loc[corr_outputs['cause_id'] == 493, 'fe_prop_'+str(i)] * \
						corr_outputs.loc[corr_outputs['cause_id'] == 493, 'ratio_'+str(i)]
				corr_outputs['step'] = "IHD only, all locations except SSA; no correction in SSA"
				
		# rescale the proportions to add up to one.
		rescale = corr_outputs.groupby(self.group_cols)[raw_prop_cols].sum()
		rescale.reset_index(inplace=True)
		
		for i in xrange(1000):
			rescale.rename(columns={'raw_prop_'+str(i):'total_'+str(i)}, inplace=True)
			
		# merge the totals with the original matrix
		corr_outputs = rescale.merge(corr_outputs, on=self.group_cols, how='inner')
		
		# calculate the rescaled corrected props at the draw level
		for i in xrange(1000):
			corr_outputs['adj_prop_'+str(i)] = corr_outputs['raw_prop_'+str(i)] / corr_outputs['total_'+str(i)]
		
		# calculate the rescaled corrected prevalence at the draw level		
		# merge envelope with corrected proportions
		corr_outputs = corr_outputs.merge(self.envelope, on=self.group_cols, how='inner')
		
		# calculate the rescaled corrected prevalence at the draw level
		for i in xrange(1000):
			corr_outputs['adj_prev_'+str(i)] = corr_outputs['hf_prev_'+str(i)] * corr_outputs['adj_prop_'+str(i)]
		
		# Make a copy of the prevalence draws so they can be used.
		corr_prev_draws = corr_outputs.copy()
		for i in xrange(1000):
			corr_prev_draws.rename(columns={'adj_prev_'+str(i):'draw_'+str(i)}, inplace=True)
		corr_prev_draws = corr_prev_draws[self.index_cols + draw_cols]
		
		# add some necessary columns
		corr_prev_draws['measure_id'] = 5
		
		# Compute 25 and 75 percentiles of prevalence distribution for each row
		prev_stats = corr_outputs[adj_prev_cols].transpose().describe(
						percentiles=[.25, .75]).transpose()[['mean', '25%', '75%']]		
		prev_stats.rename(
				columns={'mean':'prevalence','25%': 'lower_prev', '75%': 'upper_prev'}, inplace=True)

		# Compute 25 and 75 percentiles of proproportion distribution for each row
		prop_stats = corr_outputs[adj_prop_cols].transpose().describe(
						percentiles=[.25, .75]).transpose()[['mean', '25%', '75%']]		
		prop_stats.rename(
				columns={'mean':'proportion','25%': 'lower', '75%': 'upper'}, inplace=True)

		# add these percentiles to the dataset
		# for proportion:
		corr_outputs['proportion'] = prop_stats['proportion']
		corr_outputs['lower'] = prop_stats['lower']
		corr_outputs['upper'] = prop_stats['upper']
		
		# for prevalence:
		corr_outputs['prevalence'] = prev_stats['prevalence']
		corr_outputs['lower_prev'] = prev_stats['lower_prev']
		corr_outputs['upper_prev'] = prev_stats['upper_prev']
		
		# Make a Unique identifier for this set of dataset 
		#corr_outputs['step'] = "corrected certain causes"
		
		# Make sure the proportions sum up to 1
		if not np.allclose(corr_outputs.groupby(self.group_cols)['proportion'].sum(), 1.0) and self.send_Slack == "YES":
			message = "The corrected proportion don't sum to 1 for {location_id}".format(location_id=location_id)
			self.slack.chat.post_message(self.channel, message)

		#assert np.allclose(corr_outputs.groupby(self.group_cols)['proportion'].sum(), 1.0), "proportions don't sum to 1"
		
		# make sure that the matrix is square
		assert_df_is_square(corr_outputs, self.send_Slack, self.slack, self.channel, self.location_id, "corr_outputs")
		
		# Make sure there aren't any duplicates
		if corr_outputs[self.index_cols + ['step']].duplicated().any() and self.end_Slack == "YES":
			message = "There are duplicates in the corrected for {location_id}".format(location_id=location_id)
			self.slack.chat.post_message(self.channel, message)
		
		#assert not isch_outputs[self.index_cols + ['step']].duplicated().any(),\
		#	'duplicates introduced in custom cause generation'
		
		self.corr_prev_draws = corr_prev_draws
		self.corr_outputs = corr_outputs[self.index_cols + ['proportion', 'lower', 'upper',
															'prevalence', 'lower_prev', 'upper_prev',
															'step']]
		
	def append_all(self):
		"""Append all the relevant steps to get a clear picture of the process."""
		
		self.get_data()
		self.get_final_results()
		self.get_envelope()
		self.get_uncorrected_outputs()
		
		# If user chooses "test_all"
		if self.test_all == "YES":
			self.test_all_options()
		else:
			self.get_corrected_outputs()
			
		# Append everything
		compare = self.uncorr_outputs.append(self.corr_outputs)
				
		# Make sure everything is square
		assert_everything_is_square(compare, self.send_Slack, self.slack, self.channel, self.location_id)
		
		# Make sure there aren't any duplicates
		if compare[self.index_cols + ['step']].duplicated().any() and self.send_Slack == "YES":
			message = "There are duplicates in the compare-dataset Corrected for {location_id}"\
						.format(location_id=self.location_id)
			self.slack.chat.post_message(self.channel, message)
		
		#assert not compare[self.index_cols + ['step']].duplicated().any(), 'duplicates in the corrected DF'
		
		self.compare = compare


if __name__ == '__main__':
	""" MAIN """
	
	# Bring in aguments
	out_path, in_path, draws_path, location_id, is_ssa, option, test_all, send_Slack, token, channel = sys.argv[1:11]
	location_id = int(location_id)
	is_ssa = int(is_ssa)
	option = int(option)
	
	# Make the file paths for draws
	FILE_PATHS = [out_path + 'diagnostics/',
				  out_path + 'prevalence/']
	
	for file_path in FILE_PATHS:
		if not os.path.exists(file_path):
			os.makedirs(file_path)
	
	# verify Slack API token/channel
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
	
	# instantiate the class
	corrects = Correction_Diagnostics(in_path,
									  draws_path,
									  location_id,
									  is_ssa,
									  option,
									  test_all,
									  send_Slack,
									  slack,
									  channel)
	corrects.append_all()
	
	# Write the results to CSVs
	if test_all == "YES":
		corrects.compare.to_csv("{out_path}{file_name}_{location_id}.csv".format(out_path=out_path,
																				 file_name='diagnostics/corr_prop_diagnostics',
																				 location_id=location_id), index=False, encoding='utf-8')
	else:
		# the files to be written
		FILES = [corrects.compare,
				 corrects.corr_prev_draws]
		
		# what the files will be named
		FILE_NAMES = ['diagnostics/corr_prop_diagnostics',
					  'prevalence/corr_prev_draws']
																		  
		for _file, file_name in izip(FILES, FILE_NAMES):
			_file.to_csv("{out_path}{file_name}_{location_id}.csv".format(out_path=out_path,
																		  file_name=file_name,
																		  location_id=location_id), index=False, encoding='utf-8')

	
