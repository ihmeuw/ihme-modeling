

# standard libraries
import os
import pandas as pd
import numpy as np
from itertools import izip
import shutil

# remote libraries
from transmogrifier.draw_ops import get_draws

def assert_df_is_square(df):
	"""
	Assert that the dataframe has all locations and is square on age, sex,
	cause.
	
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

	square = YEARS.merge(CAUSES, on='merge_col', how='inner')\
					  .merge(SEXES, on='merge_col', how='inner')\
					  .merge(AGE_GROUPS, on='merge_col', how='inner')
					  
	m = square.merge(df, how='inner')
	assert len(m) == len(square), \
	       'the dataset is not square or is missing some location'

class Factors:
	"""
	Factors: This class produces has all the inworkings of making the
	correction factors.
	"""
	
	def __init__(self,
				 info_path,
				 in_path,
				 draws_path,
				 out_path,
				 index_cols = ['sex_id',
						       'age_group_id',
						       'cause_id'],
				 group_cols = ['sex_id',
						       'age_group_id']):

		# inputs
		self.index_cols = index_cols
		self.group_cols = group_cols
		
		# Initialize input
		self.info_path = info_path
		self.in_path = in_path
		self.draws_path = draws_path
		self.out_path = out_path
		
		# outputs
		self.corr_outputs = None
		self.mktscan = None
		self.outputs = None
		self.corr_factors = None
	
	def marketscan_data(self):
		"""
		Brings in the MarketScan data and produce the proportions.
		"""
		
		# Make sure to drop composite causes
		mktscan = pd.read_csv("{}MarketScan_cleaned.csv".format(self.in_path))
		mktscan = mktscan[self.index_cols + ['cases_wHF']]
		
		# drop all composite causes
		comp_causes = pd.read_csv("{}composite_cause_list.csv".format(\
		                                                        self.info_path))
		COMPOSITE_IDS = comp_causes.composite_id.unique()
		mktscan = mktscan.query('cause_id not in {}'.format(\
		                                                   list(COMPOSITE_IDS)))
		
		# STEP1: Aggregate HF counts for GBD 2016 data
		total_HF = mktscan.groupby(self.group_cols)[['cases_wHF']].sum()
		total_HF.reset_index(inplace=True)
		total_HF.rename(columns={'cases_wHF':'total_HF_cases'}, inplace=True)
		mktscan = mktscan.merge(total_HF, on=self.group_cols, how='inner')
		
		# Make the porportions
		mktscan['mk_prop'] = mktscan['cases_wHF'] /mktscan['total_HF_cases']
		
		mktscan = mktscan[self.index_cols + ['mk_prop']]
		
		assert np.allclose(mktscan.groupby(self.group_cols)\
			   [['mk_prop']].sum(), 1.0), "proportions don't sum to 1"
		
		self.mktscan = mktscan
	
	def output_data(self):
		"""
		Use retrieve draw files for each of the HF etiologies for the U.S.
		-- the uncorrected model estimates from DisMod that have been rescaled,
		and split into sub-causes.
		"""
		
		# age group IDs
		AGES = range(2,21) + [30,31,32,235]

		# draw columns
		draw_cols = (['draw_%s' % i for i in range(0, 1000)])
		prop_cols = (['fe_prop_%s' % i for i in range(0, 1000)])
		
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
		
		# get the U.S. data for all the causes
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
					inner_temp = pd.read_csv("{}5_102.csv".format(\
					                                           inner_temp_path))
					
					# drop all unneeded columns
					inner_temp = inner_temp[self.group_cols + 
					                        ['year_id'] + 
											draw_cols]
					
					# append a cause column
					inner_temp['cause_id'] = subcause_id
					temp_list.append(inner_temp)
					temp = pd.concat(temp_list)
					temp.reset_index(inplace=True)
					
			else:			
				temp = pd.read_csv("{}5_102.csv".format(temp_path))
				
				# drop all unneeded columns
				temp = temp[self.group_cols + 
				            ['year_id'] +
							draw_cols]
			
				# append a cause column
				temp['cause_id'] = cause_id
			outputs_list.append(temp)
				
		outputs = pd.concat(outputs_list)
		outputs.reset_index(inplace=True)
		
		# filter out the unneeded ages 
		outputs = outputs.query('age_group_id in {}'.format((AGES)))

		# rename draws as fe_prop (Final-estimate proportion)
		for i in xrange(1000):
			outputs.rename(columns={'draw_'+str(i):'fe_prop_'+str(i)},
			               inplace=True)
		
		outputs = outputs[self.index_cols + 
		                  ['year_id'] +
						  prop_cols]
						   
		#assert np.allclose(outputs.groupby(local_group_cols)\
		#				[prop_cols].sum(), 1.0), "proportions don't sum to 1"
						
		assert np.allclose(np.isclose(outputs.groupby(self.group_cols + \
		                                              ['year_id'])\
	           [prop_cols].sum(), 1.0, atol=0.03), True), \
			   "proportions don't sum to 1"
						   
		self.outputs = outputs
		
	def correction_factor(self):
		"""
		calculate the correction factors:
		* take MarketScan proportion made in "marketscan_data" function
		* take final estime proportion (1000 draws)
		* divide Makretscan proportion by final estimate draws
		* this produces 1000 Marketscan proportion draws
		"""
		self.marketscan_data()
		self.output_data()
		
		# proportion columns
		fe_prop_cols = (['fe_prop_%s' % i for i in range(0, 1000)])
		test_prop_cols = (['test_prop_%s' % i for i in range(0, 1000)])
		
		# ratio columns
		ratio_cols = (['ratio_%s' % i for i in range(0, 1000)])
		
		# get number of rows before, to test after merge
		size_1 = len(self.outputs)
		
		# merge the columns.
		corr_outputs = self.mktscan.merge(self.outputs,
		                                  on=self.index_cols,
										  how='right')
		corr_outputs.fillna(0, inplace=True)
		
		# get number of rows to test after merge
		size_2 = len(corr_outputs)
		
		# Make sure the dataset didn't change size after the merge.
		assert size_1 == size_2, "Size of matrix changed on the merge."
		
		# Make sure the MarketScan proportions still sum to 1		
		assert np.allclose(np.isclose(corr_outputs.groupby(self.group_cols + 
		                                                   ['year_id'])\
						   [['mk_prop']].sum(), 1.0, atol=0.01), True), \
			   "proportions don't sum to 1"
		
		# Make sure the final estimate proportions still add to 1
		assert np.allclose(np.isclose(corr_outputs.groupby(self.group_cols + 
		                                                   ['year_id'])\
								[fe_prop_cols].sum(), 1.0, atol=0.01), True), \
			   "proportions don't sum to 1"
		
		# MarketScan to outputs difference in proportion
		for i in xrange(1000):
			corr_outputs['ratio_'+str(i)] = corr_outputs['mk_prop'] / \
			                                corr_outputs['fe_prop_'+str(i)]		
		
			# account for all the 0/0 cases
			# 	for final estimates:
			corr_outputs.loc[corr_outputs['fe_prop_'+str(i)] <= \
			                 10e-4, 'ratio_'+str(i)] = 1
			
			# 	for MarketScan:
			corr_outputs.loc[corr_outputs['mk_prop'] == 0, 'ratio_'+str(i)] = 1
			
			# if ratio is greater than 50 set it to zero
			corr_outputs.loc[corr_outputs['ratio_'+str(i)] >= \
			                 50, 'ratio_'+str(i)] = 1
		
		# Make sure the matrix is square
		assert_df_is_square(corr_outputs)
		
		# Make sure there aren't any duplicates
		assert not corr_outputs[self.index_cols +
                        		['year_id']].duplicated().any(), \
			   'duplicates introduced in custom cause generation'
		
		# Make sure the proportions are reversable					
		test_outputs = corr_outputs.copy()
		
		for i in xrange(1000):
			test_outputs['test_prop_'+str(i)] = test_outputs['ratio_'+str(i)] *\
                 			                    test_outputs['fe_prop_'+str(i)]
			
		# Make sure proportions add up to one
		rescale = test_outputs.groupby(self.group_cols + ['year_id'])\
		          [test_prop_cols].sum()
		rescale.reset_index(inplace=True)
		for i in xrange(1000):
			rescale.rename(columns={'test_prop_'+str(i):'total_'+str(i)},
			               inplace=True)
		
		# merge totals to original matrix to add up to one
		test_outputs = test_outputs.merge(rescale,
		                                  on=self.group_cols + ['year_id'],
										  how='inner')
		
		# rescale
		for i in xrange(1000):
			test_outputs['test_prop_'+str(i)] = \
			                                test_outputs['test_prop_'+str(i)]/ \
											test_outputs['total_'+str(i)]
			
		test_outputs = test_outputs[self.index_cols + \
		                            ['year_id'] + \
									test_prop_cols]
		
		
		#### IF THERE IS A ROUNDING PROBLEM SWITCH THE BELOW ####
		
		assert np.allclose(test_outputs.groupby(self.group_cols + ['year_id'])\
		       [test_prop_cols].sum(), 1.0), "proportions don't sum to 1"
		#assert np.allclose(np.isclose(test_outputs.groupby(group_cols)[\
		#						test_prop_cols].sum(), 1.0, atol=0.01), True), \
		#       "proportions don't sum to 1"
		
		#### IF THERE IS A ROUNDING PROBLEM SWITCH THE ABOVE ####
		
		# Make sure the number of rows didn't change
		size_3 = len(test_outputs)
		
		assert size_1 == size_2, "number of rows changed."
		
		# Compute 25 and 75 percentiles of ratios distribution for each row
		factors = corr_outputs.copy()
		
		prop_stats = factors[fe_prop_cols].transpose().describe(
						percentiles=[.25, .75]).transpose()\
						[['mean', '25%', '75%']]		
		prop_stats.rename(
				columns={'mean':'mean_fe_prop',
						 '25%': 'lower_fe_prop',
						 '75%': 'upper_fe_prop'}, inplace=True)
		
		ratio_stats = factors[ratio_cols].transpose().describe(
						percentiles=[.25, .75]).transpose()\
						[['mean', '25%', '75%']]		
		ratio_stats.rename(
				columns={'mean':'mean_ratio',
				         '25%': 'lower_ratio',
						 '75%': 'upper_ratio'}, inplace=True)
		
		factors['mean_fe_prop'] = prop_stats['mean_fe_prop']
		
		factors['mean_ratio'] = ratio_stats['mean_ratio']
		factors['lower_ratio'] = ratio_stats['lower_ratio']
		factors['upper_ratio'] = ratio_stats['upper_ratio']
		
		self.corr_factors = factors[self.index_cols +
		                            ['year_id'] + 
							        ['mk_prop', 
							         'mean_fe_prop',
								     'mean_ratio',
								     'lower_ratio',
								     'upper_ratio']]
								
		self.corrections = corr_outputs[self.index_cols + 
		                                ['year_id'] + 
										ratio_cols]
	
if __name__ == '__main__':
	""" MAIN """
	
	# necessary files 
	info_path = "FILEPATH"
	in_path = "FILEPATH"
	draws_path = "FILEPATH"
	
	# output file-path 
	out_path = "FILEPATH"
	
	if not os.path.exists(out_path):
		os.makedirs(out_path)
	
	# Instantiate the process class
	factors = Factors(info_path, in_path, draws_path, out_path)
	factors.correction_factor()
	
	# files to save
	FILES = [factors.corrections, factors.mktscan]
	FILE_NAMES = ["corrections", "marketscan"]
	for _file, file_name in izip(FILES, FILE_NAMES):
		_file.to_csv("{out_path}{file_name}_draws.csv".format(\
		                                                   out_path=out_path, 
														   file_name=file_name),
														   index=False,
														   encoding='utf-8')
															  
	factors.corr_factors.to_csv("FILEPATH/HF_factors.csv")
	
	