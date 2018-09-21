
# local/custom libraries
from hf_functions import wait, make_square_matrix, make_custom_causes

# standard libraries
import subprocess
import submitter
import pandas as pd
import numpy as np
import os
import sys
import shutil

# remote libraries
from db_tools.ezfuncs import query
from transmogrifier.draw_ops import get_draws
from db_queries import get_location_metadata
			
class MarketScan:
	"""MarketScan"""
	def __init__(self,
				 in_path,
				 out_path,
				 metadata_path,
				 group_cols,
				 measure_cols,
				 AGE_RESTRICTED_CAUSES,
				 comp_causes,
				 success_cases_col,
				 sample_size_col,
				 ndraws=1000,
				 quantile=0.975,
				 collapse=False,
				 seed=1325):

		# inputs
		self.in_path = in_path
		self.out_path = out_path
		self.metadata_path = metadata_path
		self.group_cols = group_cols
		self.measure_cols = measure_cols
		self.AGE_RESTRICTED_CAUSES = AGE_RESTRICTED_CAUSES
		self.comp_causes = comp_causes
		self.success_cases_col = success_cases_col
		self.sample_size_col = sample_size_col
		self.ndraws = ndraws
		self.collapse = collapse
		self.seed = seed
		self.quantile = quantile
		
		# outputs
		self.ages = None
		self.cause_ids = None # Not including custom causes
		self.mktscan = None
	
	def fetch_ages(self):
		"""Fetch relevant ages from the shared.age_group table"""
		q = '''
			SELECT age_group_years_start AS age_start, age_group_id 
			FROM shared.age_group 
			WHERE (age_group_id BETWEEN 5 AND 20) OR
			      (age_group_id BETWEEN 30 AND 32) OR
				  age_group_id IN (28, 235)
			ORDER BY age_start
		'''
		self.ages = query(q, conn_def="shared")
		
	def prep_marketscan(self):
		"""Fetch and clean the marketscan data.
		
		Arguments:
			none
			
		Requires:
			global WORK_DIR has been set.
			function fetch_ages()
			
		Returns:
			pandas dataframe like so:
			[key columns] : [value columns]
			[cause_id, age_group_id, sex_id] : [acause, cause_name, prop_hf_mkt]
		"""
		
		# split <1 age group into early neonatal, late neonatal, and post 
		# neonatal
		young_age_groups = pd.DataFrame({'age_group_id':[2, 3, 4],
		                                 'temp_id':1})
		
		# read the mkt scan data
		df = pd.read_stata("{}all_hf_ratios_gbd2016_v2_with_sample_size.dta"\
		                   .format(self.in_path))
		
		# Make sure everything is the correct type
		df = df.astype(dtype={'sex':'int64',
							  'HFcategory':'object',
							  'cases':'float64',
							  'cases_wHF':'float64',
							  'age_start':'float64',
							  'age_end':'float64',
							  'sample_size':'float64'})
		
		# rename the column for consistency and for the merge
		df.rename(columns={'HFcategory':'hfcategory'}, inplace=True)
		
		# drop any nulls or blanks in the dataset
		df = df.query('hfcategory != ""').reset_index()
		df.drop('index', axis=1, inplace=True)
		
		# if cases < 10, then set cases_wHF to 0 so that the proportion will be
		# zero
		df.loc[df['cases'] < 10, 'cases_wHF'] = 0
		
		# map the hfcategory names to acause
		map_hfcat_acause = pd.read_csv(("{}/marketscan/map_hfcategory_acause"
		                                "_gbd2016.csv").format(\
										                    self.metadata_path))
		
		# spaces in some hfcategory labels cause merge to fail
		map_hfcat_acause['hfcategory'] = map_hfcat_acause.hfcategory.replace(
											          to_replace='myocarditis ',
											          value='myocarditis')
		map_hfcat_acause['hfcategory'] = map_hfcat_acause.hfcategory.replace(
											 to_replace='other cardiomyopathy ',
											 value='other cardiomyopathy')

		df = df.merge(map_hfcat_acause.dropna(), how='left')

		assert df.acause.notnull().values.all(), 'merge with causes failed'
		
		# get acauses
		acauses = str(tuple(df.acause.unique()))
		
		# now use that mapping to map to cause_id
		q = '''SELECT cause_id, acause, cause_name
			   FROM shared.cause
			   WHERE acause in {};'''.format(acauses)
		causes = query(q, conn_def="shared")
		df = df.merge(causes, how='left')

		assert df.cause_id.notnull().values.all(), \
		       'merge with cause hierarchy failed'
		
		# certain causes have will have age cutoffs of 30, where all proportions
		# below 30 will be set to 0.
		# set cases_wHF to 0 so that the proportion will be zero.
		df.loc[(df['cause_id'].isin(self.AGE_RESTRICTED_CAUSES))&\
		       (df['age_start'] < 30), 'cases_wHF'] = 0
		
		# replace age_start and age_end with age group id
		# 0-4 is really 0-1
		# this fix is only for clarity, doesnt affect
		# anything as age_start is right
		self.fetch_ages()
		df.ix[(df['age_start']==0) & (df['age_end']==4), 'age_end'] = 1
		df = df.merge(self.ages, on='age_start', how='left')

		assert df.age_group_id.notnull().values.all(), 'merge with ages failed'
		
		# rename sex
		df['sex_id'] = df['sex']
				 
		# take the <1 age group and split it out into neonatals
		temp = df.query('age_group_id == 28').copy()
		df = df.query('age_group_id != 28')
		
		# Make a temporary id to merge with age groups DataFrame
		temp.drop('age_group_id', axis=1, inplace=True)
		temp['temp_id'] = 1
		temp = temp.merge(young_age_groups, on='temp_id', how='inner')
		temp.drop('temp_id', axis=1, inplace=True)
		
		# Append the new df w/ age groups to the original (excluding the <1 age
		# composite).
		df = df.append(temp)
		
		# write MarketScan to CSV to be used for correction factors
		df.to_csv("{}MarketScan_cleaned.csv".format(self.out_path))
		
		self.mktscan = df[self.group_cols + ['cause_id'] + self.measure_cols]
		
		# get unique cause IDs
		self.cause_ids = df.cause_id.unique()

	def append_beta_draws_to_df(self):
		"""Return a pandas dataframe with samples from a binomial distribution.
		
		Uses numpy's random.binomial package, which takes n=number of trials,
		and
		p=probability of success.
			p = df[success_case_col] / df[sample_size_col]
			n = df[sample_size_col]
		Calculates ndraws samples from this distribution
		for each row in the dataframe, then adds either ndraws
		columns with each sample from the distribution, or if collapse = True,
		adds the mean, 75th percentile, and 25th percentile as columns.
		
		Arguments:
			df: pandas dataframe
			success_cases_col: must be a numeric column in df
			sample_size_col: must be a numeric column in df
			
		Throws:
			AssertionError if success_cases_col is ever
				greater than sample_size_col
			
		Returns:
			pandas dataframe as follows:
			[key cols] : [value cols]
			if collapse == True:
				[df.index] : [**, success_cases_col,
							  sample_size_col, mean, upper, lower]
			else:
				[df.index] : [**, success_cases_col, sample_size_col, 
								draw_0, ... , draw_n, ... , draw_ndraws]
			where ** are the other original columns in df
		"""
		df = self.mktscan.copy()
		
		# make sure successes are never more than total trials
		assert np.all(df[self.success_cases_col] <= df[self.sample_size_col]), \
			   ('there are instances where there are more successes in {} than'
			    ' trials in {}').format(self.success_cases_col,
				                       self.sample_size_col)
	
		# set the seed
		np.random.seed(self.seed)
		
		# construct dataframe of draws
		draws_df = pd.DataFrame()
		# for each index
		for idx in df.index:
			# store sample size, or trials and upcast to float
			sample_size = df.loc[idx, self.sample_size_col] * 1.0
			# store successes and upcast to float
			success_cases = df.loc[idx, self.success_cases_col] * 1.0
			# store the non cases
			non_cases = sample_size - success_cases
			# get an ndarray of samples
			if success_cases > 0:
				# go for it, draw using the numpy beta distribution
				draws = np.random.beta(success_cases,
				                       non_cases,
									   size=(1, self.ndraws))
			elif success_cases == 0:
				# beta distribution will fail, report all zeros
				draws = np.zeros((1, self.ndraws))
			else:
				raise ValueError("cannot have negative success cases")
			# either add the mean/upper/lower or all samples
			if self.collapse:
				columns = ['mean', 'upper', 'lower', 'std_dev']
				vals = [[draws.mean(),
						 np.percentile(draws, 75),
						 np.percentile(draws, 25),
						 np.std(draws)
						]]
			else:
				columns = ['draw_'+str(i) for i in range(0, self.ndraws)]
				vals = draws
			# construct a dataframe row and append to dataframe of all draws
			draws_row = pd.DataFrame(data=vals, columns=columns, index=[idx])
			draws_df = draws_df.append(draws_row)
			# return the result
		self.mktscan = pd.concat([df, draws_df], axis=1)
	
	def get_marketscan_w_uncertainty(self):
		"""Return marketscan data with proportions calculated"""
		self.append_beta_draws_to_df()
		self.mktscan.rename(columns={'mean':'prop_hf'}, inplace=True)
		self.mktscan['coeff_var_mkt'] = (self.mktscan['std_dev'] / \
		                                 self.mktscan['prop_hf'])
		self.mktscan['coeff_var_mkt'] = self.mktscan['coeff_var_mkt'].fillna(0)
		#mkt_scan = mkt_scan[['cause_id','age_group_id', 'sex_id', 'prop_hf',
		#                    'coeff_var_mkt']]
		
	def make_square_matrix(self):
		"""Make sure the matrix doesn't have any missing data."""
		
		CAUSES = self.mktscan[['cause_id']].drop_duplicates().reset_index()
		CAUSES['merge_col'] = 1
		CAUSES.drop('index', axis=1, inplace=True)

		AGE_GROUPS = self.mktscan[['age_group_id']].drop_duplicates()\
		                                           .reset_index()
		AGE_GROUPS['merge_col'] = 1
		AGE_GROUPS.drop('index', axis=1, inplace=True)

		SEXES = self.mktscan[['sex_id']].drop_duplicates().reset_index()
		SEXES['merge_col'] = 1
		SEXES.drop('index', axis=1, inplace=True)

		square = CAUSES.merge(SEXES, on='merge_col', how='inner')\
					   .merge(AGE_GROUPS, on='merge_col', how='inner')
					  
		self.mktscan = square.merge(self.mktscan, how='left')
		self.mktscan.fillna(0, inplace=True)
		
	def get_marketscan(self):
		"""get_marketscan"""
		self.prep_marketscan()
		self.mktscan = make_custom_causes(self.mktscan,
		                                  self.group_cols,
										  self.measure_cols)
		self.get_marketscan_w_uncertainty()
		self.make_square_matrix()

class CoDcorrect:
	"""CoDcorrect"""
	def __init__(self,
				 prefix,
				 qsub_errors,
				 qsub_output,
				 CAUSE_IDS,
				 LOCATION_IDS,
				 comp_causes,
				 mean_col,
				 upper_col,
				 lower_col,
				 send_Slack,
				 slack,
				 token,
				 channel):
				 
		# inputs
		self.prefix = prefix
		self.qsub_errors = qsub_errors
		self.qsub_output = qsub_output
		self.CAUSE_IDS = CAUSE_IDS
		self.LOCATION_IDS = LOCATION_IDS
		self.comp_causes = comp_causes
		self.group_cols = ['location_id',
		                   'age_group_id',
						   'sex_id']
		self.measure_cols = ['mean_death',
		                     'upper_death',
							 'lower_death']
		self.mean_col = mean_col
		self.upper_col = upper_col
		self.lower_col = lower_col
		self.send_Slack = send_Slack
		self.slack = slack
		self.token = token
		self.channel = channel
		
		# outputs
		self.death_data = None
		
	def get_cod_correct_data(self):
		"""Fetch cod correct data."""
		
		# the directory on the cluster that the death data is saved to
		out_path = self.prefix + "codcorrect_results/"
		
		# Parallelize the death data queries
		
		#### For debugging ####
		#need_draws = True
		need_draws = False
		#### For debugging ####
		
		if need_draws:
		
			if not os.path.exists(out_path):
				os.makedirs(out_path)
			else:
				shutil.rmtree(out_path)
				os.makedirs(out_path)
				print("Deleting and remaking an empty {}.".format(out_path))
				
			if not os.path.exists(out_path):
				os.makedirs(self.qsub_errors)
			else:
				shutil.rmtree(self.qsub_errors)
				os.makedirs(self.qsub_errors)
			if not os.path.exists(self.qsub_output):
				os.makedirs(self.qsub_output)
			else:
				shutil.rmtree(self.qsub_output)
				os.makedirs(self.qsub_output)
				
			cause_count = 0
			for cause_id in self.CAUSE_IDS:
				location_count = 0
				for location_id in self.LOCATION_IDS:
					call = ('qsub -cwd -N "draw_{location}" '
					        '-P proj_custom_models '
							'-o {qsub_output} '
							'-e {qsub_errors} '
							'-l mem_free=40G -pe multi_slot 4 cluster_shell.sh '
							'01_get_draws_subprocess.py '
							'{cause} '
							'{location} '
							'{out_path} '
							'{send_Slack} '
							'{token} '
							'{channel}'.format(qsub_output=self.qsub_output, 
											   qsub_errors=self.qsub_errors,
											   cause=cause_id,
											   location=location_id,
											   out_path=out_path,
											   send_Slack=self.send_Slack,
											   token=self.token,
											   channel=self.channel))    
					subprocess.call(call, shell=True)
					location_count += 1
					print "		results for {} locations submitted.".format(\
					                                             location_count)
				cause_count += 1
				print "results for {} causes submitted.".format(cause_count)
				wait('draw', 100)
		
		# After parallelization is complete oncatenate death data in one 
		# dataset
		
		#### For debugging ####
		#draws_ready = True
		draws_ready = False
		#### For debugging ####
		
		if draws_ready:
			df = pd.read_csv("{}codcorrect_draws_df.csv".format(self.prefix))
		else:
			df_list = []
			total_count = 0
			cause_count = 0
			for cause_id in self.CAUSE_IDS:
				location_count = 0
				for location_id in self.LOCATION_IDS:
					DF = pd.read_csv('{}codcorrect_{}_{}.csv'.format(\
					                                               out_path,
					                                               cause_id,
																   location_id))
					DF['cause_id'] = cause_id
					df_list.append(DF)
					location_count += 1
					print "		results for {} locations appended.".format(\
					                                             location_count)
				cause_count += 1
				total_count += 1
				print "results for {} causes appended.".format(cause_count)
			df = pd.concat(df_list)
			df.reset_index(inplace=True)
			df.to_csv('{}codcorrect_draws_df.csv'.format(self.prefix),
			          index=False)
			
		self.death_data = df
		
		assert not np.allclose(df.groupby(self.group_cols)['mean_death'].sum(),\
       		                         0.0), "summed fo all causes aren't above 0"

	def calc_coeff_var_using_upper_lower(self):
		"""Calculate the coefficient of variation using the upper and lower
		
		Arguments:
			df: pandas dataframe
			mean_col: column in df that represents mean estimate
			upper_col: column in df that represents upper bound (75th pctile)
			lower_col: column in df that represents lower bound (25th pctile)
		
		Returns:
			copy of df with addition of coeff_var column.
		"""
		self.death_data['std_err'] = (self.death_data[self.upper_col] - \
		                              self.death_data[self.lower_col]) / \
									  (2*1.96)
		self.death_data['coeff_var'] = self.death_data['std_err'] / \
		                               self.death_data[self.mean_col]
		self.death_data.drop('std_err', axis=1, inplace=True)
		
	def get_death_data(self):
		"""get_death_data"""
		self.get_cod_correct_data()
		self.death_data = make_custom_causes(self.death_data,
											 self.group_cols,
											 self.measure_cols)
		self.death_data = self.death_data[self.group_cols + 
										  ['cause_id'] +
										  self.measure_cols]									 
		self.calc_coeff_var_using_upper_lower()
		
		# make sure matrix is square
		self.death_data = make_square_matrix(self.death_data)

class MarketScan_DeathData:
	"""MarketScan_DeathData"""
	def __init__(self,
				 mktscan,
				 codcorr,
				 prop_hf_col,
				 cc_death_col,
				 mkt_cv_col, 
				 cc_cv_col,
				 hf_deaths_col,
				 cv_col,
				 group_cols,
				 mkt_coeff_var,
				 cod_coeff_var,
				 hf_prop_var,
				 limit):
		# inputs
		self.mktscan = mktscan
		self.codcorr = codcorr
		self.prop_hf_col = prop_hf_col
		self.cc_death_col = cc_death_col
		self.mkt_cv_col = mkt_cv_col
		self.cc_cv_col = cc_cv_col
		self.hf_deaths_col=hf_deaths_col
		self.cv_col=cv_col
		self.group_cols=group_cols
		self.mkt_coeff_var=mkt_coeff_var
		self.cod_coeff_var=cod_coeff_var
		self.hf_prop_var=hf_prop_var
		self.limit=0.25
		
		# outputs
		self.mktscan_codcorr = None
		
	def sqrt_sum_squares(self, x):
		"""Return the sqrt(sum of square vals in x).
		
		Arguments:
			x: array-like
		
		Returns:
			scalar
		"""
		return np.sqrt(np.square(x).sum())

	def combine_mkt_scan_w_deaths(self):
		"""Combine marketscan data with codcorrect.
		
		Also calculates the heart failure deaths and coefficient 
		of variation.
		"""
		df = pd.merge(self.codcorr, self.mktscan,
					  on=['age_group_id', 'sex_id', 'cause_id'],
					  how='left')
		df[self.prop_hf_col] = df[self.prop_hf_col].fillna(0)
		df[self.mkt_cv_col] = df[self.mkt_cv_col].fillna(0)
		#assert df[prop_hf_col].notnull().values.all(), 'merge failed'
		
		df['hf_deaths'] = df[self.prop_hf_col] * df[self.cc_death_col]
		df['mkt_cc_cv'] = np.sqrt(df[self.mkt_cv_col]**2 + \
		                          df[self.cc_cv_col]**2)
		
		# df = df.drop([mkt_cv_col, cc_cv_col], axis=1)
		self.mktscan_codcorr = df

	def group_and_summ_deaths_uncertainty(self):
		"""Summarize the hf deaths and coefficient of variation"""
		df = self.mktscan_codcorr.copy()
		
		# columns for aggregates
		group_cols = ['location_id', 'age_group_id', 'sex_id']
		prefix = 'summ_'
		groups = df.groupby(group_cols)
		summ_df = groups.agg({
				self.hf_deaths_col: np.sum,
				self.cv_col: lambda x: self.sqrt_sum_squares(x)
			}).reset_index()
		summ_df = summ_df.rename(columns = {
				self.hf_deaths_col: prefix + self.hf_deaths_col,
				self.cv_col: prefix + self.cv_col
			})
		
		df = df.merge(summ_df, on=group_cols, how='left')
		
		assert df[prefix+self.cv_col].notnull().values.all(), 'merge failed'
		
		df['hf_target_prop'] = df[self.hf_deaths_col] / \
		                       df[prefix + self.hf_deaths_col]
		
		assert np.allclose(df.groupby(group_cols)['hf_target_prop'].sum(), \
		                                      1.0), "proportions don't sum to 1"
		
		df['hf_target_prop_cv'] = np.sqrt(df[self.cv_col]**2 + \
		                                  df[prefix + self.cv_col]**2)
		df['std_err'] = df['hf_target_prop'] * df['hf_target_prop_cv']
		keep_cols = group_cols + ['cause_id', 'hf_target_prop', 'std_err']
		#df = df[keep_cols]
		self.mktscan_codcorr = df

	def max_two_cols_with_limit(self, x, col1, col2, limit):
		"""Return the lesser of the maximum of the columns or the limit."""
		max_cols =  x[col1].append(x[col2]).max()
		if max_cols>=limit:
			return limit
		else:
			return max_cols

	def adjust_std_error_with_limit(self):
		"""
		Adjust the coefficients of variance and standard error.
		
		Coefficients of variance are very large, and
		this results in extreme standard errors. Instead, the coefficients of 
		variance will be a bounded maximum. Return a new coefficient of variance
		for each proportion that is the maximum of the mkt_coeff_var and the 
		cod_coeff_var, or the limit, whichever is smaller. Do this by the group 
		columns.
		
		Arguments:
			df: pandas dataframe
			mkt_coeff_var: column in df that is the coefficient of variance for 
			the marketscan data cod_coeff_var: column in df that is the 
			coefficient of variance for the codcorrect data hf_prop_var: column 
			in df that is the final heart failure target proportion limit: the 
			bound to place on the maximum
			
		Throws:
			AssertionError if coeff_var_prop_adj, std_err_adj is already a 
			column
			
		Returns:
			pandas dataframe that is the df with an additional columns, 
			'coeff_var_prop_adj' and 'std_err_adj'
		"""
		df = self.mktscan_codcorr.copy()
		
		assert 'coeff_var_prop_adj' not in df.columns, \
		       'coeff_var_prop_adj already defined'
		assert 'std_err_adj' not in self.mktscan_codcorr.columns, \
		       'std_err_adj already defined'
		groups = df.groupby(self.group_cols)
		max_df = groups.apply(lambda x: self.max_two_cols_with_limit(\
		                                                     x, 
								                             self.cod_coeff_var, 
								                             self.mkt_coeff_var, 
								                             self.limit))
		max_df.name='coeff_var_prop_adj'
		max_df = max_df.reset_index()
		df = df.merge(max_df, how='left')
		assert df.coeff_var_prop_adj.notnull().values.all(), 'merge failed'
		df['std_err_adj'] = df[self.hf_prop_var] * df['coeff_var_prop_adj']
		
		self.mktscan_codcorr = df

	def assert_df_is_square(self):
		"""
		Assert that the dataframe has all locations and is square on age, sex, 
		and cause.
		
		Throws:
			AssertionError if not true
		"""
		df = self.mktscan_codcorr.copy()
		locations = get_location_metadata(location_set_id=35, gbd_round_id=4)\
		                                   .query('level >= 3')[['location_id']]
		locations['join_col'] = 1
		causes = df.cause_id.drop_duplicates().reset_index()
		causes['join_col'] = 1
		sexes = df.sex_id.drop_duplicates().reset_index()
		sexes['join_col'] = 1
		ages = df.age_group_id.drop_duplicates().reset_index()
		ages['join_col'] = 1
		square = locations.merge(causes).merge(sexes).merge(ages)
		square = square.drop('join_col', axis=1)
		m = square.merge(df, how='inner')
		assert len(m) == len(square), \
		       'the dataset is not square or is missing some locations'
		
	def get_inputs(self):
		"""get_inputs"""
		self.combine_mkt_scan_w_deaths()
		self.group_and_summ_deaths_uncertainty()
		
		
		# group by location age sex, summing deaths and
		#  applying sqrt/sum_squares to coeffs
		# merge this with ms*deaths and coefficients by location/age/sex/cause
		# set proportion as ms*deaths over total ms*deaths
		# set coeff var as sqrt(coeff_2 + total_coeff_2)
		# set std. err as proportion * coeff
		self.adjust_std_error_with_limit()
		
		# sex = 3 got in there in the cod correct query somehow
		self.mktscan_codcorr = self.mktscan_codcorr.query('sex_id != 3')
		
		# test that the dataframe is square and has all locations
		self.assert_df_is_square()
