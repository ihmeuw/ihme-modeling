
"""
Prepares the final result of the '00_prep_hf_mktscan_parallel.py' for
a diagnostic visualization.
"""

import os
import pandas as pd
from db_queries import get_location_metadata, get_ids

def setup_for_shiny(df, out_path):
	"""
	Description:
		Prepares the final result of the '00_prep_hf_mktscan_parallel.py' for
		a diagnostic visualization.
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
	
	# columns necessary for creating appending necesary aggregates and adding
	# columns
	# with metadata useful to diagnostics (e.g. location name)
	index_cols = ['hf_target_prop', 
				  'std_err_adj', 
				  'sex_id', 
				  'cause_id', 
				  'age_group_id']
	
	# columns used for creating aggregates for the region and super region
	# proportions.
	group_cols = ['sex_id', 
				  'cause_id',
				  'age_group_id']
	
	# columns used in the final dataset.
	final_cols = ['hf_target_prop',
				  'std_err_adj',
				  'location_id',
				  'location_ascii_name', 
				  'sex_id',
				  'cause_id',
				  'age_group_id', 
				  'age_group_name',
				  'cause_name']

	locations = get_location_metadata(location_set_id=35)\
	                                                   [['location_id',
	                                                     'location_ascii_name']]
	ages = get_ids('age_group')
	causes = get_ids('cause')
	
	# Exclude composite etiologies for input diagnostics
	df = df.query('cause_id not in (520, 385, 499)')
	
	# location metadata
	df = df.merge(locations, on='location_id', how='inner')
	
	# add column with age group names
	df = df.merge(ages, on='age_group_id', how='inner')
	
	# To make the age progression linear and consecutive recode some of the
	# age_groups.
	df['age_group_id'] = df['age_group_id'].replace(to_replace=28, value=4)
	df.sort_values(by='age_group_id', axis=0, ascending=True, inplace=True)
	
	# add column with cause names
	df = df.merge(causes, on='cause_id', how='inner')
	
	# drop unnecessary columns
	df = df[final_cols]
	
	df.rename(columns={'hf_target_prop':'proportion'}, inplace=True)
	df.rename(columns={'hf_target_prop':'standard_error'}, inplace=True)
	
	# write the diagnostic input data to csv
	df.to_csv("{}hf_inputs.csv".format(out_path), index=False, encoding='utf-8')
		
if __name__ == '__main__':
		# The out-path for the Market Scan data prepared for diagnostics	
		out_path = "FILEPATH/imp_hf/input_diagnostics/"
		in_path = "FILEPATH"
		
		if not os.path.exists(out_path):
			os.makedirs(out_path)
		
		inputs = pd.read_csv("{}/heart_failure_target_props_subnat.csv"\
		                     .format(in_path))
		setup_for_shiny(inputs, out_path)
	



