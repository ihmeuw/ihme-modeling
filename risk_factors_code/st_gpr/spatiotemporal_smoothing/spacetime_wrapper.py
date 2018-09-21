## Arguments
import sys
run_root	  = sys.argv[1]
central_root  = sys.argv[2]
holdout_num	  = sys.argv[3]
parallel      = int(sys.argv[4])
if parallel == 1:
    loc_start = int(sys.argv[5])
    loc_end = int(sys.argv[6])

## Load
import os
os.chdir('%s' %(central_root))
os.environ['MKL_NUM_THREADS'] = '1' ## Force it to run processes using 1 node
os.environ['OMP_NUM_THREADS'] = '1'
import st_gpr.spacetime as st
import numpy as np
import pandas as pd
import multiprocessing as mp
reload(st)

def blockPrint():
	sys.stdout = open(os.devnull, 'w')

# Restore
def enablePrint():
	sys.stdout = sys.__stdout__

################################
## Load Data
################################

print('holdout number: %s' %(holdout_num))
os.chdir('%s' %(run_root))

# Load parameters
params = pd.read_hdf('param_%s.h5' %(holdout_num), 'parameters')

# Load location hierarchy
locs = pd.read_hdf('param_%s.h5' %(holdout_num), 'location_hierarchy')

# Load data
data = pd.read_hdf('temp_%s.h5' %(holdout_num), 'prepped')

# Load prior
prior = pd.read_hdf('temp_%s.h5' %(holdout_num), 'prior')

## Merge data, prior, locations
df = pd.merge(prior, data, how='left', on=['location_id', 'year_id', 'age_group_id', 'sex_id'])
df = pd.merge(df, locs, on='location_id')

## Create temporary age index
df['age_group_id_orig'] = df['age_group_id']
df['age_group_id'] = df.groupby(['age_group_id']).grouper.group_info[0]

# Sort
df = df.sort(columns=['location_id', 'year_id', 'age_group_id', 'sex_id'])

################################
## Set Parameters
################################

# ST parameters
lambdaa = float(params['st_lambdaa'])
omega = float(params['st_omega'])
zeta = float(params['st_zeta'])
zeta_no_data = float(params['st_zeta_no_data'])
location_set_version_id = int(params['location_set_version_id'])

# Set threshold for data count for below which zeta_no_data is used
data_threshold = 5

# Detect start and end year
year_start = int(np.min(df.year_id))
year_end = int(np.max(df.year_id))

# Detect age groups
age_start = int(np.min(df.age_group_id))
age_end = int(np.max(df.age_group_id))

## ls_list
ls_list = df[['location_id', 'sex_id']].drop_duplicates()
## If paralell, subset
if parallel == 1:
	ls_list = ls_list[(ls_list.location_id >= loc_start) & (ls_list.location_id <= loc_end)]
ls_list = ls_list.as_matrix()
ls_list = tuple(map(tuple, ls_list))

################################
## Function to run ST
################################

def run_spacetime(location_id, df, age_start, age_end, year_start, year_end, lambdaa, zeta, zeta_nodata, omega):
	 
	################################
	## Setup
	################################

	## Detect level and parent
	national_id = int(locs.level_3[locs.location_id == location_id])
	level = int(locs.level[locs.location_id == location_id])

	# Making sure that only borrowing strength from higher levels
	columns_to_keep = list(df.columns.values)
	df = df[((df.level<=3) | (df.level_3==national_id)) & (df.level<=level)]
	df = df[columns_to_keep]

	# Count the number of data (maximum number of data in an age group for that sex)
	data_count = df.loc[df.location_id == location_id].groupby('age_group_id').agg('count')
	data_count = np.max(data_count.data)
		
	# If data count is less than threshold, pass a flag to ST
	if data_count >= data_threshold:
		zeta_threshold = 1
	else:
		zeta_threshold = 0

	## If level > 3, set zeta to 0.5
	if level > 3:
		zeta = 0.5

	################################
	## Set weights
	################################
	# Initialize the smoother
	s = st.Smoother(df, 
					location_set_version_id, 
					timevar='year_id', 
					agevar='age_group_id', 
					spacevar='location_id', 
					datavar='data', modelvar='prior',
					pred_age_group_ids=range(age_start, age_end+1),
					pred_start_year=year_start,
					pred_end_year=year_end,
					snvar='cv_subgeo')
	
	# Set parameters (can additionally specify omega (age weight, positive real number) and zeta (space weight, between 0 and 1))
	s.lambdaa = lambdaa
	s.zeta = zeta
	s.zeta_no_data = zeta_no_data
	if 22 not in pd.unique(df['age_group_id_orig']):
		s.omega = omega
	
	# Tell the smoother to calculate both time weights and age weights
	s.time_weights()
	if 22 not in pd.unique(df['age_group_id_orig']):
		s.age_weights()
		
	################################
	## Run Smoother
	################################
	s.smooth(locs=location_id, level=level, zeta_threshold=zeta_threshold)
	results = pd.merge(df, s.long_result(), on=['age_group_id', 'year_id', 'location_id'], how='right')
	
	################################
	## Clean
	################################
	cols = ['location_id', 'year_id', 'age_group_id', 'age_group_id_orig', 'sex_id', 'st']
	results =results[cols].drop_duplicates()
	
	return results


################################
## Function to run ST
################################

## Define a class to run the spacetime function
class st_launch(object):
	def __init__(self, df, 
				age_start, age_end, year_start, year_end, 
				lambdaa, zeta, zeta_no_data, omega):
		self.df = df
		self.age_start = age_start
		self.age_end = age_end
		self.year_start = year_start
		self.year_end = year_end
		self.lambdaa = lambdaa
		self.zeta = zeta
		self.zeta_no_data = zeta_no_data
		self.omega = omega
	def __call__(self, ls):
		df = self.df
		location_id = ls[0]
		df = df.loc[df.sex_id==ls[1]]
		return  run_spacetime(location_id, df, 
				self.age_start, self.age_end, self.year_start, self.year_end, 
				self.lambdaa, self.zeta, self.zeta_no_data, self.omega)
		 
if parallel == 1:
	p = mp.Pool(processes=4)
else :
	p = mp.Pool(processes=30)

result = p.map(st_launch(df, age_start, age_end, year_start, year_end, lambdaa, zeta, zeta_no_data, omega), ls_list) 
p.close()

## Concatenate list
df = pd.concat(result)

## Clean age index
df['age_group_id'] = df['age_group_id_orig']
df = df.drop('age_group_id_orig', 1)

################################
## Save
################################

## Save
if parallel == 1:
    if not os.path.exists('st_temp_%s/' %(holdout_num)):
        os.makedirs('st_temp_%s/' %(holdout_num), 0777)
    df.to_csv('st_temp_%s/%s_%s.csv' %(holdout_num, loc_start, loc_end), index=False)
else:
    df.to_hdf('temp_%s.h5' %(holdout_num), 'st', mode='a', format='fixed')

