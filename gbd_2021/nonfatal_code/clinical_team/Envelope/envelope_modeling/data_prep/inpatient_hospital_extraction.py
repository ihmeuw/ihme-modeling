## This script will extract our representative inpatient hospital sources for the envelope

import pandas as pd
import numpy as np
import os as os

## Load in libraries
home_dir = 'FILEPATH'
os.chdir(home_dir)
from extractor import gdoc_query, extractor
from db_queries import get_population, get_covariate_estimates, get_model_results
from functions_to_prep_extractions import *
from cluster_utilities import *
#from elmo import run

write_dir = sys.argv[1]
data_dir = sys.argv[2]
name = sys.argv[3]
drop_births = sys.argv[4] # Whether you want live births only or not
print(drop_births)

ages = pd.read_csv('FILEPATH')
ages = ages[['age_group_id', 'age_group_years_start', 'age_group_years_end']]

df = pd.read_hdf(data_dir + name + '.H5')

df = df.loc[(df.facility_id == 'inpatient unknown') | (df.facility_id == 'hospital')]

df = df.loc[df.diagnosis_id == 1]

code_sys = df.code_system_id.unique()

if len(code_sys) > 1:
    print('Multiple ICD systems!')
    # Do ICD9 and ICD10 separately
    if drop_births == 'T':
    	df1 = df.loc[df.code_system_id == 1]
    	df1 = df1.loc[~df1.cause_code.str.contains('V3[0-9]')]
    
    	df2 = df.loc[df.code_system_id == 2]
    	df2 = df2.loc[~(df2.cause_code == 'Z380')]
    	df = pd.concat([df1, df2])

else:
    if drop_births == 'T':
        if code_sys == 1:
            df = df.loc[~df.cause_code.str.contains('V3[0-9]')]
        if code_sys == 2:
            df = df.loc[~(df.cause_code == 'Z380')]


# get columns we want
df = df[['location_id', 'year_start', 'year_end','sex_id', 'age_group_id', 'val', 'nid']]
# groupby to get the sum of values. Gets total sum of primary admissions at a certain age
df = df.groupby(['age_group_id', 'sex_id', 'year_start', 'year_end', 'location_id', 'nid']).sum().reset_index()

# Get population, merge on and calculate IP admissions per capita
pop = get_population(location_id=df.location_id.unique().tolist(),
	year_id = df.year_start.unique().tolist(),
    age_group_id = df.age_group_id.unique().tolist(), sex_id=[1,2,3], gbd_round_id = 7, decomp_step = 'iterative')
df = df.merge(pop[['age_group_id', 'sex_id', 'location_id', 'population', 'year_id']], 
	left_on = ['age_group_id', 'sex_id', 'year_start', 'location_id'], 
	right_on = ['age_group_id', 'sex_id', 'year_id', 'location_id'],how='left')

df['mean'] = df['val']/df['population']


df.rename(columns={'population':'sample_size'}, inplace=True)
df.rename(columns={'val':'cases'}, inplace=True)

# These should be whole numbers
df = df.round({'cases':0, 'sample_size':0})

# Get males and females if not already done
df = df.loc[df.sex_id.isin([1,2])]

# Format for ST-GPR
df['measure'] ='continuous'

df = pd.merge(df, ages)
df = df.rename(columns={'age_group_years_start':'age_start', 'age_group_years_end':'age_end'})
df['lower'] = ''
df['upper'] = ''
df['standard_error'] = ''


# Some more formatting
df['is_outlier'] = 0
df['visit_type'] = 'ip' 

if drop_births == 'T':
    write_dir = write_dir + '/no_births'
    if not os.path.exists(write_dir):
        os.makedirs(write_dir)
df.to_csv(write_dir + '/' + name + '.csv')
print('Done!')
