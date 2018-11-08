# -*- coding: utf-8 -*-
"""

"""
import pandas as pd
import platform
import numpy as np
import sys

# load our functions
USERNAME_path = r"FILEPATH/Functions"
sys.path.append(USERNAME_path)

from hosp_prep import *

# Environment:
if platform.system() == "Linux":
    root = r"FILEPATH/j"
else:
    root = "J:"

#####################################################
# READ DATA AND KEEP RELEVANT COLUMNS
# ASSIGN FEATURE NAMES TO OUR STRUCTURE
#####################################################

df_list = []
years = [2005, 2006, 2007, 2008, 2009, 2010, 2011, 2012, 2013]#, 2014]

for year in years:
    if year == 2014:
        df = pd.read_csv("FILEPATH/MEX_MOH_HOSP_DISCHARGES_2014_EGRESO_Y2015M08D12.CSV")
    else:
        df = pd.read_csv("FILEPATH/MEX_MOH_HOSP_DISCHARGES_" + str(year) + "_Y2016M09D23.CSV")
    df['year_start'] = year
    df['year_end'] = year
    df_list.append(df)
df = pd.concat(df_list)
del df_list

# Replace feature names on the left with those found in data where appropriate
hosp_wide_feat = {
    'nid': 'nid',
    'CEDOCVE': 'location_id',
    'representative_id': 'representative_id',
    # 'year': 'year',
    'year_start': 'year_start',
    'year_end': 'year_end',
    'SEXO': 'sex_id',
    'EDAD': 'age',
    
    # measure_id variables
    'age_start': 'age_start',
    'age_end': 'age_end',
    'age_group_unit': 'age_group_unit',
    'facility_id': 'facility_id',
    'code_system_id': 'code_system_id',
    'MOTEGRE': 'outcome_id',
    'CLUES': 'facility_id',
    # diagnosis varibles
    'AFECPRIN4': 'dx_1'}

# Rename features using dictionary created above
df.rename(columns= hosp_wide_feat, inplace = True)

# set difference of the columns you have and the columns you want,
# yielding the columns you don't have yet 
new_col_df = pd.DataFrame(columns = list(set(hosp_wide_feat.values())\
                                         - set(df.columns))) 
df = df.join(new_col_df)

#####################################################
# FILL COLUMNS THAT SHOULD BE HARD CODED
#####################################################

# These are completely dependent on data source 
df['representative_id'] = 1  

# every row that isn't a death is a discharge
df['outcome_id'] = np.where(df['outcome_id'] == 5, 'death', 'discharge')

# group_unit 1 signifies age data is in years
df['age_group_unit'] = 1
df['source'] = 'MEX_SAEH'

# code 1 for ICD-9, code 2 for ICD-10
# MEX is ICD-10
df['code_system_id'] = 2

# metric_id == 1 signifies that the 'val' column consists of counts
df['metric_id'] = 1

# Create a dictionary with year-nid as key-value pairs
nid_dict = {2004: 86953, 2005: 86954, 2006: 86955, 2007: 86956,
            2008: 86957, 2009: 86958, 2010: 94170, 2011: 94171,
            2012: 121282, 2013: 150449, 2014: 220205, 2015: 281773}
df = fill_nid(df, nid_dict)

# map mex states to location_id
state_map = pd.read_csv("FILEPATH/mex_state_map.csv")
state_code = pd.read_csv("FILEPATH/MEX_MOH_HOSP_DISCHARGES_2004_2013_CATENTIDAD_Y2016M09D23.CSV")

state_map.iloc[4,1] = state_code.iloc[4,1]
state_map.iloc[21,1] = state_code.iloc[21,1]

# merge mexican state codes onto gbd mexican state location ids
state_map = state_map.merge(state_code, how='left', left_on='location_name', right_on='DESCRIP')
state_map.drop(['DESCRIP', 'location_name'], axis=1, inplace=True)

df['EDO'] = df['location_id']
df.drop('location_id', axis=1, inplace=True)

# merge gbd location IDs on state codes
df = df.merge(state_map, how='left', on='EDO')
df.drop('EDO', axis=1, inplace=True)
assert df.location_id.isnull().sum() == 0, "Null values were introduced"

#####################################################
# CLEAN VARIABLES
#####################################################

# Columns contain only 1 optimized data type
int_cols = ['location_id', 'year_start', 'year_end', 'age_group_unit',
            'age_start', 'age_end', 'sex_id', 'nid', 'representative_id',
            'metric_id']
str_cols = ['source', 'facility_id', 'outcome_id']

for col in int_cols:
    df[col] = pd.to_numeric(df[col], errors='raise', downcast='integer')
for col in str_cols:
    df[col] = df[col].astype(str)
    
# Turn 'age' into 'age_start' and 'age_end'
#   - bin into year age ranges
#   - under 1, 1-4, 5-9, 10-14 ...
df = age_binning(df)

# drop unknown sex_ids
# codebook confirms 1 == Male, 2 == Female
df = df.query("sex_id == 1 | sex_id == 2")

# Find all columns with dx_ at the start
diagnosis_feats = df.columns[df.columns.str.startswith('dx_')]
# Remove non-alphanumeric characters from dx feats
for feat in diagnosis_feats:
    df[feat] = sanitize_diagnoses(df[feat])

#####################################################
# IF MULTIPLE DX EXIST:
    # TRANSFORM FROM WIDE TO LONG
#####################################################

if len(diagnosis_feats) > 1:
    # Reshape diagnoses from wide to long
    #   - review `hosp_prep.py` for additional function documentation
    df = stack_merger(df)
elif len(diagnosis_feats) == 1:
    df.rename(columns={'dx_1': 'cause_code'}, inplace=True)
    df['diagnosis_id'] = 1
else:
    print("Something went wrong, there are no ICD code features")

# If individual record: add one case for every diagnosis
df['val'] = 1

#####################################################
# GROUPBY AND AGGREGATE
#####################################################

# Check for missing values
print("Are there missing values in any row?\n" + str(not(df.isnull().sum()).all==0))

# Group by all features we want to keep and sums 'val'
group_vars = ['cause_code', 'diagnosis_id', 'sex_id', 'age_start','age_end','year_start',
           'year_end','location_id','nid','age_group_unit','source','facility_id',
          'code_system_id','outcome_id','representative_id', 'metric_id']
df_agg = df.groupby(group_vars).agg({'val':'sum'}).reset_index()

#####################################################
# ARRANGE COLUMNS AND PERFORM INTEGRITY CHECKS
#####################################################

# Arrange columns in our standardized feature order
hosp_frmat_feat = ['age_group_unit', 'age_start', 'age_end',
                   'year_start', 'year_end',
                   'location_id',
                   'representative_id',
                   'sex_id',
                   'diagnosis_id', 'metric_id', 'outcome_id', 'val',
                   'source','nid',
                   'facility_id',
                   'code_system_id', 'cause_code']

df_agg = df_agg[hosp_frmat_feat]

# check if all columns are there
assert len(hosp_frmat_feat) == len(df_agg.columns), "The DataFrame has the wrong number of columns"
for i in range(len(hosp_frmat_feat)):
    assert hosp_frmat_feat[i] in df_agg.columns, "%s is missing from the columns of the DataFrame" % (hosp_frmat_feat[i])

# check data types 
for i in df_agg.drop(['cause_code','source','facility_id','outcome_id'], axis=1, inplace=False).columns:
    # assert that everything but cause_code, source, measure_id (for now) are NOT object
    assert df_agg[i].dtype != object, "%s should not be of type object" % (i)

# check number of unique feature levels
assert len(df_agg['year_start'].unique()) == len(df_agg['nid'].unique()),    "number of feature levels of years should match number of feature levels of nid"
assert len(df_agg['age_start'].unique()) == len(df_agg['age_end'].unique()),    "number of feature levels age start should match number of feature levels age end"
assert len(df_agg['diagnosis_id'].unique()) <= 2, "diagnosis_id should have 2 or less feature levels"
assert len(df_agg['sex_id'].unique()) == 2, "There should only be two feature levels to sex_id"
assert len(df_agg['code_system_id'].unique()) <= 2, "code_system_id should have 2 or less feature levels"
assert len(df_agg['source'].unique()) == 1, "source should only have one feature level"

#####################################################
# WRITE TO FILE
#####################################################

# Saving the file
write_path = "FILEPATH/MEX_SAEH.H5"
df_agg.to_hdf(write_path, key='table', format='table', complib='blosc', complevel=5)