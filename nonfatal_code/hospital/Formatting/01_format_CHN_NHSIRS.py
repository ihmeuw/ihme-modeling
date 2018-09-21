# -*- coding: utf-8 -*-
"""
Script for formatting China data from FILEPATH
"""
import pandas as pd
import platform
import numpy as np
import sys
from db_tools.ezfuncs import query

# Environment:
if platform.system() == "Linux":
    root = "FILEPATH"
else:
    root = "FILEPATH"

# load our functions
USERNAME_PATH = "FILEPATH"
sys.path.append(USERNAME_PATH)


from hosp_prep import *

#####################################################
# READ DATA AND KEEP RELEVANT COLUMNS
# ASSIGN FEATURE NAMES TO OUR STRUCTURE
#####################################################
# column names, see data folder for relevant info
column_names = ['Administrative division', 'Disease code', 'Age',
                'Discharge method', 'Sex', 'Number of people discharged']
# read in data
df_list = []
years = [2013, 2014, 2015]
#  filepath will change once data is catelogued.  Skip the first row cuz we
# have our own column names.
for year in years:
    df = pd.read_csv(root + "FILEPATH",
        encoding='utf-8', skiprows=[0], names=column_names)

    df['year_start'] = year
    df['year_end'] = year
    df_list.append(df)
df = pd.concat(df_list)


# Replace feature names on the left with those found in data where appropriate
hosp_wide_feat = {
    'nid': 'nid',
    'location_id': 'location_id',
    'representative_id': 'representative_id',
    'year_start': 'year_start',
    'year_end': 'year_end',
    'Sex': 'sex_id',
    'age_start': 'age_start',
    'age_end': 'age_end',
    'age_group_unit': 'age_group_unit',
    'facility_id': 'facility_id',
    'code_system_id': 'code_system_id',

    # measure_id variables
    'Discharge method': 'outcome_id',
    'facility_id': 'facility_id',
    'Number of people discharged': 'val',
    # diagnosis varibles
    'Disease code': 'dx_1'}

# Rename features using dictionary created above
df.rename(columns=hosp_wide_feat, inplace=True)

# set difference of the columns you have and the columns you want,
# yielding the columns you don't have yet
new_col_df = pd.DataFrame(columns=list(set(hosp_wide_feat.values()) -
                                       set(df.columns)))
df = df.join(new_col_df)

#####################################################
# FILL COLUMNS THAT SHOULD BE HARD CODED
#####################################################

# These are completely dependent on data source
df['representative_id'] = 1  # Rep for National
df['diagnosis_id'] = 1

# group_unit 1 signifies age data is in years
df['age_group_unit'] = 1
df['source'] = 'CHN_NHSIRS'

# code 1 for ICD-9, code 2 for ICD-10
df['code_system_id'] = 2

df['facility_id'] = 'hospital'

# metric_id == 1 signifies that the 'val' column consists of counts
df['metric_id'] = 1

# Create a dictionary with year-nid as key-value pairs
nid_dictionary = {2013: 282493, 2014: 282496, 2015: 282497}
df = fill_nid(df, nid_dictionary)

#####################################################
# CLEAN VARIABLES
#####################################################

# fit min and max ages to the same pattern as every other group
df.loc[df['Age'] == '95S', 'Age'] = '95_99S'
df.loc[df['Age'] == '1_S', 'Age'] = '0_1S'
# remove the S
df['Age'] = df['Age'].str.replace("S", "")
# split into age groups
df['age_start'], df['age_end'] = df['Age'].str.split("_", 1).str

# drop old age column
df.drop('Age', axis=1, inplace=True)

# clean sex_id
# According to the contact that provided the data:
# 1: Male
# 2: Female
# 3: Error
# 9: Unknown
# 0: They didn't tell us

df.dropna(subset=['sex_id'], axis=0, inplace=True)
df = df.query("sex_id == 1 | sex_id == 2")  # for now keeping only male and
# female, since we don't know how to split sexes
df['sex_id'] = df['sex_id'].astype(int)

# outcomes
# According to the contact that provided the data:
# 1: Discharged on advice of doctor;
# 2: Transferred to hospital on advice of doctor;
# 3: Transferred to community health service facility/township hospital
# on advice of doctor
# 4: Left hospital without advice from doctor
# 5: Died
# 6: They didn't tell us
# 9: Other
# [Empty]: Didn’t fill out
# we only have values "discharge" and "death" in our system.
df['outcome_id'] = np.where(df['outcome_id'] == 5, 'death', 'discharge')

# clean cause_code
# This data has plenty of strange ICD codes that don't mean anything. We're
# leaving them in, because they will drop out when we map.
df.dropna(subset=['dx_1'], axis=0, inplace=True)

# location
############## test to see if Chinese data at provincial level
# read map
# admin_map = pd.read_csv(r"FILEPATH")
# read map from shared db
admin_map = query("SQL QUERY")
admin_map = admin_map[0:33]


# drop location_id because it will be added with the merge
df.drop('location_id', axis=1, inplace=True)

df['map_id'] = df['Administrative division']
# convert num to str
admin_map['map_id'] = admin_map['map_id'].astype(str)
df['map_id'] = df['map_id'].astype(str)
# extract first two digits - code for provincial level
admin_map['map_id'] = admin_map['map_id'].str[0:2]
df['map_id'] = df['map_id'].str[0:2]

# merge on admin codes
df = df.merge(admin_map, how='left', on='map_id')

assert df.location_name.isnull().sum() == 0, "Null values were introduced"
assert (df.location_name=="").sum() == 0, "blank values were introduced"

df.drop(['map_id', 'Administrative division', 'location_name'], axis=1, inplace=True)

# Find all columns with dx_ at the start
diagnosis_feats = df.columns[df.columns.str.startswith('dx_')]
# Remove non-alphanumeric characters from dx feats
for feat in diagnosis_feats:
    df[feat] = sanitize_diagnoses(df[feat])


# Columns contain only 1 optimized data type
int_cols = ['location_id', 'year_start', 'year_end', 'age_group_unit',
            'age_start', 'age_end', 'sex_id', 'nid', 'representative_id',
            'metric_id']
#str_cols = ['source', 'facility_id', 'outcome_id']

for col in int_cols:
    df[col] = pd.to_numeric(df[col], errors='raise', downcast='integer')
#for col in str_cols:
#    df[col] = df[col].astype(str)

#####################################################
# IF MULTIPLE DX EXIST:
    # TRANSFORM FROM WIDE TO LONG
#####################################################

if len(diagnosis_feats) > 1:
    # Reshape diagnoses from wide to long
    #   - review `hosp_prep.py` for additional documentation
    df = stack_merger(df)

elif len(diagnosis_feats) == 1:
    df.rename(columns={'dx_1': 'cause_code'}, inplace=True)
    df['diagnosis_id'] = 1

else:
    print("Something went wrong, there are no ICD code features")

#####################################################
# GROUPBY AND AGGREGATE
#####################################################

# Check for missing values
print("Are there missing values in any row?\n" +
      str(not(df.isnull().sum()).all() == 0))

# Group by all features we want to keep and sums 'val'
group_vars = ['cause_code', 'diagnosis_id', 'sex_id', 'age_start',
              'age_end', 'year_start', 'year_end', 'location_id', 'nid',
              'age_group_unit', 'source', 'facility_id', 'code_system_id',
              'outcome_id', 'representative_id', 'metric_id']
df_agg = df.groupby(group_vars).agg({'val': 'sum'}).reset_index()

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
                   'source', 'nid',
                   'facility_id',
                   'code_system_id', 'cause_code']

df_agg = df_agg[hosp_frmat_feat]

# check if all columns are there
assert len(hosp_frmat_feat) == len(df_agg.columns),\
    "The DataFrame has the wrong number of columns"
for i in range(len(hosp_frmat_feat)):
    assert hosp_frmat_feat[i] in df_agg.columns,\
        "%s is missing from the columns of the DataFrame"\
        % (hosp_frmat_feat[i])

# check data types
for i in df_agg.drop(['cause_code', 'source', 'facility_id', 'outcome_id'],
                     axis=1, inplace=False).columns:
    # assert that everything but cause_code, source, measure_id (for now)
    # are NOT object
    assert df_agg[i].dtype != object, "%s should not be of type object" % (i)

# check number of unique feature levels
assert len(df_agg['year_start'].unique()) == len(df_agg['nid'].unique()),\
    "number of feature levels of years and nid should match number"
assert len(df_agg['age_start'].unique()) == len(df_agg['age_end'].unique()),\
    "number of feature levels age start should match number of feature " +\
    r"levels age end"
assert len(df_agg['diagnosis_id'].unique()) <= 2,\
    "diagnosis_id should have 2 or less feature levels"
assert len(df_agg['sex_id'].unique()) == 2,\
    "There should only be two feature levels to sex_id"
assert len(df_agg['code_system_id'].unique()) <= 2,\
    "code_system_id should have 2 or less feature levels"
assert len(df_agg['source'].unique()) == 1,\
    "source should only have one feature level"

#####################################################
# WRITE TO FILE
#####################################################

# Saving the file
df_agg['cause_code'] = df_agg['cause_code'].astype(str)
write_path = root + "FILEPATH"
df_agg.to_hdf(write_path, key='df', format='table', complib='blosc',
              complevel=5, mode='w')
# df_agg.to_csv("FILEPATH", index=False)
# df_agg.to_csv(write_path, index = False)
