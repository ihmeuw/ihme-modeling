"""

# Year: NID
# 2010: 285460
# 2011: 285464
# 2012: 285465
# 2013: 285467
# 2014: 285468
# 2015: 285471

GHDx entries:
ADDRESS

"""
import pandas as pd
import platform
import numpy as np
import sys
import datetime


# load our functions
# USERNAME_path = "FILEPATH"
# #USERNAME_path = "FILEPATH"
# sys.path.append(USERNAME_PATH)
# #sys.path.append(USERNAME_PATH)

# when running on the cluster:
USERNAME_PATH = "FILEPATH"
sys.path.append(USERNAME_PATH)

from hosp_prep import *

# Environment:
if platform.system() == "Linux":
    root = "FILEPATH"
else:
    root = "FILEPATH"


# In[ ]:

#####################################################
# READ DATA AND KEEP RELEVANT COLUMNS
# ASSIGN FEATURE NAMES TO OUR STRUCTURE
#####################################################

# years = [2010, 2011, 2012, 2013, 2014, 2015]
# df_list = []
# for year in years:
#     df = pd.read_excel("FILEPATH")
#     df['year_start'] = year
#     df['year_end'] = year
#     df_list.append(df)
# df_orig = pd.concat([df_list])

# This is not ideal but it works:
df_2010 = pd.read_excel("FILEPATH")
df_2010['year_start'] = 2010

df_2011 = pd.read_excel("FILEPATH")
df_2011['year_start'] = 2011

df_2012 = pd.read_excel("FILEPATH")
df_2012['year_start'] = 2012

df_2013 = pd.read_excel("FILEPATH")
df_2013['year_start'] = 2013

df_2014 = pd.read_excel("FILEPATH")
df_2014['year_start'] = 2014

df_2015 = pd.read_excel("FILEPATH")
df_2015['year_start'] = 2015

df_orig = pd.concat([df_2010, df_2011, df_2012, df_2013, df_2014, df_2015])
df_orig = df_orig.reset_index()  # ALWAYS DO THIS AFTER CONCAT
df_orig['year_end'] = df_orig['year_start']


# In[ ]:

del df_2010
del df_2011
del df_2012
del df_2013
del df_2014
del df_2015


# In[ ]:

# Select features from raw data to keep
keep = ['ANA_SESSO', 'DATA_DECESSO', 'ETA_DATA_INGRESSO',
        'DIA1', 'DIA2', 'DIA3', 'DIA4', 'DIA5', 'DIA6', 'year_start',
        'year_end']
df = df[keep]

# CODEBOOK:
# ANA_SESSO: sex (m=male, f=female),
# DATA_DECESSO: date of death. If 31/12/9999, then person is till alive,
# ETA_DATA_INGRESSO: age at hospital access, (might not be age at departure)
# DIA1, DIA2, ..., DIA6: diagnoses in ICD-9-CM
# used the file RICOVERI_DIA_INT_legend to make this


# In[ ]:

# Replace feature names on the left with those found in data where appropriate
# ALL OF THESE COLUMNS WILL BE MADE unless you comment out the ones you don't
# want
hosp_wide_feat = {
    'nid': 'nid',
    'location_id': 'location_id',
    'representative_id': 'representative_id',
#    'year': 'year',
    'year_start': 'year_start',
    'year_end': 'year_end',
    'ANA_SESSO': 'sex_id',
#    'age': 'age',
#    'age_start': 'age_start',
#    'age_end': 'age_end',
    'age_group_unit': 'age_group_unit',
    'code_system_id': 'code_system_id',

    # measure_id variables
    'outcome_id': 'outcome_id',
    'facility_id': 'facility_id',

    # diagnosis varibles
    'DIA1': 'dx_1',
    'DIA2': 'dx_2',
    'DIA3': 'dx_3',
    'DIA4': 'dx_4',
    'DIA5': 'dx_5',
    'DIA6': 'dx_6',
    # source specific
    'ETA_DATA_INGRESSO': 'age',
#     'DATA_NASCITA': 'DOB',
    'DATA_DECESSO': 'DOD',
#     'DATA_USCITA': 'DOE',
#     'ATTRIBSDO_DESC': 'type_of_admission'
}

# Rename features using dictionary created above
df.rename(columns=hosp_wide_feat, inplace=True)

# set difference of the columns you have and the columns you want,
# yielding the columns you don't have yet
new_col_df = pd.DataFrame(columns=list(set(hosp_wide_feat.values()) -
                                       set(df.columns)))
df = df.join(new_col_df)


# In[ ]:

#####################################################
# FILL COLUMNS THAT SHOULD BE HARD CODED
#####################################################

# These are completely dependent on data source
df['representative_id'] = 3  # Not representative
df['location_id'] = 86  # this is national level data for Italy

# group_unit 1 signifies age data is in years
df['age_group_unit'] = 1
df['source'] = 'ITA_IMCH'  # Institute for Maternal and Child Health

# code 1 for ICD-9, code 2 for ICD-10
df['code_system_id'] = 1  # icd 9

# metric_id == 1 signifies that the 'val' column consists of counts
df['metric_id'] = 1

# Create a dictionary with year-nid as key-value pairs
# Year: NID
# 2010: 285460
# 2011: 285464
# 2012: 285465
# 2013: 285467
# 2014: 285468
# 2015: 285471

nid_dictionary = {
2010: 285460,
2011: 285464,
2012: 285465,
2013: 285467,
2014: 285468,
2015: 285471}

#nid_dictionary = {'example_year': 'example_nid'}
df = fill_nid(df, nid_dictionary)

df['facility_id'] = 'inpatient unknown'


# In[ ]:

# infer outcome_id from the Date of Death information. If data marks them as alive,
# then we will mark them as a discharge, else, mark as death

# first, make sure we're dealing with strings and not datetime dtypes:
df['DOD'] = df['DOD'].astype(str)

# the data doesn't explicitly say "this person died" or not. they mark
# date of deaths. The documentation states that "If 31/12/9999,
# then person is till[sic] alive". Note pandas interprets
# 31/12/9999 as 9999-12-31 00:00:00

df.loc[df['DOD'] == "9999-12-31 00:00:00", 'outcome_id'] = 'discharge'
df.loc[df['DOD'] != "9999-12-31 00:00:00", 'outcome_id'] = 'death'

df.drop('DOD', axis=1, inplace=True)


# In[ ]:

#####################################################
# CLEAN VARIABLES
#####################################################

# replace string values in the sex_id with ints
df['sex_id'] = np.where(df['sex_id'] == "M", 1, 2)

# Columns contain only 1 optimized data type
int_cols = ['location_id', 'year_start', 'year_end', 'age_group_unit', 'age',
            'sex_id', 'nid', 'representative_id',
            'metric_id']
# BE CAREFUL WITH NULL VALUES IN THE STRING COLUMNS, they will be converted
# to the string "nan"
str_cols = ['source', 'facility_id', 'outcome_id']

for col in int_cols:
    df[col] = pd.to_numeric(df[col], errors='raise', downcast='integer')
for col in str_cols:
    df[col] = df[col].astype(str)


# In[ ]:

#####################################################
# CLEAN AGE
#####################################################

# test code to get age
# test = df.iloc[:10, :]
# test = test[['DOE', 'DOB']]  # DOE means "date of egress"
# test['age_days'] = test.DOE - test.DOB
# test['age_days_int'] = test['age_days'].astype(datetime.timedelta).map(lambda x: np.nan if pd.isnull(x) else x.days)
# test['age'] = np.floor(test['age_days_int'] / 365)
# # test

# test['diff_in_days'] = test['DOE'] - test['DOB']
# test['diff_in_years'] = test['diff_in_days'] / datetime.timedelta(days=365)
# test

# this method accounts for leap years: days are absolute, and the number of days in a year
# varies by year (bcuz of leap years). The subtraction yields days, and then we divide by
# the "true" number of days in a year (dividing by 365.242 days would be more technically
# correct, i think. That's a "sidereal" year)
#df['age'] = df.DOE - df.DOB
#df['age'] = df['age'].astype(datetime.timedelta).map(lambda x: np.nan if pd.isnull(x) else x.days)
#df['age'] = np.floor(df['age'] / 365.25)  # divide by Juliean year
#print("calculated age was different (greater than) age at admission " + str((df.age > df.age_at_ingress).sum() / float(df.shape[0])) + " percent of the time")

# Turn 'age' into 'age_start' and 'age_end'
#   - bin into year age ranges
#   - under 1, 1-4, 5-9, 10-14 ...
df = df[df['age'] < 125]  # toss out data where age is greater than GBD terminal age
df.loc[df['age'] > 95, 'age'] = 95  # this way everything older than GBD
# terminal age start will be lumped into the terminal age group.
df = age_binning(df)
df['age_start'] = pd.to_numeric(df['age_start'], downcast='integer', errors='raise')
df['age_end'] = pd.to_numeric(df['age_end'], downcast='integer', errors='raise')
df.drop('age', axis=1, inplace=True)  # changed age_binning to not drop age col


# In[ ]:

#####################################################
# SEX
#####################################################
# Already did it

#####################################################
# SOURCE SPECIFIC CLEANING
#####################################################
# NA


# In[ ]:

# keep columns we want, because it might be messing up stack_merger,
# this line shouldn't be dropping anything
df = df[['sex_id', 'dx_1', 'dx_2', 'dx_3', 'dx_4', 'dx_5', 'dx_6',
         'year_start', 'year_end', 'representative_id', 'nid',
         'code_system_id', 'facility_id', 'age_group_unit', 'outcome_id',
         'location_id', 'source', 'metric_id', 'age_start', 'age_end']]


# In[ ]:

#####################################################
# WIDE TO LONG DX
#####################################################

# Find all columns with dx_ at the start
diagnosis_feats = df.columns[df.columns.str.startswith('dx_')]
# Remove non-alphanumeric characters from dx feats
for feat in diagnosis_feats:
    df[feat] = sanitize_diagnoses(df[feat])


# In[ ]:

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

# If individual record: add one case for every diagnosis
df['val'] = 1


# In[ ]:

# after reshaping, make sure ICD codes are strings
df['cause_code'] = df['cause_code'].astype(str)


# In[ ]:

#####################################################
# GROUPBY AND AGGREGATE
#####################################################

# Check for missing values
print("Are there missing values in any row?\n")
null_condition = df.isnull().values.any()
if null_condition:
    print(">> Yes.  ROWS WITH ANY NULL VALUES WILL BE LOST ENTIRELY")
else:
    print(">> No.")


# In[ ]:

# Group by all features we want to keep and sums 'val'
group_vars = ['cause_code', 'diagnosis_id', 'sex_id', 'age_start',
              'age_end', 'year_start', 'year_end', 'location_id', 'nid',
              'age_group_unit', 'source', 'facility_id', 'code_system_id',
              'outcome_id', 'representative_id', 'metric_id']
df = df.groupby(group_vars).agg({'val': 'sum'}).reset_index()


# In[ ]:

#####################################################
# ARRANGE COLUMNS AND PERFORM INTEGRITY CHECKS
#####################################################

# Arrange columns in our standardized feature order
columns_before = df.columns
hosp_frmat_feat = ['age_group_unit', 'age_start', 'age_end',
                   'year_start', 'year_end',
                   'location_id',
                   'representative_id',
                   'sex_id',
                   'diagnosis_id', 'metric_id', 'outcome_id', 'val',
                   'source', 'nid',
                   'facility_id',
                   'code_system_id', 'cause_code']
df = df[hosp_frmat_feat]
columns_after = df.columns

# check if all columns are there
assert set(columns_before) == set(columns_after),    "You lost or added a column when reordering"
for i in range(len(hosp_frmat_feat)):
    assert hosp_frmat_feat[i] in df.columns,        "%s is missing from the columns of the DataFrame"        % (hosp_frmat_feat[i])

# check data types
for i in df.drop(['cause_code', 'source', 'facility_id', 'outcome_id'],
                     axis=1, inplace=False).columns:
    # assert that everything but cause_code, source, measure_id (for now)
    # are NOT object
    assert df[i].dtype != object, "%s should not be of type object" % (i)

# check number of unique feature levels
assert len(df['year_start'].unique()) == len(df['nid'].unique()),    "number of feature levels of years and nid should match number"
assert len(df['age_start'].unique()) == len(df['age_end'].unique()),    "number of feature levels age start should match number of feature " +    r"levels age end"
assert len(df['diagnosis_id'].unique()) <= 2,    "diagnosis_id should have 2 or less feature levels"
assert len(df['sex_id'].unique()) == 2,    "There should only be two feature levels to sex_id"
assert len(df['code_system_id'].unique()) <= 2,    "code_system_id should have 2 or less feature levels"
assert len(df['source'].unique()) == 1,    "source should only have one feature level"


# In[ ]:

# df.apply(lambda x: pd.lib.infer_dtype(x.values))


# In[ ]:

#####################################################
# WRITE TO FILE
#####################################################

# Saving the file
write_path = "FILEPATH"
df.to_hdf(write_path, key='df', format='table',
              complib='blosc', complevel=5, mode='w')
# df.to_csv(write_path, index = False)
