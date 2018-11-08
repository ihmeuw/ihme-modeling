# -*- coding: utf-8 -*-
"""

"""
import pandas as pd
import platform
import numpy as np
import sys
import warnings
from db_queries import get_location_metadata
import getpass

user = getpass.getuser()

# Environment:
if platform.system() == "Linux":
    root = r"FILEPATH/j"
    hosp_path = r"FILEPATH/Functions".format(user)
else:
    root = "J:"
    hosp_path = r"FILEPATH/Functions".format(user)

sys.path.append(hosp_path)
import hosp_prep

#####################################################
# READ DATA AND KEEP RELEVANT COLUMNS
# ASSIGN FEATURE NAMES TO OUR STRUCTURE
#####################################################

filepath = root + r"FILEPATH/"\
                  r"hospital discharge data 2016.xlsx"
try:
    df = pd.read_excel(filepath)

# If this assert fails uncomment this line:
#df = df.reset_index(drop=True)
assert df.shape[0] == len(df.index.unique()), ("index is not unique, " +
    "the index has a length of " + str(len(df.index.unique())) +
    " while the DataFrame has " + str(df.shape[0]) + " rows" +
    "try this: df = df.reset_index(drop=True)")

# Replace feature names on the left with those found in data where appropriate
# ALL OF THESE COLUMNS WILL BE MADE unless you comment out the ones you don't
# want
hosp_wide_feat = {
    'nid': 'nid',
    'location_id': 'location_id',
    'representative_id': 'representative_id',
    # 'year': 'year',
    'year_start': 'year_start',
    'year_end': 'year_end',
    'SEX': 'sex_id',
    # 'age': 'age',
    'year': 'age',
    # 'age_end': 'age_end',
    'age_group_unit': 'age_group_unit',
    'code_system_id': 'code_system_id',

    # measure_id variables
    'alive': 'outcome_id',
    'facility_id': 'facility_id',
    # diagnosis varibles
    'ICD-10': 'dx_1',
    
    'Admiss date': 'adm_date',
    'Date discharge': 'dis_date'
    }

# Rename features using dictionary created above
df.rename(columns=hosp_wide_feat, inplace=True)

# set difference of the columns you have and the columns you want,
# yielding the columns you don't have yet
new_col_df = pd.DataFrame(columns=list(set(hosp_wide_feat.values()) -
                                       set(df.columns)))
df = df.join(new_col_df)

#####################################################
# FILL COLUMNS THAT SHOULD BE HARD CODED
# this is where you fill in the blanks with the easy
# stuff, like what version of ICD is in the data.
#####################################################

df['representative_id'] = 3  # data is for a single hospital

# use loc hierarchy to go from ISO country code to location id
locs = get_location_metadata(location_set_id=9, gbd_round_id=5)
loc_id = locs.loc[locs.location_name == "Jordan", "location_id"]
loc_id = loc_id.tolist()[0]
df['location_id'] = loc_id
assert (df.location_id == 144).all(),\
    "loc id check failed"  # hardcode a location_id check

# group_unit 1 signifies age data is in years
df['age_group_unit'] = 1
df['source'] = 'JOR_ABHD'

# code 1 for ICD-9, code 2 for ICD-10
df['code_system_id'] = 2

df['year_start'] = 2016  
df['year_end'] = 2016

# metric_id == 1 signifies that the 'val' column consists of counts
df['metric_id'] = 1

# Create a dictionary with year-nid as key-value pairs
nid_dictionary = {2016: 317423}
df = hosp_prep.fill_nid(df, nid_dictionary)

#####################################################
# MANUAL PROCESSING
# this is where fix the quirks of the data, like making values in the
# data match the values we use.

# For example, repalce "Male" with the number 1
#####################################################

# the length of stay variable in the data seems to have some errors
df['los'] = df['dis_date'] - df['adm_date']

# df['los_d'] = df['los'].dt.days

# drop day cases and negative values
df = df[df['los'] > "0 days"]
# drop stays longer than 365 days in a single year (not possible)
df = df[df['los'] <= "365 days"]

non_day_cases = df.shape[0]

df['facility_id'] = "hospital"

# drop cols
df.drop(['los', 'Length of stay', 'dis_date', 'adm_date', 'month', 'day'],
        axis=1, inplace=True)

df['outcome_id'].replace(['alive', 'dead'],
                         ['discharge', 'death'], inplace=True)

#####################################################
# CLEAN VARIABLES
#####################################################

# Columns contain only 1 optimized data type
int_cols = ['location_id', 'year_start', 'year_end', 'age_group_unit', 'age',
            'sex_id', 'nid', 'representative_id',
            'metric_id']

str_cols = ['source', 'facility_id', 'outcome_id', 'dx_1']

# use this to infer data types
# df.apply(lambda x: pd.lib.infer_dtype(x.values))

if df[str_cols].isnull().any().any():
    warnings.warn("\n\n There are NaNs in the column(s) {}".
                  format(df[str_cols].columns[df[str_cols].isnull().any()]) +
                  "\n These NaNs will be converted to the string 'nan' \n")

for col in int_cols:
    df[col] = pd.to_numeric(df[col], errors='raise', downcast='integer')
for col in str_cols:
    df[col] = df[col].astype(str)

assert df.dx_1.isnull().sum() == 0

# Turn 'age' into 'age_start' and 'age_end'
#   - bin into year age ranges
#   - under 1, 1-4, 5-9, 10-14 ...

df.loc[df['age'] > 95, 'age'] = 95  # this way everything older than GBD
# terminal age start will be lumped into the terminal age group.
df = hosp_prep.age_binning(df)

# drop unknown sex_id
df.loc[(df['sex_id'] != 1) & (df['sex_id'] != 2), 'sex_id'] = 3

#####################################################
# IF MULTIPLE DX EXIST:
    # TRANSFORM FROM WIDE TO LONG
#####################################################
# Find all columns with dx_ at the start
diagnosis_feats = df.columns[df.columns.str.startswith('dx_')]
# Remove non-alphanumeric characters from dx feats
for feat in diagnosis_feats:
    df[feat] = hosp_prep.sanitize_diagnoses(df[feat])

# there are rows with missing cause codes. instead of dropping these lets
# convert them to our bunk icd code
df.loc[df.dx_1.isnull(), 'dx_1'] = "cc_code"

if len(diagnosis_feats) > 1:
    # Reshape diagnoses from wide to long
    #   - review `hosp_prep.py` for additional documentation
    df = hosp_prep.stack_merger(df)

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
print("Are there missing values in any row?\n")
null_condition = df.isnull().values.any()
if null_condition:
    warnings.warn(">> Yes.  ROWS WITH ANY NULL VALUES WILL BE LOST ENTIRELY")
else:
    print(">> No.")

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
columns_before = df_agg.columns
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
columns_after = df_agg.columns

# check if all columns are there
assert set(columns_before) == set(columns_after),\
    "You lost or added a column when reordering"
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

assert (df_agg.val >= 0).all(),\
    ("for some reason there are negative case counts")
assert non_day_cases == df_agg.val.sum(),\
    "case sum before and after groupby don't match"

#####################################################
# WRITE TO FILE
#####################################################

# Saving the file
write_path = "FILEPATH/formatted_JOR_ABHD.H5"

hosp_prep.write_hosp_file(df_agg, write_path, backup=True)
