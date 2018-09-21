# -*- coding: utf-8 -*-
"""
@author: USERNAME and USERNAME

Format Indonesia Integrated Hospital Data 2013

ADDRESS
"""
import pandas as pd
import platform
import numpy as np
import sys
import warnings
import getpass
from db_tools.ezfuncs import query

# load our functions
if getpass.getuser() == 'USERNAME':
    USERNAME = "FILEPATH"
    sys.path.append(USERNAME)

from hosp_prep import *

# Environment:
if platform.system() == "Linux":
    root = "FILEPATH"
else:
    root = "FILEPATH"

######################################################
# functions for pre-formatting
#####################################################


def check_sums(data, sexes=["male", "female"]):
    for sex in sexes:
        cols = data.columns[data.columns.str.contains("\d-" + sex)]
        data[sex + "_sums"] = dat[cols].sum(axis=1)
        data[sex + "_diff"] = data["total_" + sex] - data[sex + "_sums"]
        assert data[data[sex + "_diff"] > 0].shape[0] == 0,\
            "this one {} is bad {} column {}".format(sex, i, col)


def reshape_long(df):
    # reshape long to create age start, age end, sex and val columns
    df = df.set_index(['cause_code', 'subnat']).stack().\
        reset_index()
    df.rename(columns={'level_2': 'age_sex', 0: 'val'}, inplace=True)

    # split age start age end cols
    df['age_start'], df['age_end'], df['sex_id'] = df['age_sex'].str.split("-", 2).str
    # clean values
    df.sex_id.replace(['male', 'female'], [1, 2], inplace=True)
    # age cols to int
    df['age_start'], df['age_end'] = pd.to_numeric(df['age_start'], errors='raise'), pd.to_numeric(df['age_end'], errors='raise')
    # drop unneeded columns
    df.drop(['age_sex'], axis=1, inplace=True)
    return(df)

#####################################################
# READ DATA AND KEEP RELEVANT COLUMNS
# ASSIGN FEATURE NAMES TO OUR STRUCTURE
#####################################################
filepath = root + "FILEPATH"

# IDN subnational data
df = pd.read_excel(filepath, sheetname="Tables C.1.1-C.1.29")


# assign roughly formatted column names
# translations are in the excel sheet on the first tab
age_groups = ['0-6 hr', '7-28 hr', '29hr-<1 th', '1-4', '5-14', '15-24',
              '25-44', '45-64', '65-99']
age_sex_cols = []
for age in age_groups:
    lk = age + "-male"
    pr = age + "-female"
    age_sex_cols.append(lk)
    age_sex_cols.append(pr)
# order really matters when making this list
col_names = ['row_num', 'idn_disease_code', 'cause_code', 'disease_name'] +\
        age_sex_cols +\
        ['total_male', 'total_female', 'total_discharges', 'total_deaths']
# assign column names
df.columns = col_names

# every subnational table is in the same spreadsheet, extract each subnat name
df['row_num'] = df['row_num'].astype(str)
# pull subnational table headers
headers = df['row_num'][df['row_num'].str.startswith("TABEL")]

# loop over the indices of the headers subsetting the data and assigning
# subnat name. this could be done with a mask later
provinces = []
for i in np.arange(0, headers.size, 1):
    # subset each subnational table
    if i == headers.size-1:
        dat = df.iloc[headers.index[i]:df.shape[0], :].copy()
    else:
        dat = df.iloc[headers.index[i]:headers.index[i+1], :].copy()

    # process the subset
    # drop the rows before ICD codes start
    dat = dat.loc[dat.index[dat['row_num'] == "1"][0]:, :]
    # drop the total of cases by age from the last row
    dat = dat[dat['row_num'] != "TOTAL"]
    # add the provincial table name to pull subnational location
    subnat = headers.iloc[i]
    # find where the province name starts and ends
    subnat_start = subnat.find("PROVINSI") + 9  # 8 chars plus whitespace
    subnat_end = subnat.find("TAHUN") - 1 # remove whitespace
    dat['subnat'] = subnat[subnat_start:subnat_end].lower()
    for col in age_sex_cols + ['total_male', 'total_female',
                               'total_discharges', 'total_deaths']:
        # clean the val columns
        # find cells with a "," in them, there seems to have been a trailing
        # zero cut off from time to time

        # find which position from the right the comma is in [1 indexed!]
        dat['comma'] =\
            dat.loc[dat[col].astype(str).str.contains(","), col].str.len() -\
            dat.loc[dat[col].astype(str).str.contains(","), col].str.find(",")

        dat.loc[dat['comma'] == 3, col] = dat.loc[dat['comma'] == 3, col] + "0"
        dat.loc[dat['comma'] == 2, col] =\
            dat.loc[dat['comma'] == 2, col] + "00"
        dat.loc[dat['comma'] == 1, col] =\
            dat.loc[dat['comma'] == 1, col] + "000"

        # drop non alpha_numeric stuff like commas then coerce to numeric
        dat[col] = dat[col].astype(str).str.replace("\W", "")
        dat[col] = pd.to_numeric(dat[col], errors='coerce')
    # combine all under 1 columns
    dat['0-1-male'] = dat['0-6 hr-male'].fillna(0) +\
        dat['7-28 hr-male'].fillna(0) +\
        dat['29hr-<1 th-male'].fillna(0)
    dat['0-1-female'] = dat['0-6 hr-female'].fillna(0) +\
        dat['7-28 hr-female'].fillna(0) +\
        dat['29hr-<1 th-female'].fillna(0)
    dat.drop(labels=['0-6 hr-male', '0-6 hr-female',
                     '7-28 hr-male', '7-28 hr-female',
                     '29hr-<1 th-male', '29hr-<1 th-female'], axis=1,
                     inplace=True)
    # check sums of all the age groups row by row
    check_sums(dat)
    provinces.append(dat)

df = pd.concat(provinces)

# manually fix the total this row which seems to be totally wrong
df.loc[4518, 'total_discharges'] = df.loc[4518, 'total_male'] +\
    df.loc[4518, 'total_female']

# copy the df and test the total discharges
data = df.copy()
data = data[data['total_discharges'].notnull()]
assert (data["female_sums"] + data["male_sums"] ==
        data['total_discharges']).all()

def get_case_sum(df):
    total = 0
    for col in df.filter(regex="^[0-9]").columns:
        colsum = df[col].sum()
        total = total + colsum
    return(total)
val_sum = get_case_sum(df)
#############################################
# DEATHS AND DISCHARGES ARE TOGETHER
# AND WE LIKE THAT
##############################################
## get all age/sex columns
#to_pct = df.filter(regex="[0-9]", axis=1).columns
#df[to_pct] = df[to_pct].fillna(0)
#
## create age percents by disease
#for col in to_pct:
#    df[col + "_pct"] = df[col] / df['total_discharges']
#
## subtract deaths from outgoing cases
#df['total_deaths'] = df['total_deaths'].fillna(0)
#df['total_discharges'] = df['total_discharges'] - df['total_deaths']
## drop cols with discharges and deaths
#df.drop(to_pct, axis=1, inplace=True)
#
#for col in to_pct:
#    df[col+"_est"] = df[col + "_pct"] * df['total_discharges']


# drop the cols we used to check sum totals to prepare for the reshape
# if we want to keep deaths we'll need to store them in a different way
df.drop(['comma', 'male_sums', 'male_diff', 'female_sums', 'female_diff',
         'total_male', 'total_female', 'total_discharges', 'row_num',
         'idn_disease_code', 'disease_name', 'total_deaths'], axis=1,
         inplace=True)

# reshape long
df = reshape_long(df)
# remove case counts of zero
df = df[df['val'] != 0]

# check cases weren't lost after reshape
assert val_sum == df.val.sum()

# If this assert fails uncomment this line:
df = df.reset_index(drop=True)
assert df.shape[0] == len(df.index.unique()), ("index is not unique, " +
    "the index has a length of " + str(len(df.index.unique())) +
    " while the DataFrame has " + str(df.shape[0]) + " rows" +
    "try this: df = df.reset_index(drop=True)")

# Replace feature names on the left with those found in data where appropriate
# ALL OF THESE COLUMNS WILL BE MADE unless you comment out the ones you don't
# want
hosp_wide_feat = {
    'nid': 'nid',
    # 'location_id': 'location_id',
    'representative_id': 'representative_id',
    'year_start': 'year_start',
    'year_end': 'year_end',
    'sex_id': 'sex_id',
    'age_start': 'age_start',
    'age_end': 'age_end',
    'age_group_unit': 'age_group_unit',
    'code_system_id': 'code_system_id',

    # measure_id variables
    'outcome_id': 'outcome_id',
    'facility_id': 'facility_id',
    # diagnosis varibles
    'cause_code': 'dx_1'}

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

# These are completely dependent on data source


df['representative_id'] = 1  # GHDx lists it as representative
df['location_parent_id'] = 11
df['outcome_id'] = 'case'
df['facility_id'] = 'hospital'
df['year_start'], df['year_end'] = [2013, 2013]

# get subnational location names
loc_id_query = SQL QUERY
locs = query(loc_id_query, conn_def='epi')
locs['location_name'] = locs['location_name'].str.lower()
# fix some location names
df.loc[df.subnat == "dki jakarta", 'subnat'] = "jakarta"
df.loc[df.subnat == "di yogyakarta", 'subnat'] = "yogyakarta"
df.loc[df.subnat == "kepulauan bangka belitung", 'subnat'] = "bangka belitung"
df.rename(columns={'subnat': 'location_name'}, inplace=True)
df = df.merge(locs, how='left', on='location_name')
assert df.location_id.isnull().sum() == 0

# fix some of the names in the data
# group_unit 1 signifies age data is in years
df['age_group_unit'] = 1
df['source'] = 'IDN_SIRS'

# code 1 for ICD-9, code 2 for ICD-10
df['code_system_id'] = 2

# metric_id == 1 signifies that the 'val' column consists of counts
df['metric_id'] = 1
df['nid'] = 206640

# Create a dictionary with year-nid as key-value pairs
# nid_dictionary = {'example_year': 'example_nid'}
# df = fill_nid(df, nid_dictionary)

#####################################################
# CLEAN VARIABLES
#####################################################

# Columns contain only 1 optimized data type
int_cols = ['location_id', 'year_start', 'year_end', 'age_group_unit',
            'age_start', 'age_end', 'sex_id', 'nid', 'representative_id',
            'metric_id']
# BE CAREFUL WITH NULL VALUES IN THE STRING COLUMNS, they will be converted
# to the string "nan"
# fast way to cast to str while preserving Nan:
# df['casted_foo'] = df.foo.loc[df.foo.notnull()].map(str)
str_cols = ['source', 'facility_id', 'outcome_id']
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

# Turn 'age' into 'age_start' and 'age_end'
#   - bin into year age ranges
#   - under 1, 1-4, 5-9, 10-14 ...

# df.loc[df['age'] > 95, 'age'] = 95  # this way everything older than GBD
# terminal age start will be lumped into the terminal age group.
# df = age_binning(df)

# adjust unknown sex_id
df.loc[(df['sex_id'] != 1)&(df['sex_id'] != 2), 'sex_id'] = 3

# Create year range if the data covers multiple years
#df = year_range(df)

#####################################################
# MANUAL PROCESSING
# this is where fix the quirks of the data, like making values in the
# data match the values we use.

# For example, repalce "Male" with the number 1
#####################################################


# Replace feature levels manually
# Make the values contained in the data match the shared tables
#   - E.g.: "MALE" should become 1, "FEMALE" should become 2
# These are merely examples
# df['sex_id'].replace(['2 - MEN','1 - FEMALE'],[1,2], inplace = True)
# df['outcome_id'].replace(['3 - DIED (LA)','2 - Translated (A) TO ANOTHER HOSPITAL','1 - Out (A) HOME'],
#                          ["death","discharge","discharge"], inplace = True)
# df['facility_id'].replace(['3 - EMERGENCY AFTER 24 HOURS','1 - ROUTINE','2 - EMERGENCY TO 24 HOURS'],
#                           ['hospital','hospital','emergency'], inplace=True)

# Manually verify the replacements
# assert len(df['sex_id'].unique()) == 2, "df['sex_id'] should have 2 feature levels"
# assert len(df['outcome_id'].unique()) == 2, "df['outcome_id'] should have 2 feature levels"
# assert len(df['facility_id'].unique() == 2), "df['facility_id] should have 2 feature levels"

# Find all columns with dx_ at the start
diagnosis_feats = df.columns[df.columns.str.startswith('dx_')]
# Remove non-alphanumeric characters from dx feats
for feat in diagnosis_feats:
    df[feat] = sanitize_diagnoses(df[feat])

pre_unique_codes = df['dx_1'].sort_values().unique()
df['dx_1'] = df['dx_1'].astype(str)
assert (pre_unique_codes == df['dx_1'].sort_values().unique()).all()
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
# df['val'] = 1

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
assert round(val_sum, 3) == round(df_agg.val.sum(), 3),\
    "some cases were lost"

#####################################################
# WRITE TO FILE
#####################################################

# Saving the file
write_path = "FILEPATH"
df_agg.to_hdf(write_path, key='df', format='table',
              complib='blosc', complevel=5, mode='w')
# df_agg.to_csv(write_path, index = False)
