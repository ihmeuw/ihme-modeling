
# coding: utf-8

"""
Template for formatting raw hospital data.  Follow the instructions in the
comments and ensure that the code is relevant to your particular source.
"""
import pandas as pd
import platform
import numpy as np
import sys
import warnings
import time
import getpass

# load our functions
user = getpass.getuser()
prep_path = r"FILEPATH/Functions"
sys.path.append(prep_path)

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
start = time.time()
filepath = "FILEPATH/BRA_SIH_2015_2016.H5"
df = pd.read_hdf(filepath, key='df')
print((time.time()-start)/60)

assert df.shape[0] == len(df.index.unique()), ("index is not unique, " +
    "the index has a length of " + str(len(df.index.unique())) +
    " while the DataFrame has " + str(df.shape[0]) + " rows" +
    "try this: df = df.reset_index(drop=True)")

df.drop('DIAG_SECUN', axis=1, inplace=True)

# Replace feature names on the left with those found in data where appropriate
# ALL OF THESE COLUMNS WILL BE MADE unless you comment out the ones you don't
# want
hosp_wide_feat = {
    'nid': 'nid',
    'MUNIC_RES': 'location_id',
    'representative_id': 'representative_id',
    'ANO_CMPT': 'year_start',
    'SEXO': 'sex_id',
    'IDADE': 'age',
    'COD_IDADE': 'age_group_unit',
    'code_system_id': 'code_system_id',

    # measure_id variables
    'MORTE': 'outcome_id',
    'facility_id': 'facility_id',

    # dates
    'DT_SAIDA': 'dis_date',
    'DT_INTER': 'adm_date',
    # diagnosis varibles
    'DIAG_PRINC': 'dx_1',
    'DIAGSEC1': 'dx_2',
    'DIAGSEC2': 'dx_3',
    'DIAGSEC3': 'dx_4',
    'DIAGSEC4': 'dx_5',
    'DIAGSEC5': 'dx_6',
    'DIAGSEC6': 'dx_7',
    'DIAGSEC7': 'dx_8'}

# Rename features using dictionary created above
df.rename(columns=hosp_wide_feat, inplace=True)

# set difference of the columns you have and the columns you want,
# yielding the columns you don't have yet
new_col_df = pd.DataFrame(columns=list(set(hosp_wide_feat.values()) -
                                       set(df.columns)))
df = df.join(new_col_df)
df['representative_id'] = 1

# make a dictionary from munic res values to location IDs using a method
# from the GBD 2015 Stata scripts
loc_dict = {"12": 4750,  # Acre
            "27": 4751,  # Alagoas
            "13": 4752,  # Amazonas
            "16": 4753,  # Amapa
            "29": 4754,  # Bahia
            "23": 4755,  # Ceara
            "53": 4756,  # Distrito Federal
            "32": 4757,  # Espirito Santo
            "52": 4758,  # Goias
            "21": 4759,  # Maranhao
            "31": 4761,  # Minas Gerais
            "50": 4750,  # Mato Grosso do Sul
            "51": 4762,  # Mato Grosso
            "15": 4763,  # Para
            "25": 4764,  # Paraiba
            "41": 4765,  # Parana
            "26": 4766,  # Pernambuco
            "20": 4766,  # Pernambuco
            "22": 4767,  # Piaui
            "33": 4768,  # Rio de Janeiro
            "24": 4769,  # Rio de Janeiro do Norte
            "11": 4770,  # Rondonia
            "14": 4771,  # Roraima
            "43": 4772,  # Rio Grande do Sul
            "42": 4773,  # Santa Catarina
            "28": 4774,  # Sergipe
            "35": 4775,  # Sao Paulo
            "17": 4776}  # Tocantins

df['to_map_id'] = df['location_id'].astype(str).str[0:2]


df['location_id'] = df['to_map_id'].map(loc_dict)
# drop the var we used to map
df.drop('to_map_id', axis=1, inplace=True)

# match year end to year start
df['year_end'] = df['year_start']

# group_unit 1 signifies age data is in years
df['source'] = 'BRA_SIH'

# code 1 for ICD-9, code 2 for ICD-10
df['code_system_id'] = 2

# set facility_id, needs to match what's in the former BRA SIH data
df['facility_id'] = 'inpatient unknown'

# case is the sum of live discharges and deaths
df['outcome_id'] = df['outcome_id'].replace([0, 1], ['discharge', 'death'])
# males are coded correctly to 1 but females are coded to 3
df['sex_id'] = df['sex_id'].replace([3], [2])

# metric_id == 1 signifies that the 'val' column consists of counts
df['metric_id'] = 1

# Create a dictionary with year-nid as key-value pairs
nid_dictionary = {2015: 237832, 2016: 281543}
df = fill_nid(df, nid_dictionary)


def convert_age_units(df):
    """
    Data includes coded age_group_units,
    ie is the age variable a year, month, etc
    2 == days
    3 == months
    4 == years
    5 == more than 100 years
    """
    # convert age for less than 1 year
    df.loc[df.age_group_unit.isin([2, 3]), 'age'] = 0
    # convert age for more than 100 years
    df.loc[df.age_group_unit == 5, 'age'] = 99

    # now that everything is in years convert age_group_unit accordingly
    df['age_group_unit'] = 1
    return df

df = convert_age_units(df)

def drop_day_cases(df, drop_date_cols=True):
    """
    drop individuals who we know stayed for less than 24 hours using the
    admission and discharge dates. This isn't perfect (it keeps someone who
    stays for 8 hours overnight) but it does remove the cases we know aren't
    full days
    """
    pre_drop = df.shape[0]

    df['dis_date'] = pd.to_datetime(df['dis_date'])
    df['adm_date'] = pd.to_datetime(df['adm_date'])

    # drop day cases
    df = df[df.dis_date != df.adm_date].copy()
    dropped = round(1 - (float(df.shape[0]) / pre_drop), 4) * 100
    print("{} percent of rows were dropped".format(dropped))

    # check for negative lengths of stay
    los = df['dis_date'].subtract(df['adm_date'])
    assert los.min() == pd.Timedelta('1 days 00:00:00'),\
        "Minimum length of stay is less than a day, something wrong"
    if drop_date_cols:
        df.drop(['dis_date', 'adm_date'], axis=1, inplace=True)
    return df

df = drop_day_cases(df, drop_date_cols=True)

#####################################################
# CLEAN VARIABLES
#####################################################

# Columns contain only 1 optimized data type
int_cols = ['location_id', 'year_start', 'year_end', 'age_group_unit', 'age',
            'sex_id', 'nid', 'representative_id',
            'metric_id']

str_cols = ['source', 'facility_id', 'outcome_id']


if df[str_cols].isnull().any().any():
    warnings.warn("\n\n There are NaNs in the column(s) {}".
                  format(df[str_cols].columns[df[str_cols].isnull().any()]) +
                  "\n These NaNs will be converted to the string 'nan' \n")

for col in int_cols:
    df[col] = pd.to_numeric(df[col], errors='raise')
for col in str_cols:
    df[col] = df[col].astype(str)

# Turn 'age' into 'age_start' and 'age_end'
#   - bin into year age ranges
#   - under 1, 1-4, 5-9, 10-14 ...

df.loc[df['age'] > 95, 'age'] = 95

df = age_binning(df)

# re-code unknown sex_id
df.loc[(df['sex_id'] != 1) & (df['sex_id'] != 2), 'sex_id'] = 3

start = time.time()

# store the data wide for the EN matrix
df.to_stata(r"FILEPATH/BRA_SIH.dta")
write_path = r"FILEPATH/BRA_SIH.H5"
write_hosp_file(df, write_path, backup=False)

#####################################################
# IF MULTIPLE DX EXIST:
    # TRANSFORM FROM WIDE TO LONG
#####################################################
# Find all columns with dx_ at the start
diagnosis_feats = df.columns[df.columns.str.startswith('dx_')]
# Remove non-alphanumeric characters from dx feats
for feat in diagnosis_feats:
    df[feat] = sanitize_diagnoses(df[feat])


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

# takes about 6 minutes
print("This ran in {} min".format((time.time()-start) / 60))

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
    "%s is missing from the columns of the DataFrame"% (hosp_frmat_feat[i])

# check data types
for i in df_agg.drop(['cause_code', 'source', 'facility_id', 'outcome_id'],
                     axis=1, inplace=False).columns:
    
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

assert (df.val >= 0).all(), ("for some reason there are negative case counts")

#####################################################
# WRITE TO FILE
#####################################################

# Saving the file
write_path = "FILEPATH/formatted_BRA_SIH.H5"

write_hosp_file(df_agg, write_path, backup=True)
