
"""
Created on Mon Jan 23 17:32:28 2017
@author: USERNAME and USER

Format the UK UTLA data, data has already been merged with regional data
and desuppressed with linear model weighting in the scripts
Formatting/001_pre_format_uk_cluster.py
and
Formatting/001_pre_format_UK_UTLA_fit_models.py
respectively.

URL
"""
import pandas as pd
import platform
import numpy as np
import sys
import warnings
import getpass


if getpass.getuser() == 'USERNAME':
    USER_path = r"FILENAME"
    sys.path.append(USER_path)
if getpass.getuser() == 'USERNAME':
    USER_path = r"FILENAME"
    USER_path_local = r"FILEPATH"
    sys.path.append(USER_path)
    sys.path.append(USER_path_local)

from hosp_prep import *


if platform.system() == "Linux":
    root = "FILENAME"
else:
    root = "FILEPATH"





if platform.system() == "Linux":
    filepath = root + r"FILEPATH"
    df = pd.read_csv(filepath)
else:
    filepath = root + r"FILEPATH"
    df = pd.read_hdf(filepath, key="df")
    
    
    
    


inj = pd.read_csv(root + "FILENAME"
                        r"GBR_ENGLAND_HES_2001_2014_INJ_POISON_BY_AGE_SEX_"
                        r"FILEPATH")

inj.rename(columns={'SEX': 'sex_id', 'CAUSE': 'cause_code'}, inplace=True)


inj = inj[inj.cause_code.notnull()]


inj['FYEAR'] = inj['FYEAR'].astype(str)
inj['year_end'] = inj['FYEAR'].str[-2:].astype(np.int)
inj['year_end'] = inj['year_end'] + 2000
inj['year_start'] = inj['year_end'] - 1
inj['fiscal_year'] = (inj['year_start'] + inj['year_end']) / 2

inj.loc[inj['AgeBand'] == '90+', 'AgeBand'] = '9099'
inj.loc[inj['AgeBand'] == '0000', 'AgeBand'] = '0001'

inj['age_start'] = inj['AgeBand'].str[0:2]
inj['age_end'] = inj['AgeBand'].str[2:]
inj['age_start'] = pd.to_numeric(inj['age_start'], errors='raise')
inj['age_end'] = pd.to_numeric(inj['age_end'], errors='raise')


inj.loc[inj.age_start.isnull(), ['age_start', 'age_end']] = [0, 99]

inj.drop(['FYEAR', 'year_start', 'year_end', 'AgeBand', 'DiagCode3'],
         axis=1, inplace=True)
inj['location_id'] = 4749


df = pd.concat([df, inj])


df = df.reset_index(drop=True)
assert df.shape[0] == len(df.index.unique()), ("index is not unique, " +
    "the index has a length of " + str(len(df.index.unique())) +
    " while the DataFrame has " + str(df.shape[0]) + " rows" +
    "try this: df = df.reset_index(drop=True)")


df = df[['location_id', 'age_start', 'age_end', 'sex_id', 'fiscal_year',
         'cause_code', 'value']]

val_sum = df.value.sum()



df['start_value'] = df['value'] / 2
df['end_value'] = df['value'] / 2

df['year_start'] = df['fiscal_year'] - 0.5
df['year_end'] = df['fiscal_year'] + 0.5
df.drop(['fiscal_year', 'value'], axis=1, inplace=True)

df = df.set_index(['location_id', 'age_start', 'age_end', 'sex_id',
                   'cause_code', 'year_start', 'year_end']).\
                stack().reset_index()


df.loc[df['level_7'] == 'start_value', 'year_end'] =\
    df.loc[df['level_7'] == 'start_value', 'year_start']
df.loc[df['level_7'] == 'end_value', 'year_start'] =\
    df.loc[df['level_7'] == 'end_value', 'year_end']
df.rename(columns={0: 'value'}, inplace=True)
df.drop(['level_7'], axis=1, inplace=True)


year_min = df.year_start.min()
year_max = df.year_start.max()
df.loc[df['year_start'] == year_min, 'value'] =\
        df.loc[df['year_start'] == year_min, 'value'] * 2
df.loc[df['year_end'] == year_max, 'value'] =\
        df.loc[df['year_end'] == year_max, 'value'] * 2


val1 = df[(df.year_start != df.year_start.min()) & (df.year_end != df.year_end.max())].value.sum()
val2 = df[df.year_start == df.year_start.min()].value.sum() / 2
val3 = df[df.year_end == df.year_end.max()].value.sum() / 2
assert round((val1 + val2 + val3), 2) == round(val_sum, 2)
val_sum = df.value.sum()




hosp_wide_feat = {
    'nid': 'nid',
    'location_id': 'location_id',
    'representative_id': 'representative_id',
    
    
    
    'sex_id': 'sex_id',
    
    'age_start': 'age_start',
    'age_end': 'age_end',
    'age_group_unit': 'age_group_unit',
    'code_system_id': 'code_system_id',

    
    'outcome_id': 'outcome_id',
    'facility_id': 'facility_id',
    
    'cause_code': 'dx_1',
    'value': 'val'}


df.rename(columns=hosp_wide_feat, inplace=True)



new_col_df = pd.DataFrame(columns=list(set(hosp_wide_feat.values()) -
                                       set(df.columns)))
df = df.join(new_col_df)
























df['representative_id'] = 1  


df['age_group_unit'] = 1
df['source'] = 'UK_HOSPITAL_STATISTICS'


df['code_system_id'] = 2


df['metric_id'] = 1
df['facility_id'] = "hospital"
df['outcome_id'] = "case"



nid_dictionary = {2001: 134187, 2002: 121272, 2003: 121273, 2004: 121274,
                  2005: 121275, 2006: 121276, 2007: 121277, 2008: 121278,
                  2009: 121279, 2010: 121280, 2011: 121281, 2012: 265423,
                  2013: 265424, 2014: 265425, 2015: 265425}
df = fill_nid(df, nid_dictionary)

assert df['nid'].isnull().sum() == 0






int_cols = ['location_id', 'year_start', 'year_end', 'age_group_unit',
            'age_start', 'age_end', 'sex_id', 'nid', 'representative_id',
            'metric_id']




str_cols = ['source', 'facility_id', 'outcome_id']




if df[str_cols].isnull().any().any():
    warnings.warn("There are Nulls in one of the columns {}"
                  r" These nulls will be converted to the string nan".
                  format(str_cols), Warning)

for col in int_cols:
    df[col] = pd.to_numeric(df[col], errors='raise')
for col in str_cols:
    df[col] = df[col].astype(str)


df.loc[(df['sex_id'] != 1)&(df['sex_id'] != 2), 'sex_id'] = 3


df.age_end = df.age_end + 1
df.loc[df.age_end == 2, 'age_end'] = 1

























diagnosis_feats = df.columns[df.columns.str.startswith('dx_')]

for feat in diagnosis_feats:
    df[feat] = sanitize_diagnoses(df[feat])



    


if len(diagnosis_feats) > 1:
    
    
    df = stack_merger(df)

elif len(diagnosis_feats) == 1:
    df.rename(columns={'dx_1': 'cause_code'}, inplace=True)
    df['diagnosis_id'] = 1

else:
    print("Something went wrong, there are no ICD code features")









print("Are there missing values in any row?\n")
null_condition = df.isnull().values.any()
if null_condition:
    warnings.warn(">> Yes.  ROWS WITH ANY NULL VALUES WILL BE LOST ENTIRELY")
else:
    print(">> No.")


group_vars = ['cause_code', 'diagnosis_id', 'sex_id', 'age_start',
              'age_end', 'year_start', 'year_end', 'location_id', 'nid',
              'age_group_unit', 'source', 'facility_id', 'code_system_id',
              'outcome_id', 'representative_id', 'metric_id']
df_agg = df.groupby(group_vars).agg({'val': 'sum'}).reset_index()






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


assert set(columns_before) == set(columns_after),\
    "You lost or added a column when reordering"
for i in range(len(hosp_frmat_feat)):
    assert hosp_frmat_feat[i] in df_agg.columns,\
        "%s is missing from the columns of the DataFrame"\
        % (hosp_frmat_feat[i])


for i in df_agg.drop(['cause_code', 'source', 'facility_id', 'outcome_id'],
                     axis=1, inplace=False).columns:
    
    
    assert df_agg[i].dtype != object, "%s should not be of type object" % (i)





assert len(df_agg['age_start'].unique()) == len(df_agg['age_end'].unique()),\
    "number of feature levels age start should match number of feature " +\
    r"levels age end"
assert len(df_agg['diagnosis_id'].unique()) <= 2,\
    "diagnosis_id should have 2 or less feature levels"
assert len(df_agg['sex_id'].unique()) == 3,\
    "There should only be two feature levels to sex_id"
assert len(df_agg['code_system_id'].unique()) <= 2,\
    "code_system_id should have 2 or less feature levels"
assert len(df_agg['source'].unique()) == 1,\
    "source should only have one feature level"
assert round(df_agg.val.sum(), 3) == round(val_sum, 3),\
    "some cases were lost"





write_path = root + r"FILEPATH"
write_hosp_file(df_agg, write_path, backup=True)



