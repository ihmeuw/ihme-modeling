
"""
Created on Mon Jan 23 17:32:28 2017
@author: USERNAME and USER

Template for formatting raw hospital data.  Follow the instructions in the
comments and ensure that the code is relevant to your particular source.

URL
"""
import pandas as pd
import platform
import numpy as np
import sys


USER_path = r"FILEPATH"
USER_path = r"FILEPATH"
sys.path.append(USER_path)
sys.path.append(USER_path)

import hosp_prep


if platform.system() == "Linux":
    root = "FILENAME"
else:
    root = "FILEPATH"


df02 = pd.read_excel(r"FILEPATH", header=1)
df03 = pd.read_excel(r"FILEPATH", header=1)

df03w = pd.read_excel(r"FILEPATH", header=1)

df02['year_start'] = 2002
df03['year_start'] = 2003
df03w['year_start'] = 2003
df03w['SEX'] = 'W'


def reshape_qatar(df):
    
    df['ICD 9 CODE'] = df['ICD 9 CODE'].fillna(method='ffill')

    
    df.dropna(subset=['NAT', 'SEX'], inplace=True)
    assert sorted(df.SEX.unique()) == sorted(['F', 'M'])
    assert sorted(df.NAT.unique()) == sorted(['Q', 'NQ'])
    age_cols = df.filter(regex="^[0-9]").columns

    for col in age_cols:
        df[col] = pd.to_numeric(df[col], errors='raise')

    df['sum_calc'] = df[age_cols].sum(axis=1)
    assert (df.SUM == df.sum_calc).all()
    
    df.drop(axis=1, labels=["DISHCARGE DIAGNOSIS", "SUM", "sum_calc",
                            "AVG LOS", "NO. OF DEATHS"], inplace=True)

    df.rename(columns={'ICD 9 CODE': 'cause_code', 'NAT': 'nat',
                       'SEX': 'sex_id'},
                       inplace=True)

    df = df.set_index(['cause_code', 'year_start', 'nat', 'sex_id']).stack().\
        reset_index()
    df.rename(columns={'level_4': 'age', 0: 'val'}, inplace=True)

    
    df.loc[df.age == "85+", "age"] = "85-124"  

    
    df.val = pd.to_numeric(df.val)

    
    df = df[df.val != 0]

    df['age_start'], df['age_end'] = df['age'].str.split("-", 1).str

    
    df.drop(['age', 'nat'], axis=1, inplace=True)

    pre = df.cause_code.value_counts()
    pre.index = pre.index.astype(str)
    pre = pre.sort_index()
    
    df['cause_code'] = df['cause_code'].astype(str)
    df['cause_code'] = df['cause_code'].str.upper()
    post = df.cause_code.value_counts().sort_index()
    assert (pre == post).all()

    return(df)


df_list = []
for df in [df02, df03]:
    df_list.append(reshape_qatar(df))

df = pd.concat(df_list)



df.age_end = pd.to_numeric(df.age_end, errors='raise') + 1

df.to_hdf(root + r"FILENAME"
          r"FILEPATH",
          key="df")








df.reset_index(drop=True, inplace=True)
assert df.shape[0] == len(df.index.unique()), ("index is not unique, " +
    "the index has a length of " + str(len(df.index.unique())) +
    " while the DataFrame has " + str(df.shape[0]) + " rows" +
    "try this: df = df.reset_index(drop=True)")




hosp_wide_feat = {
    'nid': 'nid',
    'location_id': 'location_id',
    'representative_id': 'representative_id',
    
    'year_start': 'year_start',
    'year_end': 'year_end',
    'sex_id': 'sex_id',
    
    'age_start': 'age_start',
    'age_end': 'age_end',
    'age_group_unit': 'age_group_unit',
    'code_system_id': 'code_system_id',

    
    'outcome_id': 'outcome_id',
    'facility_id': 'facility_id',
    
    'cause_code': 'dx_1'}


df.rename(columns=hosp_wide_feat, inplace=True)



new_col_df = pd.DataFrame(columns=list(set(hosp_wide_feat.values()) -
                                       set(df.columns)))
df = df.join(new_col_df)























df['representative_id'] = 3  
df['location_id'] = 151


df['age_group_unit'] = 1
df['source'] = 'QAT_AIDA'


df['code_system_id'] = 1


df['metric_id'] = 1

df['facility_id'] = 'hospital'
df['year_end'] = df['year_start']
df['outcome_id'] = 'discharge'


nid_dictionary = {2002: 68367, 2003: 68535}
df = hosp_prep.fill_nid(df, nid_dictionary)




df['sex_id'].replace(['M', 'F'], [1, 2], inplace=True)



int_cols = ['location_id', 'year_start', 'year_end', 'age_group_unit',
            'age_start', 'age_end', 'sex_id', 'nid', 'representative_id',
            'metric_id']




str_cols = ['source', 'facility_id', 'outcome_id']

for col in int_cols:
    df[col] = pd.to_numeric(df[col], errors='raise', downcast='integer')
for col in str_cols:
    df[col] = df[col].astype(str)


























diagnosis_feats = df.columns[df.columns.str.startswith('dx_')]

for feat in diagnosis_feats:
    df[feat] = hosp_prep.sanitize_diagnoses(df[feat])



    


if len(diagnosis_feats) > 1:
    
    
    df = hosp_prep.stack_merger(df)

elif len(diagnosis_feats) == 1:
    df.rename(columns={'dx_1': 'cause_code'}, inplace=True)
    df['diagnosis_id'] = 1

else:
    print("Something went wrong, there are no ICD code features")









print("Are there missing values in any row?\n")
null_condition = df.isnull().values.any()
if null_condition:
    print(">> Yes.  ROWS WITH ANY NULL VALUES WILL BE LOST ENTIRELY")
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






write_path = root + r"FILENAME" +\
        r"FILEPATH"
hosp_prep.write_hosp_file(df_agg, write_path, backup=True)
