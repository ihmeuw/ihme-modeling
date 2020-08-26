
"""
Created on Wed Jun 27 17:32:28 2018
@author: USERNAME and USER and Zichen Liu

Formatting raw hospital data from GEO Hospital Discharges
"""
import pandas as pd
import platform
import numpy as np
import sys
import warnings
import getpass
import getpass


user = getpass.getuser()
prep_path = r"FILEPATH".format(user)
sys.path.append(prep_path)
from hosp_prep import *


if platform.system() == "Linux":
    root = "FILENAME"
else:
    root = "FILEPATH"





filepath = root + r"FILEPATH"\
            r"FILENAME"
files = glob.glob(filepath + "FILEPATH")

df_list = []
for each_file in files:
    df = pd.read_excel(each_file)
    df['year_start'] = df['year_end'] = int(each_file.split('.')[0].split('_')[5])
    df_list.append(df)
df = pd.concat(df_list)


df = df.reset_index(drop=True)
assert df.shape[0] == len(df.index.unique()), ("index is not unique, " +
    "the index has a length of " + str(len(df.index.unique())) +
    " while the DataFrame has " + str(df.shape[0]) + " rows" +
    "try this: df = df.reset_index(drop=True)")

keep = ['year_start', 'year_end', 'Sex', 'Age (years)', 'Disharge status', 'Main Diagnosis (ICD10)',
       'External causes (ICD10)', 'Complication (ICD 10)', 'Comorbidity (ICD 10)']
df = df[keep]




hosp_wide_feat = {
    'nid': 'nid',
    'location_id': 'location_id',
    'representative_id': 'representative_id',
    'year_start': 'year_start',
    'year_end': 'year_end',
    'Sex': 'sex_id',
    'Age (years)': 'age',
    'age_group_unit': 'age_group_unit',
    'code_system_id': 'code_system_id',
    
    'Disharge status': 'outcome_id',
    'facility_id': 'facility_id',
    
    'Main Diagnosis (ICD10)': 'dx_1',
    'External causes (ICD10)': 'dx_2',
    'Complication (ICD 10)': 'dx_3',
    'Comorbidity (ICD 10)': 'dx_4'}


df.rename(columns=hosp_wide_feat, inplace=True)



new_col_df = pd.DataFrame(columns=list(set(hosp_wide_feat.values()) -
                                       set(df.columns)))
df = df.join(new_col_df)

















df['representative_id'] = 3  


df['age_group_unit'] = 1
df['location_id'] = 35
df['source'] = 'GEO_Discharge_16_17'


df['code_system_id'] = 2


df['metric_id'] = 1
df['facility_id'] = 'inpatient unknown'


nid_dictionary = {2016: 319414, 2017: 354240} 
df['nid'] = df['year_start'].map(nid_dictionary)








df['outcome_id'].replace([1, 2, 3, 4], ['discharge', 'discharge',
                            'discharge', 'death'], inplace = True)


df.loc[~df['sex_id'].isin([1, 2]), 'sex_id'] = 3


df['dx_1'] = df['dx_1'].str.upper()
df['dx_2'] = df['dx_2'].str.upper()
df['dx_3'] = df['dx_3'].str.upper()
df['dx_4'] = df['dx_4'].str.upper()





int_cols = ['location_id', 'year_start', 'year_end', 'age_group_unit',
            'age', 'sex_id', 'nid', 'representative_id',
            'metric_id']

str_cols = ['source', 'facility_id', 'outcome_id', 'dx_1', 'dx_2', 'dx_3', 'dx_4']


diagnosis_feats = df.columns[df.columns.str.startswith('dx_')]
for feat in diagnosis_feats:
    df[feat] = sanitize_diagnoses(df[feat])

for col in int_cols:
    df[col] = pd.to_numeric(df[col], errors='raise', downcast='integer')
for col in str_cols:
    df[col] = df[col].astype(str)






df = age_binning(df)
df = df.drop('age', 1)


df = swap_ecode(df, icd_vers=10)






diagnosis_feats = df.columns[df.columns.str.startswith('dx_')]


for feat in diagnosis_feats:
    df[feat] = sanitize_diagnoses(df[feat])


write_path = r"FILEPATH"
write_hosp_file(df, write_path, backup=True)

write_path = r"FILEPATH"
write_hosp_file(df, write_path, backup=True)


if len(diagnosis_feats) > 1:
    
    stack_idx = [n for n in df.columns if "dx_" not in n]
    len_idx = len(stack_idx)
    df = df.set_index(stack_idx).stack().reset_index()
    df = df.rename(columns={"level_{}".format(len_idx): 'diagnosis_id', 0: 'cause_code'})
    df.loc[df['diagnosis_id'] != "dx_1", 'diagnosis_id'] = 2
    df.loc[df.diagnosis_id == "dx_1", 'diagnosis_id'] = 1
elif len(diagnosis_feats) == 1:
    df.rename(columns={'dx_1': 'cause_code'}, inplace=True)
    df['diagnosis_id'] = 1
else:
     print("Something went wrong, there are no ICD code features")


df.loc[(df['cause_code'] == 'nan'), 'cause_code'] = 'cc_code'


df['val'] = 1





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


assert len(df_agg['year_start'].unique()) == len(df_agg['nid'].unique()),\
    "number of feature levels of years and nid should match number"
assert len(df_agg['age_start'].unique()) == len(df_agg['age_end'].unique()),\
    "number of feature levels age start should match number of feature " +\
    r"levels age end"
assert len(df_agg['diagnosis_id'].unique()) <= 2,\
    "diagnosis_id should have 2 or less feature levels"


good_ids = [1, 2, 3]
good_in_data = [n for n in good_ids if n in df_agg.sex_id.unique()]
assert set(df_agg['sex_id'].unique()).\
    symmetric_difference(set(good_in_data)) == set(),\
    "There are unexpected sex_id values {} in the df".\
    format(df_agg.sex_id.unique())

assert len(df_agg['code_system_id'].unique()) <= 2,\
    "code_system_id should have 2 or less feature levels"
assert len(df_agg['source'].unique()) == 1,\
    "source should only have one feature level"

assert (df.val >= 0).all(), ("for some reason there are negative case counts")





write_path = root + r"FILENAME"\
                    r"FILEPATH"

write_hosp_file(df_agg, write_path, backup=True)





file_path = "FILEPATH"
table = pd.read_csv(file_path)


mnid = pd.read_excel("FILEPATH")
mnid = mnid[['nid', 'merged_nid']]


source_name = df_agg.source.iloc[0]


year_list = df_agg.year_start.unique()
pairs = [(x, y) for x in year_list for y in df_agg.loc[df_agg['year_start'] == x]['nid'].unique()]


for (year_id, nid) in pairs:
    table = update_source_table(table=table, mnid=mnid, source_name=source_name, \
    year_id=year_id, nid=nid, is_active=1, active_type='inpatient')

table.to_csv(file_path, index=False)
