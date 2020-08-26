
"""
Created on Mon Jan 23 17:32:28 2017
@author: USERNAME and USER

Script for formatting China data from

"""
import pandas as pd
import platform
import numpy as np
import sys
import getpass
from db_tools.ezfuncs import query


if platform.system() == "Linux":
    root = "FILENAME"
else:
    root = "FILEPATH"

user = getpass.getuser()

hosp_path = r"FILEPATH".format(user)
sys.path.append(hosp_path)

from hosp_prep import *






column_names = ['Administrative division', 'Disease code', 'Age',
                'Discharge method', 'Sex', 'Number of people discharged']

df_list = []
years = [2013, 2014, 2015]


for year in years:
    df = pd.read_csv(root + r"FILENAME"
        r"FILENAME"
        r"FILENAME"
        r"CHN_NHSIRS_" + str(year) + "FILEPATH",
        encoding='utf-8', skiprows=[0], names=column_names)

    df['year_start'] = year
    df['year_end'] = year
    df_list.append(df)
df = pd.concat(df_list)



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

    
    'Discharge method': 'outcome_id',
    'facility_id': 'facility_id',
    'Number of people discharged': 'val',
    
    'Disease code': 'dx_1'}


df.rename(columns=hosp_wide_feat, inplace=True)



new_col_df = pd.DataFrame(columns=list(set(hosp_wide_feat.values()) -
                                       set(df.columns)))
df = df.join(new_col_df)

total = df.val.sum()






df['representative_id'] = 1  
df['diagnosis_id'] = 1


df['age_group_unit'] = 1
df['source'] = 'CHN_NHSIRS'


df['code_system_id'] = 2


df['facility_id'] = 'hospital'


df['metric_id'] = 1


nid_dictionary = {2013: 282493, 2014: 282496, 2015: 282497}
df = fill_nid(df, nid_dictionary)






df.loc[df['Age'] == '95S', 'Age'] = '95_99S'
df.loc[df['Age'] == '1_S', 'Age'] = '0_1S'

df['Age'] = df['Age'].str.replace("S", "")

df['age_start'], df['age_end'] = df['Age'].str.split("_", 1).str


df.drop('Age', axis=1, inplace=True)

assert df.val.sum() == total, "cases were lost"











df.loc[(df.sex_id != 1) & (df.sex_id != 2), 'sex_id'] = 3
df['sex_id'] = df['sex_id'].astype(int)














df['outcome_id'] = np.where(df['outcome_id'] == 5, 'death', 'discharge')




df.dx_1.fillna("cc_code", inplace=True)

assert df.val.sum() == total, "cases were lost"






admin_map = query(QUERY)
admin_map = admin_map[0:33]



df.drop('location_id', axis=1, inplace=True)

df['map_id'] = df['Administrative division']

admin_map['map_id'] = admin_map['map_id'].astype(str)
df['map_id'] = df['map_id'].astype(str)

admin_map['map_id'] = admin_map['map_id'].str[0:2]
df['map_id'] = df['map_id'].str[0:2]


df = df.merge(admin_map, how='left', on='map_id')

assert df.location_name.isnull().sum() == 0, "Null values were introduced"
assert (df.location_name=="").sum() == 0, "blank values were introduced"

df.drop(['map_id', 'Administrative division', 'location_name'], axis=1, inplace=True)


diagnosis_feats = df.columns[df.columns.str.startswith('dx_')]

for feat in diagnosis_feats:
    df[feat] = sanitize_diagnoses(df[feat])


df.loc[df.dx_1 == "", 'dx_1'] = "cc_code"


int_cols = ['location_id', 'year_start', 'year_end', 'age_group_unit',
            'age_start', 'age_end', 'sex_id', 'nid', 'representative_id',
            'metric_id']

for col in int_cols:
    df[col] = pd.to_numeric(df[col], errors='raise')


df.loc[df.age_end > 1, 'age_end'] = df.loc[df.age_end > 1, 'age_end'] + 1



    


if len(diagnosis_feats) > 1:
    
    
    df = stack_merger(df)

elif len(diagnosis_feats) == 1:
    df.rename(columns={'dx_1': 'cause_code'}, inplace=True)
    df['diagnosis_id'] = 1

else:
    print("Something went wrong, there are no ICD code features")

assert df.val.sum() == total, "cases were lost"





print("Are there missing values in any row?\n")
null_condition = df.isnull().values.any()
if null_condition:
    print(">> Yes.")
else:
    print(">> No.")


group_vars = ['cause_code', 'diagnosis_id', 'sex_id', 'age_start',
              'age_end', 'year_start', 'year_end', 'location_id', 'nid',
              'age_group_unit', 'source', 'facility_id', 'code_system_id',
              'outcome_id', 'representative_id', 'metric_id']
df_agg = df.groupby(group_vars).agg({'val': 'sum'}).reset_index()






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

assert df_agg.val.sum() == total, "cases were lost"


assert len(hosp_frmat_feat) == len(df_agg.columns),\
    "The DataFrame has the wrong number of columns"
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
assert len(df_agg['sex_id'].unique()) == 3,\
    "There should only be two feature levels to sex_id"
assert len(df_agg['code_system_id'].unique()) <= 2,\
    "code_system_id should have 2 or less feature levels"
assert len(df_agg['source'].unique()) == 1,\
    "source should only have one feature level"






df_agg['cause_code'] = df_agg['cause_code'].astype(str)
write_path = root + "FILEPATH"

write_hosp_file(df_agg, write_path, backup=True)
