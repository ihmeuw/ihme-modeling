
"""
Created on Mon Jan 23 17:32:28 2017
@author: USERNAME and USER

Format Nepal data including reshaping from wide format to long
"""
import pandas as pd
import platform
import numpy as np
import sys
import getpass


user = getpass.getuser()


prep_path = r"FILEPATH".format(user)
sys.path.append(prep_path)

from hosp_prep import *
import stage_hosp_prep


if platform.system() == "Linux":
    root = "FILENAME"
else:
    root = "FILEPATH"





fpath = root + r"FILENAME"\
               r"FILEPATH"
df10 = pd.read_excel(fpath,
                     sheet_name='2010-11')
df11 = pd.read_excel(fpath,
                     sheet_name='2011-12')
df13 = pd.read_excel(fpath,
                     sheet_name='2013-14')

df15 = pd.read_excel(root + "FILENAME"
                     r"FILENAME"\
                     r"FILEPATH", skiprows=3)

df15.drop("Unnamed: 2", axis=1, inplace=True)


age_groups = ['<28 days', '29 days - 1', '1 5', '5 15', '15 20', '20 30',
              '30 40', '40 50', '50 60', '60 125']



age_sex_groups = []
for age in age_groups:
    fem = age + " female"
    ma = age + " male"
    age_sex_groups.append(fem)
    age_sex_groups.append(ma)


def name_cols(df):
    
    df = df.iloc[:, 0:22]
    
    df.columns = ['cause_code', 'icd_name'] + age_sex_groups
    return(df)

df10 = name_cols(df10)
df11 = name_cols(df11)
df13 = name_cols(df13)
df15 = name_cols(df15)


df10 = df10.iloc[5:, :]
df11 = df11.iloc[3:, :]
df13 = df13.iloc[5:, :]


df10['year_start'] = 2010
df11['year_start'] = 2011
df13['year_start'] = 2013
df15['year_start'] = 2015


gt15 = df15.loc[df15.shape[0]-1,]

df15 = df15[df15.cause_code != "Grand Total"]

def reshape_long(df):
    
    for col in age_sex_groups:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    
    
    df = df.set_index(['cause_code', 'icd_name', 'year_start']).stack().\
        reset_index()
    df.rename(columns={'level_3': 'age_sex', 0: 'val'}, inplace=True)
    df['age_group_unit'] = 1
    
    
    
    df.loc[df.age_sex == '<28 days female', 'age_sex'] = '0 0.07671233 female'
    df.loc[df.age_sex == '<28 days male', 'age_sex'] = '0 0.07671233 male'
    df.loc[df.age_sex == '29 days - 1 female', 'age_sex'] = '0.07671233 1 female'
    df.loc[df.age_sex == '29 days - 1 male', 'age_sex'] = '0.07671233 1 male'
    
    
    splits = df.age_sex.str.split(" ", expand=True)
    splits.columns = ['age_start', 'age_end', 'sex_id']
    
    df = pd.concat([df, splits], axis=1)
    
    df.sex_id.replace(['male', 'female'], [1, 2], inplace=True)
    
    
    df.drop(['age_sex', 'icd_name'], axis=1, inplace=True)
    
    df['year_end'] = df['year_start'] + 1
    return(df)

df_list = []
for df in [df10, df11, df13, df15]:
    df_list.append(reshape_long(df))

df = pd.concat(df_list)

df.to_hdf(root + r"FILEPATH",
          key="df", mode='w')






df.reset_index(drop=True, inplace=True)


df = df[df.val != 0]



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
df['location_id'] = 164


df['source'] = 'NPL_HID'


df['code_system_id'] = 2


df['metric_id'] = 1

df['facility_id'] = 'hospital'
df['outcome_id'] = 'discharge'


nid_dictionary = {2010: 292436, 2011: 292435, 2013: 292437, 2015: 316184}
df = fill_nid(df, nid_dictionary)






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
    df[feat] = sanitize_diagnoses(df[feat])


df.dx_1 = df.dx_1.str.upper()



    


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




df_agg['cause_code'] = df_agg['cause_code'].astype(str)




compare_df = pd.read_hdf(root + "FILENAME"
                         "FILENAME"
                         "FILEPATH")

test_results = stage_hosp_prep.test_case_counts(df_agg, compare_df)
if test_results == "test_case_counts has passed!!":
    pass
else:
    msg = " --- ".join(test_results)
    assert False, msg


write_path = root + r"FILEPATH"
write_hosp_file(df_agg, write_path, backup=True)
