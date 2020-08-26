
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
import glob
import getpass
import warnings


user = getpass.getuser()
prep_path = r"FILEPATH".format(user)


sys.path.append(prep_path)

from hosp_prep import *
import stage_hosp_prep


if platform.system() == "Linux":
    root = "FILENAME"
else:
    root = "FILEPATH"





filepath = root + "FILEPATH"
years = np.arange(2010, 2016)
df_list = []
for e in years:
    f = "FILEPATH".format(pre=filepath, year=e)
    f_list = glob.glob("FILEPATH".format(base=f,year=e))
    df = pd.read_stata(f_list[0])
    df['year'] = e
    df_list.append(df)

df = pd.concat(df_list)


df = df.reset_index(drop=True)
assert df.shape[0] == len(df.index.unique()), ("index is not unique, " +
    "the index has a length of " + str(len(df.index.unique())) +
    " while the DataFrame has " + str(df.shape[0]) + " rows" +
    "try this: df = df.reset_index(drop=True)")


keep = df.columns.tolist()
df = df[keep]

df['year'] = pd.to_numeric(df['year'], downcast='integer')



hosp_wide_feat = {
    'nid': 'nid',
    
    'representative_id': 'representative_id',
    
    'year': 'year_start',
    'year_end': 'year_end',
    'female': 'sex_id',
    'age_groups': 'age',
    'age_start': 'age_start',
    'age_end': 'age_end',
    'age_group_unit': 'age_group_unit',
    'code_system_id': 'code_system_id',

    
    'outcome_id': 'outcome_id',
    'facility_id': 'facility_id',
    
    'diagnosis_1': 'dx_1',
    'count': 'val'}


df.rename(columns=hosp_wide_feat, inplace=True)
df['year_end'] = df['year_start']



new_col_df = pd.DataFrame(columns=list(set(hosp_wide_feat.values()) -
                                       set(df.columns)))
df = df.join(new_col_df)
























df['representative_id'] = 3  




df['age_group_unit'] = df.age.apply(lambda x: 1 if x.endswith('years') else 2)

df['source'] = 'JPN_DPC'


df['code_system_id'] = 2


df['outcome_id'] = df['outcome'].str.lower()
df.drop('outcome', 1)


df['metric_id'] = 1

df['facility_id'] = 'hospital'


nid_dictionary = {2015: 336200, 2014: 336199, 2013: 336198, 2012: 336197,
                2011: 336195, 2010: 336193}

df['nid'] = df['year_start'].map(nid_dictionary)


























path_map = root + "FILEPATH"
loc_map = pd.read_csv(path_map + "FILEPATH")
df = df.merge(loc_map, how='left', on='prefecture_id')
cols = ['location_name', 'prefecture_id']
df = df.drop(cols, 1)


df['sex_id'].replace([0, 1], [1, 2], inplace = True)


df['dx_1'] = df['dx_1'].str.upper()






int_cols = ['location_id', 'year_start', 'year_end', 'age_group_unit', 
            'age_start', 'age_end', 'sex_id', 'nid', 'representative_id',
            'metric_id']




str_cols = ['source', 'facility_id', 'outcome_id']




if df[str_cols].isnull().any().any():
    warnings.warn("\n\n There are NaNs in the column(s) {}".
                  format(df[str_cols].columns[df[str_cols].isnull().any()]) +
                  "\n These NaNs will be converted to the string 'nan' \n")

for col in int_cols:
    df[col] = pd.to_numeric(df[col], errors='raise', downcast='integer')
for col in str_cols:
    df[col] = df[col].astype(str)







df['age'] = df.age.str.split('_',expand=True)[1]
df.loc[df['age'] == '95+years', 'age'] = 95
df['age'] = df['age'].apply(pd.to_numeric)


unit_dict = {'days': 2, 'months': 3, 'years': 1}
df = stage_hosp_prep.convert_age_units(df, unit_dict)

df = stage_hosp_prep.age_binning(df, allow_neonatal_bins=True)
df.drop('age', 1)


df.loc[(df['sex_id'] != 1)&(df['sex_id'] != 2), 'sex_id'] = 3






    


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




compare_df = pd.read_hdf(root + "FILENAME"
                         "FILENAME"
                         "FILEPATH")
test_results = stage_hosp_prep.test_case_counts(df_agg, compare_df)
if test_results == "test_case_counts has passed!!":
    pass
else:
    msg = " --- ".join(test_results)
    assert False, msg





write_path = root + r"FILENAME"\
                    r"FILEPATH"

write_hosp_file(df_agg, write_path, backup=True)