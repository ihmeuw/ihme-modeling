
"""
Created on Mon Jan 23 17:32:28 2017
@author: USERNAME and USER

New data was provided towards the end of GBD2017, but not incorporated.
Looks like the formatting script wasn't entirely complete. The data is in two
excel sheets

URL
"""
import pandas as pd
import platform
import numpy as np
import sys
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







sheetnames = ['Stockholm', 'Rest of Sweden']
filepath = root + r"FILEPATH"
tmp_list = [pd.read_excel(filepath, sheet_name=sheet) for sheet in sheetnames]
df_raw = pd.concat(tmp_list, sort=False, ignore_index=True)

df = df_raw.copy()


assert df.shape[0] == len(df.index.unique()), ("index is not unique, " +
    "the index has a length of " + str(len(df.index.unique())) +
    " while the DataFrame has " + str(df.shape[0]) + " rows" +
    "try this: df = df.reset_index(drop=True)")

warnings.warn("\n\n\nWe're about to drop the 'Unique' column which has admissions "
              "adjusted for re-admission. it doesn't fit our current inpatient "
              "process but might be useful in the future")

keep = ['Country', 'Region', 'Year', 'Diagnos',
        'Sex', 'Age', 'Encounters', 'Reg']
df = df[keep]




hosp_wide_feat = {
    'nid': 'nid',
    'location_id': 'location_id',
    'representative_id': 'representative_id',
    'Year': 'year',
    'year_start': 'year_start',
    'year_end': 'year_end',
    'Sex': 'sex_id',
    'Age': 'age',
    'age_start': 'age_start',
    'age_end': 'age_end',
    'age_group_unit': 'age_group_unit',
    'code_system_id': 'code_system_id',

    
    'outcome_id': 'outcome_id',
    'Reg': 'facility_id',
    
    'Diagnos': 'dx_1',
    
    'Encounters': 'val'}


df.rename(columns=hosp_wide_feat, inplace=True)




new_col_df = pd.DataFrame(columns=list(set(hosp_wide_feat.values()) -
                                       set(df.columns)))
df = df.join(new_col_df)
























df['representative_id'] = 1  

df['location_id'] = 93  

pre = df.age.value_counts().reset_index()

replace_dict = {'0-6day': '0-.01917808',
                '07-27day': '.01917808-.07671233',
                '28-364day': '.07671233-1'}
for key in list(replace_dict.keys()):
    df.loc[df['age'] == key, 'age'] = replace_dict[key]
assert (df.age.value_counts().reset_index().age == pre.age).all(),\
    "renaming age values changed the counts"


df['age_group_unit'] = 1
df['source'] = 'SWE_Patient_Register_15_16'


df['code_system_id'] = 2


df['outcome_id'] = "case"


df['metric_id'] = 1


df['year'].replace([16,15],[2016, 2015], inplace = True)

df['year_start'] = df['year']
df['year_end'] = df['year']


nid_dictionary = {2015: 333649, 2016:333650}

df['nid'] = df['year_start'].map(nid_dictionary)

























df['sex_id'].replace(['Male','Female'],[1,2], inplace = True)


df['facility_id'].replace(['In', 'Out'],['inpatient unknown', 'outpatient unknown'],
                        inplace = True)






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







df['age'] = df['age'].apply(lambda x: x.split('-')[0] if '-' in x else x)
df.loc[df['age'] == '95+', 'age'] = 95
df['age'] = df['age'].apply(pd.to_numeric)

df = stage_hosp_prep.age_binning(df, allow_neonatal_bins=True)


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
assert len(df_agg['sex_id'].unique()) == 2,\
    "There should only be two feature levels to sex_id"
assert len(df_agg['code_system_id'].unique()) <= 2,\
    "code_system_id should have 2 or less feature levels"
assert len(df_agg['source'].unique()) == 1,\
    "source should only have one feature level"
















df_agg['cause_code'] = df_agg['cause_code'].apply(lambda x: x.decode('unicode_escape').\
                                          encode('ascii', 'ignore').\
                                          strip())


write_path = root +  r"FILENAME"\
                    r"FILEPATH"

write_hosp_file(df_agg, write_path, backup=True)
