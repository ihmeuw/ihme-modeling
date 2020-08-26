
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
import warnings
import getpass



user = getpass.getuser()
prep_path = r"FILEPATH".format(user)
sys.path.append(prep_path)
repo = r"FILEPATH".format(user)

from hosp_prep import *


if platform.system() == "Linux":
    root = "FILENAME"
else:
    root = "FILEPATH"

warnings.warn("""

              THIS SOURCE DOES NOT HAVE ICD CODES!!! It has 312 proprietary
              codes that MAPMASTER developed a map for.  The column cause_code
              contains the disease codes.  In the mapping step we map them to
              baby sequelae.

              """)







fpath =root + r"FILENAME" +\
        r"FILEPATH"
df = pd.read_excel(fpath, sheetname="Year 2013")




df.columns = ['disease_code', 'disease_name', 'otp_total', 'otp_female',
              'otp-cases-0-15', 'otp-deaths-0-125', 'inp_total', 'inp_female',
              'inp_total_deaths', 'inp_female_deaths', 'inp_under_15',
              'inp-cases-0-5', 'inp_under_15_deaths', 'inp-deaths-0-5']
df = df.iloc[3:,:]

inp_total_cases = df.inp_total.sum()
otp_total_cases = df.otp_total.sum()
total_cases = df.inp_total.sum() + df.otp_total.sum()

cols = list(df.columns)
cols.remove('disease_name')
for col in cols:
    df[col] = pd.to_numeric(df[col], errors='raise')
    df[col] = df[col].fillna(0)



df['inp-cases-5-15'] = df['inp_under_15'] - df['inp-cases-0-5']
df['inp-cases-15-125'] = df['inp_total'] - df['inp_under_15']

df['inp-deaths-5-15'] = df['inp_under_15_deaths'] - df['inp-deaths-0-5']
df['inp-deaths-15-125'] = df['inp_total_deaths'] - df['inp_under_15_deaths']

df['otp-cases-15-125'] = df['otp_total'] - df['otp-cases-0-15']



neg_list = []
constructed_cols = ['inp-cases-5-15', 'inp-cases-15-125', 'inp-deaths-5-15', 'inp-deaths-15-125', 'otp-cases-15-125']
for col in constructed_cols:
    if (df[col] < 0).sum() > 0:
        neg_list.append(df[df[col] < 0])
negs = pd.concat(neg_list)











df['otp_male'] = df['otp_total'] - df['otp_female']
df['inp_male'] = df['inp_total'] - df['inp_female']
df['inp_male_deaths'] = df['inp_total_deaths'] - df['inp_female_deaths']

df = df[['disease_code', 'disease_name',
         'otp_total',
         'inp_total',
         'inp_total_deaths']].copy()

df.rename(columns={'otp_total': 'otp_cases', 'inp_total': 'inp_cases',
                   'inp_total_deaths': 'inp_deaths'}, inplace=True)





df = df.set_index(['disease_code', 'disease_name']).stack().\
    reset_index()
df.rename(columns={'level_2': 'facility_outcome', 0: 'val'}, inplace=True)





df['facility_id'], df['outcome_id'] = df['facility_outcome'].str.split("_", 1).str
df.drop('facility_outcome', axis=1, inplace=True)

df['age_start'], df['age_end'] = 0, 125

assert total_cases == df[df['outcome_id'] != 'deaths'].val.sum()




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
    
    'disease_code': 'dx_1'}


df.rename(columns=hosp_wide_feat, inplace=True)



new_col_df = pd.DataFrame(columns=list(set(hosp_wide_feat.values()) -
                                       set(df.columns)))
df = df.join(new_col_df)
























df['representative_id'] = 3  
df['location_id'] = 20

df['sex_id'] = 3
df['year_start'], df['year_end'] = [2013, 2013]


df['age_group_unit'] = 1
df['source'] = 'VNM_MOH'


df['code_system_id'] = 3


df['metric_id'] = 1


df['nid'] = 299375






int_cols = ['location_id', 'year_start', 'year_end', 'age_group_unit',
            'age_start', 'age_end', 'sex_id', 'nid', 'representative_id',
            'metric_id']




str_cols = ['dx_1', 'source', 'facility_id', 'outcome_id']




if df[str_cols].isnull().any().any():
    warnings.warn("There are Nulls in one of the columns {}".format(str_cols),
                  " These nulls will be converted to the string 'nan'")

for col in int_cols:
    df[col] = pd.to_numeric(df[col], errors='raise', downcast='integer')
for col in str_cols:
    df[col] = df[col].astype(str)









































diagnosis_feats = df.columns[df.columns.str.startswith('dx_')]



df.drop("disease_name", axis=1, inplace=True)

df['outcome_id'] = df.outcome_id.replace({"cases":"case", "deaths": "death"})

df['facility_id'] = df.facility_id.replace({"otp": "outpatient unknown",
                                            "inp": "inpatient unknown"})


df = df[df.outcome_id != 'death']



df = df[df.val > 0]









    


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
assert len(df_agg['sex_id'].unique()) <= 2,\
    "There should only be two feature levels to sex_id"
assert len(df_agg['code_system_id'].unique()) <= 2,\
    "code_system_id should have 2 or less feature levels"
assert len(df_agg['source'].unique()) == 1,\
    "source should only have one feature level"
assert total_cases == df_agg.val.sum(),\
    "some cases were lost"
assert df[df.facility_id=='inpatient unknown'].val.sum() == inp_total_cases,\
    "some inpatient cases were lost"







write_path = (r"FILEPATH"
              r"FILEPATH".format(root))
write_hosp_file(df_agg, write_path, backup=True)
