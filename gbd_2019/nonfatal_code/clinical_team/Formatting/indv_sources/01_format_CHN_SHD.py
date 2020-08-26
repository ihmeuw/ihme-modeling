
"""
Created on Mon Jan 23 17:32:28 2017
@author: USERNAME and USER

Template for formatting raw hospital data.  Follow the instructions in the
comments and ensure that the code is relevant to your particular source.

PLEASE put link to GHDx entry for the source here
"""
import pandas as pd
import platform
import numpy as np
import sys
import warnings
import glob
import getpass


user = getpass.getuser()

prep_path = r"FILEPATH".format(user)
sys.path.append(prep_path)

from hosp_prep import *


if platform.system() == "Linux":
    root = "FILENAME"
else:
    root = "FILEPATH"





inp_files = glob.glob(r"FILEPATH")


warnings.warn("need to continue prepping inp main diagnosis data. it appears to be an extension, not a subset"\
              " of total diagnosis. b/c the most common dx1 codes don't make sense, ie hypertension")
inp_main_files = glob.glob(r"FILEPATH")

otp_files = glob.glob(r"FILEPATH")


inp = pd.concat([pd.read_csv(f, sep='\t', encoding='GBK') for f in inp_files], ignore_index=True)

inp_main = pd.concat([pd.read_csv(f, sep='\t', encoding='GBK') for f in inp_main_files], ignore_index=True)


pre_cases = inp.jzcnt.sum()
inp['facility_id'] = 'hospital'

inp = inp[['sex', 'age', 'zdbm', 'flg_sh', 'jzcnt', 'jzpeople', 'facility_id', 'zyts_mean']]

inp = inp[inp.flg_sh != '非沪籍']
inp.drop('flg_sh', axis=1, inplace=True)

pre_main_cases = inp_main.jzcnt.sum()
inp_main['facility_id'] = 'hospital'

inp_main = inp_main[['sex', 'age', 'zdbm', 'flg_sh', 'jzcnt', 'jzpeople', 'facility_id', 'zyts_mean']]
inp_main.rename(columns={'zdbm': 'dx_1'}, inplace=True)

inp_main = inp_main[inp_main.flg_sh != '非沪籍']
inp_main.drop('flg_sh', axis=1, inplace=True)


num_cols = inp.columns.drop(['zdbm', 'facility_id'])
for col in num_cols:
    inp[col] = pd.to_numeric(inp[col], errors='coerce')
inp.isnull().sum() / inp.shape[0]

num_cols = inp_main.columns.drop(['dx_1', 'facility_id'])
for col in num_cols:
    inp_main[col] = pd.to_numeric(inp_main[col], errors='coerce')
inp_main.isnull().sum() / inp_main.shape[0]




assert inp.zyts_mean.min() >= 1
inp.drop('zyts_mean', axis=1, inplace=True)
inp_cases = inp.jzcnt.sum()

assert inp_main.zyts_mean.min() >= 1
inp_main.drop('zyts_mean', axis=1, inplace=True)
inp_main_cases = inp_main.jzcnt.sum()







otp = []
for f in otp_files:
    if "FILEPATH" in f:
        print("fixing the bad one")
        
        
        tmp = pd.read_csv(f, sep="\t", encoding="GBK", quoting=3)
    else:
        tmp = pd.read_csv(f, sep='\t', encoding='GBK')
    
    otp.append(tmp)

back = otp.copy()

otp = pd.concat(otp, ignore_index=True)
otp['facility_id'] = 'outpatient unknown'

assert otp.regname2.unique().size <= 34

otp = otp[['sex', 'age', 'zdbm', 'flg_sh', 'jzcnt', 'jzpeople', 'facility_id']]

otp = otp[otp.flg_sh != '非沪籍']
otp.drop('flg_sh', axis=1, inplace=True)

num_cols = otp.columns.drop(['zdbm', 'facility_id'])
for col in num_cols:
    otp[col] = pd.to_numeric(otp[col], errors='coerce')
otp.isnull().sum() / otp.shape[0]


df = pd.concat([inp, inp_main, otp], ignore_index=True)


col_names = ["dx_{}".format(i) for i in np.arange(2, 13, 1)]
dx_df = df.zdbm.str.split(";", expand=True)
dx_df.columns = col_names


assert df.shape[0] == dx_df.shape[0]
df = pd.concat([df, dx_df], axis=1)

df.drop('zdbm', axis=1, inplace=True)



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
    'sex': 'sex_id',
    'age': 'age',
    
    
    'age_group_unit': 'age_group_unit',
    'code_system_id': 'code_system_id',

    
    'outcome_id': 'outcome_id',
    'facility_id': 'facility_id',
    'jzcnt': 'val'}






df.rename(columns=hosp_wide_feat, inplace=True)



new_col_df = pd.DataFrame(columns=list(set(hosp_wide_feat.values()) -
                                       set(df.columns)))
df = df.join(new_col_df)
























df['representative_id'] = 1  
df['location_id'] = 514


df['age_group_unit'] = 1
df['source'] = 'CHN_SHD'


df['code_system_id'] = 2


df['outcome_id'] = "case"


df['metric_id'] = 1

df['year_start'] = 2016
df['year_end'] = 2016


nid_dictionary = {2016: 336860}
df = fill_nid(df, nid_dictionary)
































df.loc[df['age'] > 95, 'age'] = 95  

df = age_binning(df)


df.loc[(df['sex_id'] != 1)&(df['sex_id'] != 2), 'sex_id'] = 3


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



    

df.fillna("none", inplace=True)

diagnosis_feats = df.columns[df.columns.str.startswith('dx_')]

for feat in diagnosis_feats:
    df[feat] = sanitize_diagnoses(df[feat])
    
    df.loc[df[feat].str.contains("x$"), feat] =\
            df.loc[df[feat].str.contains("x$"), feat].str[:-1]
    
    df.loc[df[feat].str.contains("x$"), feat] =\
            df.loc[df[feat].str.contains("x$"), feat].str[:-1]







df[diagnosis_feats] = df[diagnosis_feats].replace("none", np.nan)





pre = df.val.sum()
diff = df[(df.facility_id == 'outpatient unknown') &
          (df.dx_2.isnull())].val.sum()

df = df[(df.facility_id != 'outpatient unknown') | (df.dx_2.notnull())]
assert pre - df.val.sum() == diff





pre = df.val.sum()


if len(diagnosis_feats) > 1:
    
    stack_idx = [n for n in df.columns if "dx_" not in n]
    
    len_idx = len(stack_idx)

    df = df.set_index(stack_idx).stack().reset_index()

    
    pre_dx1 = df[df["level_{}".format(len_idx)] == "dx_1"].shape[0]
    df = df[df[0] != "none"]
    diff = pre_dx1 - df[df["level_{}".format(len_idx)] == "dx_1"].shape[0]
    print("{} dx1 cases/rows were lost after dropping blanks".format(diff))

    df = df.rename(columns={"level_{}".format(len_idx): 'diagnosis_id',
                            0: 'cause_code'})

    df.loc[df['diagnosis_id'] != "dx_1", 'diagnosis_id'] = 2
    df.loc[df.diagnosis_id == "dx_1", 'diagnosis_id'] = 1

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


df_agg['cause_code'] = df_agg['cause_code'].str.upper()
df_agg['cause_code'] = df_agg.cause_code.str.replace("[^a-zA-Z0-9]", "")


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
assert len(df_agg['sex_id'].unique()) == 3,\
    "There should only be three feature levels to sex_id"
assert len(df_agg['code_system_id'].unique()) <= 2,\
    "code_system_id should have 2 or less feature levels"
assert len(df_agg['source'].unique()) == 1,\
    "source should only have one feature level"

assert (df.val >= 0).all(), ("for some reason there are negative case counts")

assert inp_cases == df[(df.facility_id == 'hospital') &
                       (df.diagnosis_id == 1)].val.sum(),\
                       "Inpatient primary case counts have changed"
assert inp_cases == df_agg[(df_agg.facility_id == 'hospital') &
                           (df_agg.diagnosis_id == 1)].val.sum(),\
                           "Inpatient primary case counts have changed"





write_path = root + r"FILEPATH"

write_hosp_file(df_agg, write_path, backup=True)
