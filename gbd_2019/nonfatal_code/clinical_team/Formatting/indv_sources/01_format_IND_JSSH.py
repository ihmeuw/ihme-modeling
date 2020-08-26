
"""
Created on Mon Jan 23 17:32:28 2017
@author: USERNAME and USER

Template for formatting raw hospital data.  Follow the instructions in the
comments and ensure that the code is relevant to your particular source.

URL
URL
URL
URL
"""
import pandas as pd
import platform
import numpy as np
import sys
import warnings
import getpass

user = getpass.getuser()


local_path = r"FILEPATH"
sys.path.append(local_path)

import hosp_prep
import stage_hosp_prep


if platform.system() == "Linux":
    root = "FILENAME"
else:
    root = "FILEPATH"






fpath = r"FILEPATH"\
    r"FILEPATH"
sheetnames = ["2014", "2015", "2016", "2017"]

sheetlist = [pd.read_excel(fpath, sheetname=s) for s in sheetnames]

df = pd.concat(sheetlist, ignore_index=True)

start_cases = df.shape[0]



assert df.shape[0] == len(df.index.unique()), ("index is not unique, " +
    "the index has a length of " + str(len(df.index.unique())) +
    " while the DataFrame has " + str(df.shape[0]) + " rows" +
    "try this: df = df.reset_index(drop=True)")




hosp_wide_feat = {
    'nid': 'nid',
    'location_id': 'location_id',
    'representative_id': 'representative_id',
    
    'Year': 'year_start',
    
    'Gender': 'sex_id',
    'Age_Unit': 'age',
    
    
    'Age_Value': 'age_group_unit',
    'code_system_id': 'code_system_id',

    
    'Outcome': 'outcome_id',
    'Facility Type': 'facility_id',
    
    'Diagnosis_1': 'dx_1',
    'Diagnosis_2': 'dx_2'}


df.rename(columns=hosp_wide_feat, inplace=True)



new_col_df = pd.DataFrame(columns=list(set(hosp_wide_feat.values()) -
                                       set(df.columns)))
df = df.join(new_col_df)


cond = "(df['age_group_unit'] == 'Days') & (df['age'] < 0)"
df.loc[eval(cond), 'age'] = abs(df.loc[eval(cond), 'age'])

df['age_group_name'] = df['age_group_unit']
df['age_group_unit'] = 1
df.loc[df['age_group_name'] == 'Days', 'age_group_unit'] = 2
assert not set(['Days', 'Year']).\
    symmetric_difference(set(df['age_group_name'].unique())),\
    "There are other ages groups that need review"
df.drop('age_group_name', axis=1, inplace=True)
unit_dict = {'days': 2, 'years': 1}



    
    

warnings.warn("We're setting the 365 day olds to 364 days to "\
              "continue an error from decomp1 and gbd2017")
df.loc[(df['age_group_unit'] == unit_dict['days']) &\
       (df['age'] == 365), 'age'] = 364


assert df.loc[df.age_group_unit == 2, 'age'].max() <= 366

df = stage_hosp_prep.convert_age_units(df, unit_dict)
























df['representative_id'] = 3  
df['location_id'] = 4856


df['age_group_unit'] = 1
df['source'] = 'IND_JSSH'


df['code_system_id'] = 2



df.loc[df.outcome_id != "EXPIRED", 'outcome_id'] = 'discharge'
df.loc[df.outcome_id == "EXPIRED", 'outcome_id'] = 'death'


assert (df.facility_id.unique() == np.array('Inpatient')).all()
df['facility_id'] = 'hospital'


df['metric_id'] = 1

df['year_end'] = df['year_start']


nid_dictionary = {2014: 333358, 2015: 333359, 2016: 333360, 2017: 333361}
df = hosp_prep.fill_nid(df, nid_dictionary)














df['sex_id'].replace(['M', 'F'], [1, 2], inplace = True)
df.loc[~df.sex_id.isin([1, 2]), 'sex_id'] = 3















int_cols = ['location_id', 'year_start', 'year_end', 'age_group_unit', 'age',
            'sex_id', 'nid', 'representative_id', 'metric_id']




str_cols = ['source', 'facility_id', 'outcome_id', 'dx_1']




if df[str_cols].isnull().any().any():
    warnings.warn("\n\n There are NaNs in the column(s) {}".
                  format(df[str_cols].columns[df[str_cols].isnull().any()]) +
                  "\n These NaNs will be converted to the string 'nan' \n")

for col in int_cols:
    df[col] = pd.to_numeric(df[col], errors='raise', downcast='integer')
for col in str_cols:
    df[col] = df[col].astype(str)





df.loc[df['age'] > 95, 'age'] = 95  

df = stage_hosp_prep.age_binning(df, allow_neonatal_bins=True)



    


diagnosis_feats = df.columns[df.columns.str.startswith('dx_')]

for feat in diagnosis_feats:
    df[feat] = hosp_prep.sanitize_diagnoses(df[feat])
    df[feat] = df[feat].str.upper()


if len(diagnosis_feats) > 1:
    
    stack_idx = [n for n in df.columns if "dx_" not in n]
    
    len_idx = len(stack_idx)

    df = df.set_index(stack_idx).stack().reset_index()

    
    pre_dx1 = df[df["level_{}".format(len_idx)] == "dx_1"].shape[0]
    df = df[df[0] != "none"]
    diff = pre_dx1 - df[df["level_{}".format(len_idx)] == "dx_1"].shape[0]
    print("{} dx1 cases/rows were lost after dropping blanks".format(diff))

    df = df.rename(columns={"level_{}".format(len_idx): 'diagnosis_id', 0: 'cause_code'})

    df.loc[df['diagnosis_id'] != "dx_1", 'diagnosis_id'] = 2
    df.loc[df.diagnosis_id == "dx_1", 'diagnosis_id'] = 1

elif len(diagnosis_feats) == 1:
    df.rename(columns={'dx_1': 'cause_code'}, inplace=True)
    df['diagnosis_id'] = 1

else:
    print("Something went wrong, there are no ICD code features")


df['val'] = 1

assert start_cases == df[df.diagnosis_id == 1].val.sum(), "some cases were added or lost"





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

assert not [s for s in df_agg.sex_id.unique() if s not in [1, 2, 3]],\
            "There should only be 3 unique sex id values (1, 2, 3)"

assert len(df_agg['code_system_id'].unique()) <= 2,\
    "code_system_id should have 2 or less feature levels"
assert len(df_agg['source'].unique()) == 1,\
    "source should only have one feature level"

assert (df.val >= 0).all(), ("for some reason there are negative case counts")

assert start_cases == df_agg[df_agg.diagnosis_id == 1].val.sum(), "some cases were added or lost"




compare_df = pd.read_hdf(root + "FILENAME"
                         "FILENAME"
                         "FILEPATH")
test_results = stage_hosp_prep.test_case_counts(df_agg, compare_df)
if test_results == "test_case_counts has passed!!":
    pass
else:
    msg = " --- ".join(test_results)
    assert False, msg





write_path = r"FILEPATH"\
            "FILEPATH".format(root)

hosp_prep.write_hosp_file(df_agg, write_path, backup=True)
