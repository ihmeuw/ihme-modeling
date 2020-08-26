


"""
Created on Mon Jan 23 17:32:28 2017
@author: USERNAME and USER

Format PHL health claims data as if it were hospital data
"""
import pandas as pd
import platform
import numpy as np
import sys
import re
import time
import multiprocessing
import warnings



USER_path = r"FILENAME"

sys.path.append(USER_path)

import hosp_prep


if platform.system() == "Linux":
    root = "FILENAME"
else:
    root = "FILEPATH"

start = time.time()






df = pd.read_hdf("FILENAME"
                 "FILENAME"
                 "FILEPATH", key='df')
print("Data read in")



assert df.shape[0] == len(df.index.unique()), ("index is not unique, " +
   "the index has a length of " + str(len(df.index.unique())) + 
   " while the DataFrame has " + str(df.shape[0]) + " rows" +
   "try this: df = df.reset_index(drop=True)")


df.rename(columns={'NEWPIN': 'enrollee_id', 'ICDCODES': 'cause_code',
                  'PATAGE': 'age', 'PATSEX': 'sex_id', 'OPD_TST': 'is_otp',
                  'DATE_ADM': 'adm_date', 'DATE_DIS': 'dis_date',
                  'CLASS_DEF': 'facility_id',
                  'DATE_OF_DEATH': 'date_of_death'}, inplace=True)









df['location_id'] = 16


df['age_group_unit'] = 1  
df['source'] = 'PHIL_HEALTH_CLAIMS'

df['year_start'] = 0
df.loc[df.adm_date < "2014-01-01", 'year_start'] = 2013
df.loc[df.adm_date >= "2014-01-01", 'year_start'] = 2014
df['year_end'] = df['year_start']


df['code_system_id'] = 2  


df['outcome_id'] = df['date_of_death']
df.loc[df.outcome_id.notnull(), 'outcome_id'] = 'death'
df.loc[df.outcome_id.isnull(), 'outcome_id'] = 'discharge'


df.loc[(df.sex_id != "M") & (df.sex_id != "F"), 'sex_id'] = 3
df['sex_id'].replace(['F', 'M'], [2, 1], inplace=True)

print("Done filling columns")





df = df[(df.age >= 0) & (df.age < 150)]
total_obs = df.shape[0]  


int_cols = ['location_id', 'year_start', 'year_end', 'age_group_unit',
           'age', 'sex_id']




str_cols = ['source', 'facility_id', 'outcome_id']

for col in int_cols:
   df[col] = pd.to_numeric(df[col], errors='raise', downcast='integer')
for col in str_cols:
   df[col] = df[col].astype(str)

print("Done cleaning cols")



df['og_cause_code'] = df['cause_code']  


df['cause_code'] = df['cause_code'].str.replace("\W", "")

df['cause_code'] = df['cause_code'].str.upper()


df['cc_list'] = df.cause_code.map(lambda x: re.split("([A-Z]\d+)", x))



if sys.version[0] == '3':
   df.cc_list = df.cc_list.map(lambda x: list([_f for _f in x if _f]))  
if sys.version[0] == '2':
   df.cc_list = df.cc_list.map(lambda x: [_f for _f in x if _f])  





def create_dxdf(df):
   dat = df['cc_list'].apply(pd.Series)
   return dat

n_cores = 3
warnings.warn("This script uses multiprocessing with {} pools. If ran on fair cluster then it needs to be ran with at least {} threads.".format(n_cores))

print("Start multiprocessing", (time.time()-start)/60)


p = multiprocessing.Pool(n_cores)
l = list(p.map(create_dxdf, np.array_split(df, 45)))
dx_df = pd.concat(l)
print("Done multiprocessing", (time.time()-start)/60)


nums = np.arange(1, dx_df.shape[1] + 1, 1)
names = ["dx_" + str(num) for num in nums]
dx_df.columns = names


df.drop(['cause_code', 'cc_list', 'og_cause_code'], axis=1, inplace=True)

assert (df.index == dx_df.index).all()
df = pd.concat([df, dx_df], axis=1)


df.dx_1.fillna("CC_CODE", inplace=True)
df.loc[(df.dx_1 == "NAN") |
      (df.dx_1 == "") |
      (df.dx_1 == "NONE"), 'dx_1'] = "CC_CODE"


diagnosis_feats = df.columns[df.columns.str.startswith('dx_')]

for feat in diagnosis_feats:
   df[feat] = hosp_prep.sanitize_diagnoses(df[feat])
   df[feat] = df[feat].astype(str)

str_feats = ['enrollee_id', 'is_otp', 'facility_id', 'date_of_death',
            'source', 'outcome_id']
for feat in str_feats:
   df[feat] = df[feat].astype(str)
print("Done cleaning columns", (time.time()-start)/60)



df['los'] = df['dis_date'] - df['adm_date']



tmp = df[df.los == "0 days"].copy()
tmp = tmp[tmp.outcome_id != "death"]
non_death_day_cases = tmp.shape[0]
del tmp



pre = df.shape[0]
df = df[(df['los'] != "0 days") | (df.outcome_id != 'discharge')]

total_obs = df.shape[0]

assert pre - df.shape[0] == non_death_day_cases,\
   "The wrong number of cases were dropped"
assert df[df.los == "0 days"].outcome_id.unique()[0] == 'death',\
   "There are day cases that didn't result in death present"
assert df[df.los == "0 days"].outcome_id.unique().size == 1,\
   "Too many unique outcome id values for day cases"

print("Done removing day cases", (time.time()-start)/60)




print("Beginning formatting", (time.time()-start)/60)

df_list = np.array_split(df, 100)
del df

final_list = []

for df in df_list:
    
    
    assert df.shape[0] == len(df.index.unique()), ("index is not unique, " +
        "the index has a length of " + str(len(df.index.unique())) +
        " while the DataFrame has " + str(df.shape[0]) + " rows" +
        "try this: df = df.reset_index(drop=True)")

    df.drop(['facility_id'], axis=1, inplace=True)

    
    
    
    hosp_wide_feat = {
        'nid': 'nid',
        'location_id': 'location_id',
        'representative_id': 'representative_id',
        
        'year_start': 'year_start',
        'year_end': 'year_end',
        'sex_id': 'sex_id',
        'age': 'age',
        'age_start': 'age_start',
        'age_end': 'age_end',
        'age_group_unit': 'age_group_unit',
        'code_system_id': 'code_system_id',

        
        'outcome_id': 'outcome_id',
        'is_otp': 'facility_id'}

    
    df.rename(columns=hosp_wide_feat, inplace=True)

    
    
    new_col_df = pd.DataFrame(columns=list(set(hosp_wide_feat.values()) -
                                           set(df.columns)))
    df = df.join(new_col_df)

    print("Done renaming columns", (time.time()-start)/60)
    
    
    
    
    

    
    df['representative_id'] = 1
    

    
    df['age_group_unit'] = 1
    df['source'] = 'PHL_HICC'

    
    

    
    df['metric_id'] = 1

    df.loc[df.facility_id == 'T', 'facility_id'] = 'outpatient unknown'
    df.loc[df.facility_id == 'F', 'facility_id'] = 'inpatient unknown'

    
    nid_dictionary = {2013: 222560, 2014: 222563}
    df = hosp_prep.fill_nid(df, nid_dictionary)

    
    
    
    print("Start swapping", (time.time()-start)/60)
    
    df = hosp_prep.swap_ecode(df, 10)

    print("Ecodes swapped", (time.time()-start)/60)

    
    
    

    
    int_cols = ['location_id', 'year_start', 'year_end', 'age_group_unit', 'age',
                'age_start', 'age_end', 'sex_id', 'nid', 'representative_id',
                'metric_id']
    
    
    
    
    str_cols = ['source', 'facility_id', 'outcome_id']

    for col in int_cols:
        df[col] = pd.to_numeric(df[col], errors='raise', downcast='integer')
    for col in str_cols:
        df[col] = df[col].astype(str)

    
    
    
    
    df = df[df['age'] < 112]

    total_obs = df.shape[0]

    
    df.loc[df['age'] > 95, 'age'] = 95  
    
    df = hosp_prep.age_binning(df)

    
    df.loc[(df.sex_id != 1) & (df.sex_id != 2), 'sex_id'] = 3

    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    diagnosis_feats = df.columns[df.columns.str.startswith('dx_')]
    
    
    
    
    wide = df.copy()
    
    wide = wide[wide.outcome_id != "death"]
    
    wide.drop(['adm_date', 'dis_date', 'date_of_death', 'los'], axis=1, inplace=True)
    wide.to_stata(r"FILEPATH".format(split_int))
    del wide
    
    
    
        
    
    df = df[['location_id', 'year_start', 'year_end', 'age_start', 'age_end', 'sex_id',
              'outcome_id', 'nid', 'facility_id', 'metric_id', 'code_system_id',
              'representative_id', 'source', 'age_group_unit'] + list(diagnosis_feats)]
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    def reshape_long_mp(each_df):
        each_df = each_df.set_index(['location_id', 'year_start', 'year_end', 'age_start', 'age_end', 'sex_id',
                                 'outcome_id', 'nid', 'facility_id', 'metric_id', 'code_system_id',
                                 'representative_id', 'source', 'age_group_unit']).stack().reset_index()
    
        each_df[0] = each_df[0].str.upper()
        
        each_df = each_df[each_df[0] != "NONE"]
        each_df = each_df[each_df[0] != "NAN"]
        each_df = each_df[each_df[0] != ""]
        
        each_df = each_df.rename(columns={'level_14': 'diagnosis_id', 0: 'cause_code'})
        
        
        each_df['diagnosis_id'] = np.where(each_df['diagnosis_id'] ==
                                              'dx_1', 1, 2)
        print("One subset df is done")
        return each_df
    
    df.dx_1 = df.dx_1.str.upper()
    df.loc[(df.dx_1 == "NONE") | (df.dx_1 == ""), 'dx_1'] = 'CC_CODE'
    dx_1_val_counts = df.dx_1.value_counts()
    
    print("Start reshaping long", (time.time()-start)/60)

    if platform.system == "Linux":
        p = multiprocessing.Pool(3)
        l = list(p.map(reshape_long_mp, np.array_split(df, 15)))
        df = pd.concat(l)
    else:
        df = reshape_long_mp(df)

    assert (dx_1_val_counts ==
            df[df.diagnosis_id == 1].cause_code.value_counts()).all(),\
            "Reshaping long changed the value count of primary ICD codes"
    
    print("Done reshaping long", (time.time()-start)/60)
    
    df['val'] = 1
    
    
    row_diff = total_obs - df[df.diagnosis_id == 1].val.sum()
    assert row_diff == 0,\
        "{} rows seem to have been lost".format(row_diff)
    
    
    
    
    
    
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
    df = df.groupby(group_vars).agg({'val': 'sum'}).reset_index()
    
    
    
    
    
    
    columns_before = df.columns
    hosp_frmat_feat = ['age_group_unit', 'age_start', 'age_end',
                       'year_start', 'year_end',
                       'location_id',
                       'representative_id',
                       'sex_id',
                       'diagnosis_id', 'metric_id', 'outcome_id', 'val',
                       'source', 'nid',
                       'facility_id',
                       'code_system_id', 'cause_code']
    df = df[hosp_frmat_feat]
    columns_after = df.columns
    
    
    assert set(columns_before) == set(columns_after),\
        "You lost or added a column when reordering"
    for i in range(len(hosp_frmat_feat)):
        assert hosp_frmat_feat[i] in df.columns,\
            "%s is missing from the columns of the DataFrame" % (hosp_frmat_feat[i])
    
    
    for i in df.drop(['cause_code', 'source', 'facility_id', 'outcome_id'],
                     axis=1, inplace=False).columns:
        
        
        assert df[i].dtype != object, "%s should not be of type object" % (i)
    
    
    assert len(df['year_start'].unique()) == len(df['nid'].unique()),\
               "number of feature levels of years and nid should match number"
    assert len(df['age_start'].unique()) == len(df['age_end'].unique()),\
               "number of feature levels age start should match number of " +\
               r"feature levels age end"
    assert len(df['diagnosis_id'].unique()) <= 2,\
               "diagnosis_id should have 2 or less feature levels"
    assert len(df['sex_id'].unique()) <= 3,\
               "There should only be three feature levels to sex_id"
    assert len(df['code_system_id'].unique()) <= 2,\
               "code_system_id should have 2 or less feature levels"
    assert len(df['source'].unique()) == 1,\
               "source should only have one feature level"
    
    assert total_obs == df[df.diagnosis_id == 1].val.sum(),\
        "{} cases were lost".format(total_obs - df[df.diagnosis_id == 1].val.sum())
    final_list.append(df)

df = pd.concat(final_list)





write_path = root + r"FILENAME" +\
    r"FILEPATH"
hosp_prep.write_hosp_file(df, write_path, backup=True)
