
"""
Created on Mon Jan 23 17:32:28 2017
@author: USERNAME and USER

Template for formatting raw hospital data.  Follow the instructions in the
comments and ensure that the code is relevant to your particular source.

Formatting Turkey data

Data is stored at FILEPATH
Cooper entry is at
URL


In a 5/23/13 email from NAME: for 2011, the coverage is
71% of inpatient cases in all hospitals in the country. For 2012, coverage is
67%. Data are gathered from all hospitals with at least 100 hospital beds
(approximately 8 million patients). Data entry is performed by trained coders.

The Turkish DRG system (TIG, or Tani Iliskili Gruplar in Turkish)
is administered by a branch of the MoH called the Department of Diagnosis
Related Groups (Teshis Iliskili Gruplar Daire Baskanligi, in Turkish -
URL
actually have the DRG codes for patients. The main reimbursement agency is
the Social Security Institute (SGK, or Sosyal Güvenlik Kurumu in Turkish -
URL


Ownership is marked as partial because this is an extract of the underlying
dataset, which is the DRG database of hospital discharges.

NOTE: there is no information about the OUTCOME of each diagnosis inside the
data itself, but on GHDx it's tagged with 'discharge' so we're using that.

Files are split up by sex. THERE IS NO SEX COLUMN IN THE DATA

"""

import pandas as pd
import platform
import numpy as np
import sys


USER_path = r"FILEPATH"
USER_path = r"FILEPATH"
sys.path.append(USER_path)
sys.path.append(USER_path)

from hosp_prep import *


if platform.system() == "Linux":
    root = "FILENAME"
else:
    root = "FILEPATH"







filepath = root + r"FILENAME"


df_male_2011 = pd.read_csv(filepath +
                           r"FILENAME"
                           r"FILEPATH", sep=";",
                           names=['dx_1', 'age', 'val'])
df_female_2011 = pd.read_csv(filepath +
                             r"FILENAME"
                             r"FILEPATH", sep=";",
                             names=['dx_1', 'age', 'val'])
df_male_2012 = pd.read_csv(filepath +
                           r"FILENAME"
                           r"BY_CAUSE_AGE_MALE_Y2013M04D23.CSV.", sep=";",
                           names=['dx_1', 'age', 'val'])
df_female_2012 = pd.read_csv(filepath +
                             r"FILENAME"
                             r"BY_CAUSE_AGE_MALE_Y2013M04D23.CSV.", sep=";",
                             names=['dx_1', 'age', 'val'])

df_male_2011['sex_id'] = 1
df_male_2011['year'] = 2011

df_female_2011['sex_id'] = 2
df_female_2011['year'] = 2011

df_male_2012['sex_id'] = 1
df_male_2012['year'] = 2012

df_female_2012['sex_id'] = 2
df_female_2012['year'] = 2012

df = pd.concat([df_male_2011, df_female_2011, df_male_2012, df_female_2012])
del df_male_2011
del df_female_2011
del df_male_2012
del df_female_2012







hosp_wide_feat = {
    'nid': 'nid',
    'location_id': 'location_id',
    'representative_id': 'representative_id',
    'year': 'year',
    'year_start': 'year_start',
    'year_end': 'year_end',
    'sex_id': 'sex_id',
    'age': 'age',
    'age_start': 'age_start',
    'age_end': 'age_end',
    'age_group_unit': 'age_group_unit',
    'facility_id': 'facility_id',
    'code_system_id': 'code_system_id',

    
    'outcome_id': 'outcome_id',
    'facility_id': 'facility_id',
    



}


df.rename(columns=hosp_wide_feat, inplace=True)




new_col_df = pd.DataFrame(columns=list(set(hosp_wide_feat.values()) -
                                       set(df.columns)))
df = df.join(new_col_df)
del new_col_df






df['representative_id'] = 3
df['location_id'] = 155


df['age_group_unit'] = 1
df['source'] = 'TUR_DRGHID'


df['code_system_id'] = 2


df['metric_id'] = 1


df['nid'] = np.nan
df.loc[df['year'] == 2011, 'nid'] = 130051
df.loc[df['year'] == 2012, 'nid'] = 130054

df['outcome_id'] = "discharge"

df['facility_id'] = "hospital"






int_cols = ['location_id', 'year_start', 'year_end', 'age_group_unit',
            'age_start', 'age_end', 'sex_id', 'nid', 'representative_id',
            'metric_id']
str_cols = ['source', 'facility_id', 'outcome_id']

for col in int_cols:
    df[col] = pd.to_numeric(df[col], errors='raise', downcast='integer')
for col in str_cols:
    df[col] = df[col].astype(str)




df.loc[df['age'] > 95, 'age'] = 95  

df = age_binning(df)
df.drop('age', axis=1, inplace=True)  








df['year_start'] = df['year']
df['year_end'] = df['year']
df.drop('year', axis=1, inplace=True)




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
assert len(df_agg['sex_id'].unique()) == 2,\
    "There should only be two feature levels to sex_id"
assert len(df_agg['code_system_id'].unique()) <= 2,\
    "code_system_id should have 2 or less feature levels"
assert len(df_agg['source'].unique()) == 1,\
    "source should only have one feature level"







write_path = root + r"FILENAME" +\
    r"FILEPATH"
write_hosp_file(df_agg, write_path, backup=True)
