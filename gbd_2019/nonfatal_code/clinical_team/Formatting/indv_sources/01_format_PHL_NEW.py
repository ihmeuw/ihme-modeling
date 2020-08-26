
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
import getpass
import warnings



user = getpass.getuser()

prep_path = r"FILENAME"
sys.path.append(prep_path)

from hosp_prep import *


if platform.system() == "Linux":
    root = "FILENAME"
else:
    root = "FILEPATH"





filepath = root + "FILEPATH"
files = glob.glob(filepath + "FILEPATH")

list_df = []
for file in files:
    df = pd.read_csv(file)
    list_df.append(df)


df = pd.concat(list_df)
df_og = df.copy()


df = df.reset_index(drop=True)
assert df.shape[0] == len(df.index.unique()), ("index is not unique, " +
    "the index has a length of " + str(len(df.index.unique())) +
    " while the DataFrame has " + str(df.shape[0]) + " rows " +
    "try this: df = df.reset_index(drop=True)")


drop_cols = ['Unnamed: 0', 'household.id', 'province']
df = df.drop(drop_cols, 1)
keep = df.columns.tolist()
df = df[keep]


df['year'] = df.date_adm.apply(lambda x: x.split('-')[0])



hosp_wide_feat = {
    'nid': 'nid',
    'location_id': 'location_id',
    'representative_id': 'representative_id',
    'year': 'year',
    'year_start': 'year_start',
    'year_end': 'year_end',
    'sex': 'sex_id',
    
    'age_value': 'age',
    'age_start': 'age_start',
    'age_end': 'age_end',
    'age_unit': 'age_group_unit',
    'code_system_id': 'code_system_id',

    
    'outcome_id': 'outcome_id',
    'facility_id': 'facility_id',
    
    
    'icdcode1': 'dx_1',
    'icdcode2': 'dx_2',
    'icdcode3': 'dx_3',
    'icdcode4': 'dx_4',
    'icdcode5': 'dx_5',
    
    
    'rvscode1' : 'procx_1',
    'rvscode2' : 'procx_2',
    'rvscode3' : 'procx_3',
    'rvscode4' : 'procx_4',
    'rvscode5' : 'procx_5',

    
    'household.id' : 'household_id',
    'patient.id' : 'patient_id',
    'patient.visit.id' : 'patient_visit_id'
    }


df.rename(columns=hosp_wide_feat, inplace=True)



new_col_df = pd.DataFrame(columns=list(set(hosp_wide_feat.values()) -
                                       set(df.columns)))
df = df.join(new_col_df)
























df['representative_id'] = 5  



df['location_id'] = 16


df['age_group_unit'] = 1
df['source'] = 'PHL_PHI'


df['code_system_id'] = 2


df['outcome_id'] = "FILENAME"


df['metric_id'] = 1


nid_dictionary = {'example_year': 'example_nid'}
df = fill_nid(df, nid_dictionary)



























df = df[df.los >= 0]


df['sex_id'].replace(['M', 'F'], [1, 2], inplace = True)















df['age'] = df['age'].astype(int)
















































































































































