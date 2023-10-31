
# -*- coding: utf-8 -*-
"""

@author:  USERNAME

Formatting Australia  2017-2020

#####
NOTES: 
This data was downloaded from FILEPATH
This script formats the data to the extraction sheet.

"""

import pandas as pd
import numpy as np
import platform
import itertools
import re
import glob
import datetime
import warnings
import getpass
import sys
import os
import time
import subprocess

 

root = "filepath"



#####################################################
# READ DATA AND KEEP RELEVANT COLUMNS
#####################################################

#clean cases data structure, 
df = pd.read_excel("/filepath/AUS_NNDSS_ALL_DISEASES_BY_YEAR_1991_2021_CASES_Y2021M03D18.XLSX")
df.columns = [''] * len(df.columns)
df= df.iloc[1:]
header_row = 0 # if a different df make sure this is accurate!
df.columns = df.iloc[header_row]
df = df.iloc[1:]
df.columns = df.columns.fillna('causes')

#melt rows in a column
a = df.columns[1:]
df = pd.melt(df, id_vars=['causes'], value_vars= a,var_name='year', value_name='cases').sort_values('causes')
#subset 2017-2021
df = df.loc[df['year'].isin([2017.0, 2018.0, 2019.0, 2020.0 , 2021.0])]
#subset specific causes
df = df.loc[df['causes'].isin(['Campylobacteriosis','Cryptosporidiosis','Salmonellosis','Shigellosis','Meningococcal disease (invasive)','Cholera','Diphtheria','Haemophilus influenzae type b','Influenza (laboratory confirmed)','Measles','Pertussis','Pneumococcal disease (invasive)','Rotavirus','Tetanus', 'Varicella zoster (chickenpox)','Varicella zoster (shingles)','Varicella zoster (unspecified)'])]
df = df.reset_index(drop=True)

###########################################
#CLEAN COLUMN NAMES AND ADD COLUMNS
###########################################

#write a dictionary for parent_cause
pcause_dict = {
'Campylobacteriosis': 'diarrhea',
'Cryptosporidiosis': 'diarrhea',
'Salmonellosis': 'diarrhea',
'Shigellosis': 'diarrhea',
'Rotavirus': 'diarrhea',
'Cholera': 'diarrhea',
'Meningococcal disease (invasive)': 'meningitis',
'Pneumococcal disease (invasive)': 'meningitis',
'Haemophilus influenzae type b': 'meningitis',
'Influenza (laboratory confirmed)': 'lri', 
'Varicella zoster (chickenpox)':'varicella',
'Varicella zoster (shingles)': 'varicella',
'Varicella zoster (unspecified)': 'varicella',
'Diphtheria':'diphtheria', 
'Measles': 'measles',
'Pertussis': 'pertussis',
'Tetanus': 'tetanus'


}

df['parent_cause'] = df['causes'].map(pcause_dict)

##create dictionary for cause name and change names to lower case
cause_name = {
'Varicella zoster (chickenpox)':'chickenpox',
'Varicella zoster (shingles)': 'shingles',
'Varicella zoster (unspecified)': 'unspecified',
'Meningococcal disease (invasive)': 'IMD',
'Pneumococcal disease (invasive)': 'IPD',
'Haemophilus influenzae type b': 'hib',
'Campylobacteriosis': 'campylobacteriosis',
'Cryptosporidiosis': 'cryptosporidiosis',
'Salmonellosis': 'salmonellosis',
'Shigellosis': 'shigellosis',
'Rotavirus': 'rotavirus',
'Cholera': 'cholera',
'Influenza (laboratory confirmed)': 'influenza (laboratory confirmed)'


}

df['cause_name'] = df['causes'].map(cause_name)

######################################################################
#LAST STEPS
######################################################################

## Should have 85 rows here with subset of causes
df['nid'] = 213439
df['surveillance_name'] = "NNDSS"
df['age_start'] = 0
df['age_end'] = 99
df['sex'] = "both"
df['end_date'] = '1/01/' + df['year'].astype(str)
df['start_date'] = '12/31/' + df['year'].astype(str)
df['source_type'] = "surveillance"
df['measure_type'] = "incidence"
df['sample_size'] = 'NA'
df['case_status'] = 'unspecified'
df['group'] = 'NA'
df['specifity'] ='NA'
df['group_review'] = 'NA'
df['link'] = 'filepath/NATIONAL_NOTIFIABLE_DISEASE_SURVEILLANCE_SYSTEM_NNDSS'
df['ihme_loc_id'] = 'AUS'
df['location_name'] = "Australia"


##since influenza states it's lab confirmed, case status changes
df.loc[df['cause_name'] == 'influenza (laboratory confirmed)', 'case_status'] = "confirmed"

#re-organize columns
df = df[['nid','surveillance_name', 'source_type', 'link', 'location_name', 'ihme_loc_id', 'age_start', 'age_end', 'sex', 'start_date', 'end_date', 'parent_cause','cause_name', 'cases', 'sample_size', 'measure_type','case_status', 'group','specifity', 'group_review']]

df.to_csv("/filepath/aus_nndss_cases_2017_2021.csv")


 