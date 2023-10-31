"""

@author:  name

Formatting  NZL ESR subnationals, 2017-2021

#####
NOTES: 
This data was downloaded from https://surv.esr.cri.nz/surveillance/monthly_surveillance.php
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

from pandas.tseries.offsets import MonthEnd

##################################################
# PREPARING 
#################################################
#fixing june structure first since it was a little different, than the rest
junepath = "/filepath/201806JunNat.xls"
df_j = pd.read_excel(junepath)
df_j = df_j.drop(df_j.columns[[0]], axis=1)
df_j.to_excel("/filepath/201806JunNat.xls", index=False) 

#####################################################
# READ DATA AND KEEP RELEVANT COLUMNS
#####################################################
filepath = "filepath"
junepath = "/filepath/201806JunNat.xls"
#need to process 06 2018 separately because the columns are different a bit. 
all_files = glob.glob(filepath)
june = glob.glob(junepath)
all_files.append(junepath)


## read in file and clean structure before data manipulation
df_nz = []
for each_file in all_files:
    df = pd.read_excel(each_file)
    df.columns = [''] * len(df.columns)
    df = df.iloc[1:]
    year_id = each_file.split('/')[7]
    month = each_file.split('/')[8]
    header_row = 1 # if a different df make sure this is accurate!
    df.columns = df.iloc[header_row]
    df['year'] = int(year_id) 
    df['month'] = month[4:6]
    df = df.iloc[1:]
    df.rename(columns={ df.columns[0]: "Disease" }, inplace = True)
    df = df.loc[:, df.columns.notnull()]
    df = df.loc[df['Disease'].isin(['Campylobacteriosis','Cryptosporidiosis', 'Haemophilus influenzae type b','Salmonellosis','Shigellosis','Meningococcal disease','Invasive pneumococcal disease', 'VTEC/STEC infection', 'Measles','Pertussis'])]
    df.rename(columns={ df.columns[1]: "cases" }, inplace = True)
    df = df[["Disease", "cases", "year", "month"]]
    df = df.loc[:,~df.columns.duplicated()] # duplicated cases in January
    df.reset_index(inplace=True, drop=True)
    df_nz.append(df)
df = pd.concat(df_nz, ignore_index=True)


##########################################
# CHECK COUNTS
##########################################
#look at counts here for each year and see if anything is missing before moving on 
print(df.year.value_counts())


###########################################
#CLEAN COLUMN NAMES AND ADD COLUMNS
###########################################

#write a dictionary for parent_cause
pcause_dict = {
'Campylobacteriosis': 'diarrhea',
'Cryptosporidiosis': 'diarrhea',
'Salmonellosis': 'diarrhea',
'Shigellosis': 'diarrhea',
'VTEC/STEC infection': 'diarrhea',
'Meningococcal disease': 'meningitis',
'Invasive pneumococcal disease': 'meningitis',
'Haemophilus influenzae type b': 'meningitis',
'Diphtheria':'diphtheria', 
'Measles': 'measles',
'Pertussis': 'pertussis',

}

df['parent_cause'] = df['Disease'].map(pcause_dict)

#create dictionary for cause name and change names to lower case
cause_name = {
'Meningococcal disease': 'IMD',
'Invasive pneumococcal disease': 'IPD',
'Haemophilus influenzae type b': 'hib',
'Campylobacteriosis': 'campylobacteriosis',
'Cryptosporidiosis': 'cryptosporidiosis',
'Salmonellosis': 'salmonellosis',
'Shigellosis': 'shigellosis',


}

df['cause_name'] = df['Disease'].map(cause_name)

######################################################################
#LAST STEPS
######################################################################

####Fill in the rest of columns and clean any other messy data
df['nid'] = 'NA'
df['surveillance_name'] = "NZL_ESR"
df['age_start'] = 0
df['age_end'] = 99
df['sex'] = "both"
df['start_date'] = pd.to_datetime(df[['year', 'month']].assign(DAY=1))
df['end_date'] = pd.to_datetime(df['start_date'], format="%Y%m") + MonthEnd(1)
df['source_type'] = "surveillance"
df['measure_type'] = "incidence"
df['sample_size'] = 'NA'
#case_status needs to be classified from the information on the website, manually.
df['case_status'] = 'NA'
df['group'] = 'NA'
df['specifity'] ='NA'
df['group_review'] = 'NA'
df['link'] = '/filepath/MONTHLY_NOTIFIABLE_DISEASE_SURVEILLANCE_REPORT'
df['ihme_loc_id'] = 'NZL'
df['location_name'] = "New Zealand"

#re-organize columns
df = df[['nid','surveillance_name', 'source_type', 'link', 'location_name', 'ihme_loc_id', 'age_start', 'age_end', 'sex', 'start_date', 'end_date', 'parent_cause','cause_name', 'cases', 'sample_size', 'measure_type','case_status', 'group','specifity', 'group_review']]

## save the df
df.to_csv("/filepath/nzl_esr_cases_2017_2021.csv")