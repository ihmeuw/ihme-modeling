
# -*- coding: utf-8 -*-
"""

@author:  username

Formatting Japan IDWR 2017-2020

#####
NOTES: 
This data was downloaded from https://www.niid.go.jp/niid/en/survaillance-data-table-english.html
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



# Environment:
if platform.system() == "Linux":
    root = "/filepath"
else:
    root = "filepath"

#####################################################
# READ DATA AND KEEP RELEVANT COLUMNS
#####################################################
filepath = root + r"filepath"
nof_files = glob.glob(filepath + "/20*/*.csv")
filepath_2 = root + r"filepath"
sent_files = glob.glob(filepath_2 + "/20*/*.csv")
#split into groups based on how the source data is formatted

df_nf = []
for each_file in nof_files:
    df = pd.read_csv(each_file)
    df.columns = [''] * len(df.columns)
    df = df.iloc[2:]
    year_id = each_file.split('/')[7]
    week = each_file.split('/')[8]
    header_row = 0 
    df.columns = df.iloc[header_row]
    df = df.loc[:, df.columns.notnull()] # removing cumulative column, not needed with weekly extraction
    df['Week'] = week[5:7]
    df['Year'] = int(year_id) 
    df['start_date'] = pd.to_datetime(df.Week.astype(str)+
                           df.Year.astype(str).add('-1') ,format='%V%G-%u')
    df = df.iloc[2:]
    df.reset_index(inplace=True, drop=True)
    df_nf.append(df)
df = pd.concat(df_nf, ignore_index=True)
#Pertussis isn't in 2017 data, so selecting columns outside of loop
df = df[['Prefecture','\rDiphtheria','Measles','Invasive meningococcal infection', 'Tetanus',
    'Invasive streptococcal pneumoniae infection','Varicella (limited to hospiltalized case)', 'Western equine encephalitis',
    'Tick-borne encephalitis','Shigellosis','\rAmebiasis', "Invasive haemophilus influenzae infection",'Cryptosporidiosis',
    'Cholera', 'Avian influenza H5N1','Avian influenza H7N9','Tuberculosis','Pertussis','start_date']]
df['surveillance_name'] = 'IDWR-notified_cases'

df_sent = []
for f in sent_files:
    df2 = pd.read_csv(f)
    df2.columns = [''] * len(df2.columns)
    df2 = df2.iloc[2:]
    year_id = f.split('/')[7]
    week = f.split('/')[8]
    header_row = 0
    df2.columns = df2.iloc[header_row]
    df2 = df2.loc[:, df2.columns.notnull()] # removing columns with cumul. counts
    df2['Week'] = week[9:11]
    df2['Year'] = int(year_id)
    df2['start_date'] = pd.to_datetime(df2.Week.astype(str)+
    df2.Year.astype(str).add('-1') ,format='%V%G-%u')
    df2 = df2[['Prefecture','Bacterial meningitis','Aseptic meningitis','Respiratory syncytial virus infection',
    'Mycoplasma pneumonia','Chlamydial pneumonia(excluding psittacosis)','Influenza(excld. avian influenza and pandemic influenza)','start_date']]
    df2 = df2.iloc[2:]
    df2.reset_index(inplace=True, drop=True)
    df_sent.append(df2)
df2 = pd.concat(df_sent, ignore_index=True)
df2['surveillance_name'] = 'IDWR-sentinal_reporting'

##make copies for if you ever have to come back to this step
df_copy = df.copy()
df = df_copy
df2_copy = df2.copy()
df2 = df2_copy

##  combine both dfs
df = pd.concat([df, df2], ignore_index=True)
#reset index
df.reset_index(inplace=True, drop=True)


###########################################
#CLEAN COLUMN NAMES
###########################################
# Replace feature names on the left with those found in data where appropriate
# and set up arrangement for future columns
jpn_feat = {
'nid':'nid',
'surveillance_name': 'surveillance_name',
'source_type': 'source_type',
'link': 'link',
'Prefecture':'location_name',
'ihme_loc_id': 'ihme_loc_id',
'age_start': 'age_start',
'age_end': 'age_end',
'sex': 'sex',
'start_date':'start_date',
'end_date': 'end_date',
'parent_cause': 'parent_cause',
'cause_name': 'cause_name',
'cases': 'cases',
'sample_size': 'sample_size',
'measure_type': 'measure_type',
'case_status': 'case_status',
'group': 'group',
'specificity':'specificity',
'group_review': 'group_review',
#renaming a few here
  '\rDiphtheria': 'diphtheria', # need to checkout
  'Measles':'measles',
  'Invasive meningococcal infection':'IMD',
  'Tetanus': 'tetanus',
  'Invasive streptococcal pneumoniae infection': 'IPD',
  'Varicella (limited to hospiltalized case)':'varicella',
  'Western equine encephalitis':'encephalitis_wq',
  'Tick-borne encephalitis' : 'encephalitis_tb',  
  'Shigellosis' : 'shigellosis', # cause of diarrhea
  '\rAmebiasis': 'amoebiasis', #diarrhea 
  "Invasive haemophilus influenzae infection": "invasive Hi",
  'Cryptosporidiosis' : 'cryptosporidiosis', #cause of diar.
  'Cholera' : 'cholera', #cause of dia
  'Avian influenza H5N1': 'avian influenza H5N1', #lri
  'Avian influenza H7N9' : 'avian influenza H7N9', #lri
  'Tuberculosis': 'tuberculosis',
  'Pertussis' : 'pertussis',
  'Bacterial meningitis': 'bacterial',
  'Aseptic meningitis': 'aseptic',
  'Respiratory syncytial virus infection': 'respiratory syncytial virus infection',
  'Mycoplasma pneumonia': 'mycoplasma pneumonia',
  'Chlamydial pneumonia(excluding psittacosis)': 'chlamydial pneumonia',
  'Influenza(excld. avian influenza and pandemic influenza)': 'influenza'

  }
#Rename features using dictionary created above
df.rename(columns=jpn_feat, inplace=True)

#set difference of the columns you have and the columns you want,
#yielding the columns you don't have yet
new_col_df = pd.DataFrame(columns=list(set(jpn_feat.values()) -
                                       set(df.columns)))
df = df.join(new_col_df)

###################################################
# ADD LOCATION DATA
##################################################

#There is probably a GBD function for this, but here is a nice dictionary for now. 
# Create ihme_loc id:
locations = {
'Aichi': 'JPN_35446',
'Akita': 'JPN_35428',
'Aomori': 'JPN_35425',
'Chiba': 'JPN_35435',
'Ehime': 'JPN_35461',
'Fukui': 'JPN_35441',
'Fukuoka' :	'JPN_35463',
'Fukushima' : 'JPN_35430',
'Gifu':	'JPN_35444',
'Gunma' : 'JPN_35433',
'Hiroshima' : 'JPN_35457',
'Hokkaido': 'JPN_35424',
'Hyogo': 'JPN_35451',
'Ibaraki' :	'JPN_35431',
'Ishikawa' : 'JPN_35440',
'Iwate' :	'JPN_35426',
'Kagawa' : 'JPN_35460',
"Kagoshima": 'JPN_35469',
"Kanagawa" : 'JPN_35437',
'Kochi': 'JPN_35462',
'Kumamoto':	'JPN_35466',
"Kyoto" : 'JPN_35449',
'Mie' :	"JPN_35447",
'Miyagi' : 'JPN_35427',
'Miyazaki' : 'JPN_35468',
'Nagano' : 'JPN_35443',
'Nagasaki' : 'JPN_35465',
'Nara' : 'JPN_35452',
'Niigata' : 'JPN_35438',
'Oita': 'JPN_35467',
'Okayama' :	'JPN_35456',
'Okinawa' :	'JPN_35470',
'Osaka' : 'JPN_35450',
'Saga' : 'JPN_35464',
'Saitama' : 'JPN_35434',
'Shiga' : 'JPN_35448' ,
'Shimane' :	'JPN_35455',
'Shizuoka' : 'JPN_35445',
'Tochigi' :	'JPN_35432',
'Tokushima' : 'JPN_35459',
'Tokyo' :	'JPN_35436',
'Tottori' :	'JPN_35454',
"Toyama":	'JPN_35439',
"Wakayama" : 'JPN_35453',
'Yamagata':	'JPN_35429',
'Yamaguchi': 'JPN_35458',
'Yamanashi' : 'JPN_35442'
}

df['ihme_loc_id'] = df.location_name.map(locations)

##################################################################
# TRANSFORM FROM WIDE TO LONG
##################################################################

# Saving the wide file before changing to long 
write_path = "/filepath/jpn_niid.csv"
df.to_csv(write_path)

# Reshape each parent cause diagnoses from wide to long and extract for google sheet 
other = df[['location_name','start_date', 'surveillance_name', 'ihme_loc_id', 'diphtheria','measles','tetanus', 'varicella', 'pertussis']]
other = pd.melt(frame=other, 
        id_vars=['location_name', 'start_date','surveillance_name','ihme_loc_id'], 
        var_name="parent_cause", 
        value_name="cases")

dia = df[[ 'location_name', 'start_date','surveillance_name', 'ihme_loc_id','shigellosis','amoebiasis','cryptosporidiosis','cholera']]
dia = pd.melt(frame=dia, 
        id_vars=['location_name','start_date','surveillance_name', 'ihme_loc_id'], 
        var_name="cause_name", 
        value_name="cases")
dia['parent_cause'] = "diarrhea"

meni = df[[ 'location_name', 'start_date','surveillance_name','ihme_loc_id','invasive Hi', 'IPD', 'IMD', 'bacterial', 'aseptic']]
meni = pd.melt(frame=meni, 
        id_vars=['location_name','start_date','surveillance_name','ihme_loc_id'], 
        var_name="cause_name", 
        value_name="cases")
meni['parent_cause'] = "meningitis"

lri = df [[ 'location_name', 'start_date', 'surveillance_name', "ihme_loc_id", 'avian influenza H5N1','avian influenza H7N9' ,'respiratory syncytial virus infection', 'influenza', 'mycoplasma pneumonia', 'chlamydial pneumonia']]
lri = pd.melt(frame=lri, 
        id_vars=["location_name","ihme_loc_id", "start_date",'surveillance_name'],
        var_name="cause_name", 
        value_name="cases")
lri['parent_cause'] = "lri"

df = pd.concat([other, dia, meni, lri], ignore_index=True)
####################################################################
#CLEANING
####################################################################
#Post-Check, cleaning up validated blanks(happens because causes match to both sources,
#  so when an NC cause matches to SC source name it causes a blank, and vice versa) and removing "-" 
# from 2020 data where there is nothing reporting yet.

df['cases'] = df['cases'].replace('',np.nan)
#this shows up for late 2020 data where they don't have results yet
df['cases'] = df['cases'].replace('-',np.nan)
# clean dataset
df.dropna(subset=['cases'], inplace = True)

######################################################################
#LAST STEPS
######################################################################
#add necessary column names
df['nid'] = 'NA'  
df['age_start'] = 0
df['age_end'] = 99
df['sex'] = "both"
df['end_date'] = df['start_date'] + datetime.timedelta(days=6)
df['source_type'] = "surveillance"
df['measure_type'] = "incidence"
df['sample_size'] = 'NA'
df['case_status'] = 'unspecified'
df['group'] = 'NA'
df['specifity'] ='NA'
df['group_review'] = 'NA'
df['link'] = 'NA'

#re-organize columns
df = df[['nid','surveillance_name', 'source_type', 'link', 'location_name', 'ihme_loc_id', 'age_start', 'age_end', 'sex', 'start_date', 'end_date', 'parent_cause','cause_name', 'cases', 'sample_size', 'measure_type','case_status', 'group','specifity', 'group_review']]

#write csv and check with looking at raw data
df.to_csv("/filepath/IDWR_jpn_data_2017_2021.csv")


