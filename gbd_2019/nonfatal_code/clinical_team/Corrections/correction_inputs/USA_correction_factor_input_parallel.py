# coding: utf-8
"""
Prepare the USA HCUP data for the correction factors
"""
import pandas as pd
import numpy as np
import sys
import itertools
import time
import glob
import sys
import getpass

user = getpass.getuser()
# load our functions
for hosp_path in [FILEPATHS]:
    sys.path.append(hosp_path)

import hosp_prep
import estimate_indv

run_id = sys.argv[1]
run_id = run_id.replace("\r", "")
fpath = sys.argv[2]
fpath = fpath.replace("\r", "")

if sys.argv[2] == "test":
    fpath = FILEPATH

# read in data
df = pd.read_stata(fpath)

# keep only inpatient data
print(df.platform.value_counts(dropna=False))
df = df[df.platform == 1]
assert df.platform.unique() == 1, "There is outpatient data present somehow"

# Formatting stuff that's special to NZL, basically the data is in a legacy
# GBD2015 format and we need to align it with the new system
df = df[['year', 'age', 'sex', 'location_id', 'patient_id', 'icd_vers',
         'cause_code', 'diagnosis_id', 'amonth', 'ayear']]
# rename cols to fit our schema
df.rename(columns={'year': 'year_start', 'sex': 'sex_id',
                   'icd_vers': 'code_system_id'}, inplace=True)
# convert icd names to our coding system
df['code_system_id'].replace(['ICD9_detail', 'ICD10'], [1, 2], inplace=True)
df['year_end'] = df['year_start']

# create adm_date feature using amonth and ayear
df['aday'] = 1
# fill missing years with data year
df.loc[df['ayear'].isnull(), 'ayear'] = df.loc[df['ayear'].isnull(), 'year_start']
# fill missing months with january
df.loc[df['amonth'].isnull(), 'amonth'] = 1

# convert to type int for pd.to_datetime func to work properly
df['amonth'] = df['amonth'].astype(int)
df['ayear'] = df['ayear'].astype(int)

df['adm_date_str'] = df['ayear'].astype(str) + "/" + df['amonth'].astype(str) + "/" + df['aday'].astype(str)
df['adm_date'] = pd.to_datetime(df['adm_date_str'])# , errors='coerce')  # this seems to be quite slow

# drop the cols we used to make adm_date
df.drop(['adm_date_str', 'amonth', 'ayear', 'aday'], axis=1, inplace=True)

back = df.copy()
for cause_type in ['bundle', 'icg']:
    df = estimate_indv.main(back.copy(), cause_type=cause_type)

    # get location_id and year from the filepath
    year = fpath[-10:-8]
    loc = fpath[-7:-4]

    # write files by location and year
    write_path = FILEPATH.format(r=run_id, y=year, l=loc, c=cause_type)

    df.to_csv(write_path, index=False)
