
# coding: utf-8
"""
@author: USER

Format NZL NMDS hospital data using a modified version of the marketscan claims
process to create inputs for the correction factors

"""
import pandas as pd
import sys
import time

from clinical_info.Corrections.correction_inputs import estimate_indv

run_id = sys.argv[1]
run_id = run_id.replace("\r", "")
year = sys.argv[2]
year = int(year.replace("\r", ""))

# read in data
fpath = FILEPATH
back = pd.read_stata(fpath)

fstart = time.time()

df = back[back.year == year].copy()
pre = df.shape[0]

assert df.platform.unique() == 1, "There is outpatient data present somehow"
# keep only the columns we need
df = df[['year', 'age', 'sex', 'location_id', 'patient_id', 'icd_vers',
         'cause_code', 'diagnosis_id', 'adm_date']]
# rename cols to fit our schema
df.rename(columns={'year': 'year_start', 'sex': 'sex_id',
                   'icd_vers': 'code_system_id'}, inplace=True)
# convert icd names to our coding system
df['code_system_id'].replace(['ICD9_detail', 'ICD10'], [1, 2], inplace=True)
df['year_end'] = df['year_start']

# drop missing patient IDs
df = df[df.patient_id != "."]

print("cases", df.shape[0])
back = df.copy()

for cause_type in ['bundle', 'icg']:
    df = estimate_indv.main(back.copy(), cause_type=cause_type)

    # write files by location and year
    write_path = FILEPATH.format(
                     r=run_id, y=year, c=cause_type)
    df.to_csv(write_path, index=False)
