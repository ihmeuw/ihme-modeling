
# coding: utf-8

"""
Created on Mon Jan 23 17:32:28 2017

Format PHL health claims data for years 2013/2014 for use in the correction factors

This is the data we used in GBD2016 and 2017, after 2017 we found an issue with the
patient IDs and have added a fix recc'd by collaborators.

There is newer PHL data for 2016 which is processed with a different script because
the raw data structure is so much different
"""
import pandas as pd
import sys
import getpass

user = getpass.getuser()

# load our functions
for hosp_path in FILEPATHS
    sys.path.append(hosp_path)

import estimate_indv

run_id = sys.argv[1]
run_id = run_id.replace("\r", "")


def fix_phl_ids(df):
    """
    PHL uses household IDs in this data, rather than patient IDs, we didn't know this until
    the end, or after gbd2017. Basically to get from household ID to patient ID you can add age
    and sex to the household ID, and if there's a one year increase in age you assume that's the
    same person. This is what PHL did when they sent us 2016 data. 
    """
    df.rename(columns={'patient_id': 'household_id'}, inplace=True)

    # the real patient id is equal to the household id, a little salt so nothing overlaps and age/sex
    df['patient_id'] = df['household_id'] + "_" + df['age'].astype(str) + "_" + df['sex_id'].astype(str)

    overlap = set(df.household_id.unique()).intersection(set(df.patient_id.unique()))

    assert not overlap, "Something went wrong, household ids and patient ids can't overlap {}".format(overlap)

    return df

# pull in the prepped PHL data
full_df = pd.read_hdf(FILEPATH)
# use merged nid
full_df['nid'] = 292574

# fix the household ID issue
full_df = fix_phl_ids(full_df)

for year in full_df.year_start.unique():
    print(year)
    df = full_df[full_df.year_start == year].copy()
    # drop outpatient data
    df = df[df.facility_id == 'inpatient unknown']

    df.drop(['val', 'facility_id'], axis=1, inplace=True)

    # INP PRIMARY ADMISSIONS -- INPATIENT PRIMARY INDIVIDUALS
    # INP ANY ADMISSIONS -- INP ANY INDIVIDUALS
    back = df.copy()
    for cause_type in ['bundle', 'icg']:
        df = estimate_indv.main(back.copy(), cause_type=cause_type)

        #####################################################
        # WRITE TO FILE
        #####################################################

        # write file
        write_path = FILEPATH.format(r=run_id, y=year, c=cause_type)

        df.to_csv(write_path, index=False)
