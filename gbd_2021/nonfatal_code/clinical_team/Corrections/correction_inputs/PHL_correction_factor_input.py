
# coding: utf-8

"""
@author: USER

Format PHL health claims data for years 2013/2014 for use in the correction factors

"""
import pandas as pd
import sys

# load our functions
from clinical_info.Corrections.correction_inputs import estimate_indv
from clinical_info.Functions.live_births import live_births

run_id = sys.argv[1]
run_id = run_id.replace("\r", "")


def fix_phl_ids(df):
    df.rename(columns={'patient_id': 'household_id'}, inplace=True)

    df['patient_id'] = df['household_id'] + "_" + \
        df['age'].astype(str) + "_" + df['sex_id'].astype(str)

    overlap = set(df.household_id.unique()).intersection(
        set(df.patient_id.unique()))

    assert not overlap, "Something went wrong, household ids and patient ids can't overlap {}".format(
        overlap)

    return df


# pull in the prepped PHL data
full_df = pd.read_hdf(
    FILEPATH, key='df')
# use merged nid
full_df['nid'] = 292574

# fix the household ID issue
full_df = fix_phl_ids(full_df)

# swap live birth codes
placeholders = ['nan', 'NAN', 'NaN', 'NONE', 'NULL', '']
for col in list(df.filter(regex="^(dx_)").columns.drop("dx_1")):
    df.loc[df[col].isin(placeholders), col] = np.nan

df = live_births.swap_live_births(df,
                                  user=getuser(),
                                  drop_if_primary_still_live=False)

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
        write_path = FILEPATH.format(
                         r=run_id, y=year, c=cause_type)

        df.to_csv(write_path, index=False)
