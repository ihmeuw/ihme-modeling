"""
This script makes weights for age sex splitting empirical deaths data using
expected death rate based on SDI.  

There are two main inputs: Expected Deaths by SDI, and SDI by age, sex, year,
and location.

The expected deaths by SDI just looks like two columns: SDI and Death Rate.
This gets merged with SDI by age, sex, year, location to get a dataframe that
has expected death rate by location, age, sex, and year.

There are expections made. One is for Sri Lanka. That is handled in a different
script.  There is also an exception made for Old Andhra Pradesh, also handled
in a different script.
"""

import itertools
import numpy as np
import pandas as pd
import datetime
import gbd.constants
from db_queries import get_covariate_estimates, get_age_metadata, get_population, get_location_metadata
from db_tools.ezfuncs import query

def get_distribution_set_version(distribution_set_version_id):
    q = """
	QUERY
        """.format(distribution_set_version_id)
    
    return query(q, conn_def='cod')


age_sex_distribution_dir = ""

age_groups = get_age_metadata(12)

sdi_estimates = get_covariate_estimates(881, gbd_round_id = 5)
sdi_estimates = sdi_estimates[['location_id', 'year_id', 'mean_value']]
sdi_estimates = sdi_estimates.rename(columns={'mean_value': 'sdi'})
sdi_estimates.loc[:,'sdi_merge'] = sdi_estimates['sdi'] * 1000
sdi_estimates.loc[:,'sdi_merge'] = np.around(sdi_estimates['sdi_merge'] / 5, 0) * 5
sdi_estimates.loc[:,'sdi_merge'] = sdi_estimates['sdi_merge'].astype('int64')

expected = pd.read_csv("")
expected.loc[:,'sdi_merge'] = expected['sdi'] * 1000
expected.loc[:,'sdi_merge'] = np.around(expected['sdi_merge'] / 5, 0) * 5
expected.loc[:,'sdi_merge'] = expected['sdi_merge'].astype('int64')


# Keep just the rates from most-detailed age groups
expected = expected.loc[(expected['age_group_id'].isin(age_groups['age_group_id'].tolist()))]

# Check to make sure we have the dimensions we'd expect
expected_indicies = [
    [1, 2, 3],
    age_groups['age_group_id'].tolist(),
    [i for i in range(0, 1001, 5)]
]
expected_dimensions = pd.DataFrame(
    list(itertools.product(*expected_indicies)),
    columns=['sex_id', 'age_group_id', 'sdi_merge'])
expected_dimensions.loc[:,'expected'] = 1
expected_dimensions = pd.merge(expected, expected_dimensions, on=['sex_id', 'age_group_id', 'sdi_merge'], how='outer')
assert(len(expected_dimensions.loc[(expected_dimensions['expected'].isnull())]) == 0)
assert(len(expected_dimensions.loc[(expected_dimensions['mean'].isnull())]) == 0)

# Merge on location
sdi_distribution = pd.merge(
    sdi_estimates, 
    expected[['sex_id', 'age_group_id', 'sdi_merge', 'mean']], 
    on=['sdi_merge'], how='left')


# Reformat 
keep_cols = ['location_id', 'year_id', 'sex_id', 'age_group_id', 'weight']
sdi_distribution = sdi_distribution.rename(columns={'mean': 'weight'})
sdi_distribution = sdi_distribution[keep_cols]

d = datetime.datetime.now()
filename = "sdi_distribution_{}".format(d.strftime("%Y%m%d"))
sdi_distribution.to_csv("{}/{}.csv".format(age_sex_distribution_dir, filename), index = False)




"""
Script that adds LKA weights to the weights used in splitting.
This script will overwrite the weights file.
"""

# Get the pre-made Sri Lanka distribution
lka_distribution = get_distribution_set_version(53)
lka_distribution = lka_distribution.loc[(lka_distribution['cause_id'] == 294)]

# Reformat
keep_cols = ['location_id', 'year_id', 'sex_id', 'age_group_id', 'weight']
lka_distribution.loc[:,'location_id'] = 17
lka_distribution_data = []
for year_id in range(1950, 2018):
    temp = lka_distribution.copy(deep=True)
    temp.loc[:,'year_id'] = year_id
    lka_distribution_data.append(temp)
    
lka_distribution = pd.concat(lka_distribution_data)
lka_distribution = lka_distribution[keep_cols]


# Take SDI distribution and append on LKA distribution
sdi_distribution = sdi_distribution.loc[(sdi_distribution['location_id'] != 17)]
distribution = pd.concat([sdi_distribution, lka_distribution]).reset_index(drop=True)


# Save
print("".format(age_sex_distribution_dir,d.strftime("%Y%m%d")))
distribution.to_csv("".format(age_sex_distribution_dir,d.strftime("%Y%m%d")), index=False)



"""
This script updates the weights used in splitting to include weights for "Old
Andhra Pradesh" It does this by just copying the weights for "Andhra Pradesh",
resetting the location_id to be that of "Old Andhra Pradesh", and appending
that to the weights.

This operation should be performed every time that there are new weights as long
as there is a need to split Old Andhra Pradesh.  This script should be resistent
to GBD round changes and changes to the location heirarchy identified by 
location_set_id = 82.  However, filepaths for the input and output weights will
need to be updated.

Please do NOT overwrite the original weights file, just move it into the archive
directory, read from there, and save to a different location without
overwriting.
"""

import gbd.constants
from db_queries import get_location_metadata

INPUT_FILEPATH = "".format(age_sex_distribution_dir,d.strftime("%Y%m%d"))
OUTPUT_FILEPATH = "".format(age_sex_distribution_dir,d.strftime("%Y%m%d"))

assert INPUT_FILEPATH != OUTPUT_FILEPATH, "Please do not overwrite the input file"

# read weights in 
weights = pd.read_csv(INPUT_FILEPATH)

assert not weights.duplicated().any(), "Weights have duplicate values; The weights have potentially already been modified."

# get location information 
location_metadata = get_location_metadata(location_set_id=82, gbd_round_id=gbd.constants.GBD_ROUND_ID)

# get location ids for current Andhra Pradesh and Old Andhra Pradesh
new_andhra_pradesh_loc_id = location_metadata[location_metadata.location_name == "Andhra Pradesh"].location_id.values[0]
old_andhra_pradesh_loc_id = location_metadata[location_metadata.location_name == "Old Andhra Pradesh"].location_id.values[0]

# make a copy of the weights that correspond to just current/new Andhra Pradesh
andhra_pradesh_weights = weights[weights.location_id == new_andhra_pradesh_loc_id].copy()

# make a copy of that
old_andhra_pradesh_weights = andhra_pradesh_weights.copy()

# set the new andhra pradesh location id to the old one
# Now we have weights for old andhra pradesh
old_andhra_pradesh_weights.loc[:,"location_id"] = old_andhra_pradesh_loc_id

# some simple asserts / test
assert old_andhra_pradesh_weights.shape == andhra_pradesh_weights.shape
assert (old_andhra_pradesh_weights.location_id == old_andhra_pradesh_loc_id).all()
assert (andhra_pradesh_weights.location_id == new_andhra_pradesh_loc_id).all()
# check that besides the location ids the two frames are identical
pd.testing.assert_frame_equal(left=old_andhra_pradesh_weights.drop("location_id", axis=1),
                              right=andhra_pradesh_weights.drop("location_id", axis=1))

# attach weights for old andhra pradesh back to the weights data frame
new_weights = pd.concat([weights, old_andhra_pradesh_weights], ignore_index=True)

# more "common sense" tests
assert new_weights.shape[0] == weights.shape[0] + old_andhra_pradesh_weights.shape[0]
assert old_andhra_pradesh_loc_id in new_weights.location_id.unique()
assert new_andhra_pradesh_loc_id in new_weights.location_id.unique()
assert new_weights[new_weights.location_id == new_andhra_pradesh_loc_id].shape[0] == new_weights[new_weights.location_id == old_andhra_pradesh_loc_id].shape[0]

# save 
new_weights.to_csv(OUTPUT_FILEPATH, index=False)

