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

import pandas as pd
import gbd.constants as gbd  # shared package; install with 'pip install gbd'
from db_queries import get_location_metadata

INPUT_FILEPATH = ""
OUTPUT_FILEPATH = ""

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
old_andhra_pradesh_weights.location_id = old_andhra_pradesh_loc_id

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
