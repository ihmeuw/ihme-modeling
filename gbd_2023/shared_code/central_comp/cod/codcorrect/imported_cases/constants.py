"""Constants for imported cases."""

# Hard-coded causes (special cases) that don't have spacetime restrictions
# but do have imported cases due to modeling strategy. Currently only drug use-related causes
ALCOHOL_ID = 560  # mental_alcohol
OPIOIDS_ID = 562  # mental_drug_opioids

# Mapping of age-restricted cause -> tuple of age group id limits
CAUSE_AGE_RESTRICTIONS = {ALCOHOL_ID: (None, 7), OPIOIDS_ID: (None, 3)}

# For age-restricted causes, extend square data set to this age group, filling with 0s
# if the cause is limited to younger age groups
EXTEND_TO_AGE_GROUP_ID = 7

# Number of cores (fthreads) to request
NUM_CORES = 40

# Parent location id for all data dense locations (CoD/CODEm-specific terminology)
DATA_DENSE_LOCATION_ID = 44640

# Location set id for grabbing relevant locations
DENSE_AND_SPARE_LOCATION_SET_ID = 43

# Model version type id used by imported cases
IMPORTED_CASES_MODEL_VERSION_TYPE_ID = 7


"""Filepath constants."""

# Root directory for results
ROOT_DIR = "FILEPATH"

# Pattern imported cases draws are saved in for save_results_cod
SAVE_FILE_PATTERN = "to_upload.csv"

# Version-specific directory
VERSION_DIR = f"{ROOT_DIR}" + "{}"
