"""Constants for imported cases."""

# Hard-coded causes (special cases) that don't have spacetime restrictions
# but do have imported cases due to modeling strategy. Currently only one cause
OPIOIDS_ID = 562  # mental_drug_opioids
ADDITIONALLY_RESTRICTED_CAUSE_IDS = [OPIOIDS_ID]

AGE_GROUP_SET_ID = 19

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
ROOT_DIR = "/ROOT_DIR/"

# Pattern imported cases draws are saved in for save_results_cod
SAVE_FILE_PATTERN = "to_upload.csv"

# Version-specific directory
VERSION_DIR = f"{ROOT_DIR}" + "{}"
