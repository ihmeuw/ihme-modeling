"""constants for MMR pipeline"""

CAN_ADD_TO_COMPARE_VERSION = False
MMR_GBD_PROCESS_ID = 12
REQUIRED_GBD_METADATA_TYPE_IDS = [1]
CODCORRECT_GBD_METADATA_TYPE_ID = 1

COV_ESTIMATES_FORMAT_FILENAME = 'locset_{}_birth_covariate_estimates.csv'
ALL_LIVE_BIRTHS_FORMAT_KEY = 'year_{}'
ALL_LIVE_BIRTHS_FILENAME = 'all_live_births.h5'

OUTDIR = 'FILEPATH'
MMR_SUBDIRECTORIES = [
    'multi_year', 'multi_year/summaries',
    'single_year', 'single_year/draws',
    'single_year/summaries', 'constants']

class Columns():
    LIVE_BIRTH_VALUE_COL = 'mean_value'

LIVE_BIRTHS_COVARIATE_ID = 2298
# POPULATION_LOCATION_SET_ID = 35

AGGREGATE_LOCATION_SET_IDS = [89, 40, 3, 5, 11, 20, 24, 26, 28, 31, 32, 46]
MULTIPLE_ROOT_LOCATION_SET_IDS = [40]
AGGREGATE_AGE_GROUP_IDS = [22, 24, 27, 159, 162, 169, 197, 206, 284]
ALL_MOST_DETAILED_AGE_GROUP_IDS = [7, 8, 9, 10, 11, 12, 13, 14, 15]

OUTPUT_YEARS = list(range(1990, 2019 + 1))
UPLOAD_TYPES = ['single', 'multi']
