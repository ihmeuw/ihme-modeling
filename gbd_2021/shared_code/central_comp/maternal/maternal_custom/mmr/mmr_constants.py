"""constants for MMR pipeline"""

from gbd import constants, estimation_years

# Set ids
MMR_GBD_PROCESS_ID = 12
MATERNAL_CAPSTONE_CAUSE_SET_ID = 8

# Metadata ids
MMR_METADATA_TYPE_IDS = {
    'codcorrect': 1,
}

CURRENT_GBD_ROUND = constants.GBD_ROUND
CURRENT_ESTIMATION_YEARS = estimation_years.estimation_years_from_gbd_round(
    CURRENT_GBD_ROUND)

COV_ESTIMATES_FORMAT_FILENAME = 'locset_{}_birth_covariate_estimates.csv'
ALL_LIVE_BIRTHS_FORMAT_KEY = 'year_{}'
ALL_LIVE_BIRTHS_FILENAME = 'all_live_births.h5'

OUTDIR = 'FILEPATH'
LOGDIR = 'FILEPATH'
MMR_SUBDIRECTORIES = [
    'multi_year', 'multi_year/summaries',
    'single_year', 'single_year/draws',
    'single_year/summaries', 'constants']


class Columns():
    LIVE_BIRTH_VALUE_COL = 'mean_value'


LIVE_BIRTHS_COVARIATE_ID = 2298

AGGREGATE_LOCATION_SET_IDS = [35, 40, 3, 5, 11, 20, 24, 26, 28, 31, 32, 46]
MULTIPLE_ROOT_LOCATION_SET_IDS = [40]
AGGREGATE_AGE_GROUP_IDS = [22, 24, 27, 159, 162, 169, 197, 206, 284]
ALL_MOST_DETAILED_AGE_GROUP_IDS = [7, 8, 9, 10, 11, 12, 13, 14, 15]

OUTPUT_YEARS = list(range(1990, max(CURRENT_ESTIMATION_YEARS) + 1))
UPLOAD_TYPES = ['single', 'multi']
