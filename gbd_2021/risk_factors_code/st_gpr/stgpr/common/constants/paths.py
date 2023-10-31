import os

##### PATHS TO DATA #####
# Root directory for model output.
OUTPUT_ROOT: str = 'FILEPATH'
OUTPUT_ROOT_FORMAT: str = os.path.join(OUTPUT_ROOT, '{run_id}')

# Directories for model output.
DRAWS_ROOT_FORMAT: str = 'draws_temp_{holdout}/0'

# Individual model output files.
DATA_H5: str = 'data.h5'
STAGE1_H5: str = 'temp_0.h5'
FIT_STATS_CSV: str = 'fit_stats.csv'
OUTPUT_H5: str = 'output_0_0.h5'
DRAW_FORMAT: str = '{location}.csv'
PARAMETERS_CSV: str = 'parameters.csv'

# H5 objects
DATA_OBJ: str = 'data'
PREPPED_OBJ: str = 'prepped'
ADJ_DATA_OBJ: str = 'adj_data'
AMP_NSV_OBJ: str = 'amp_nsv'
STAGE1_OBJ: str = 'stage1'
SPACETIME_OBJ: str = 'st'
GPR_OBJ: str = 'gpr'
RAKED_OBJ: str = 'raked'

# Cluster output
ERROR_LOG_PATH_FORMAT = 'FILEPATH'
OUTPUT_LOG_PATH_FORMAT = 'FILEPATH'


##### PATHS TO CODE #####
# Main project directory - go three directories up from this file.
CODE_ROOT: str = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
