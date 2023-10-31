from stgpr.model import paths

# GLOBAL SETTINGS

# gbd round
GBD_ROUND_ID = 6

# decomp nonsense
VALID_DECOMP_ARGS = ['iterative', 'step1']

# set the location level for which everything less than is a region, SR, etc
NATIONAL_LEVEL = 3

# main variables
SPACEVAR = 'location_id'
TIMEVAR = 'year_id'
AGEVAR = 'age_group_id'
SEXVAR = 'sex_id'
DATAVAR = 'data'
VARIANCE_VAR = 'variance'
OUTLIER_VAR = 'is_outlier'
IDS = [SPACEVAR, TIMEVAR, AGEVAR, SEXVAR]


# modeling variables
STAGE1_VAR = 'stage1'
ST_VAR = 'st'


# specify maximum number of parallel submissions of full model run
MAX_SUBMISSIONS = 10

# Amplitude and NSV thresholds
AMP_THRESHOLD = 10
AMP_CUTOFF_DEFAULT_PERCENTILE = 80  # default - 80th percentile of data density
NSV_THRESHOLD = 10

# shell names
PYSHELL = '{dir}/py_fair.sh'.format(dir=paths.SHELLS_ROOT)
PYSING_SHELL = '{dir}/py_singularity.sh'.format(dir=paths.SHELLS_ROOT)

# script names
MASTER_SCRIPT = '{}/run_master.py'.format(paths.MODEL_ROOT)
PREP_SCRIPT = '{}/prep.py'.format(paths.MODEL_ROOT)
KO_SCRIPT = '{}/run_ko.py'.format(paths.MODEL_ROOT)
STAGE1_SCRIPT = '{}/stage1.R'.format(paths.MODEL_ROOT)
STAGE1_UPLOAD_SCRIPT = '{}/stage1_upload.py'.format(paths.MODEL_ROOT)
ST_SCRIPT = '{}/run_spacetime.py'.format(paths.MODEL_ROOT)
ST_UPLOAD_SCRIPT = "{}/spacetime_upload.py".format(paths.MODEL_ROOT)
IM_SCRIPT = '{}/calculate_intermediaries.py'.format(paths.MODEL_ROOT)
IM_UPLOAD = '{}/calculate_intermediaries_upload.py'.format(paths.MODEL_ROOT)
GPR_SCRIPT = '{}/run_gpr.py'.format(paths.MODEL_ROOT)
GPR_UPLOAD_SCRIPT = '{}/gpr_upload.py'.format(paths.MODEL_ROOT)
RAKE_SCRIPT = '{}/rake.py'.format(paths.MODEL_ROOT)
RAKE_UPLOAD_SCRIPT = '{}/rake_upload.py'.format(paths.MODEL_ROOT)
POST_SCRIPT = '{}/post.py'.format(paths.MODEL_ROOT)
EVAL_SCRIPT = '{}/evaluate_rmse.py'.format(paths.MODEL_ROOT)
EVAL_UPLOAD_SCRIPT = '{}/eval_upload.py'.format(paths.MODEL_ROOT)
CLEANUP_SCRIPT = '{}/clean.py'.format(paths.MODEL_ROOT)