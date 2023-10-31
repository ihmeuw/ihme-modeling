##--------------------
# Code paths and roots
##--------------------
CODE_PATHS <- list(
    FUNCTIONS_PATH = paste0("FILEPATH", Sys.info()['user'], "FILEPATH"),
    MAPPING_FUNCTION_PATH = paste0("FILEPATH",Sys.info()['user'],"FILEPATH/generic_map_function_national.R"),
    VACCINE_CODE_ROOT = paste0("FILEPATH",Sys.info()['user'],"FILEPATH"),
    VACCINE_FUNCTIONS_ROOT = file.path(paste0("FILEPATH",Sys.info()['user'],"FILEPATH"), 'funcs'),
    VACCINE_DIAGNOSTICS_ROOT = file.path(paste0("FILEPATH",Sys.info()['user'],"FILEPATH"), 'diagnostics_scripts'),
    CC_CODE_ROOT = "FILEPATH"
)

##--------------------
# Data paths and roots
##--------------------
DATA_ROOTS <- list(
    LIMITED_USE_SYMPTOM_SURVEY = "FILEPATH",
    MODEL_INPUTS_ROOT = "FILEPATH",
    DATA_INTAKE_ROOT = "FILEPATH",
    DATA_INTAKE_GAVI_ROOT = "FILEPATH",
    STATIC_VACCINE_INPUTS_ROOT = "FILEPATH",
    VACCINE_OUTPUT_ROOT = "FILEPATH"
)

# Log dirs.
SGE_ERROR_PATH <- paste0("FILEPATH",Sys.info()['user'],"FILEPATH")
SGE_OUTPUT_PATH <- paste0("FILEPATH",Sys.info()['user'],"FILEPATH")
SLURM_ERROR_PATH <- paste0("FILEPATH",Sys.info()['user'],"FILEPATH") 
SLURM_OUTPUT_PATH <- paste0("FILEPATH",Sys.info()['user'],"FILEPATH")

# R shell script and singularity image for submitting sbatches
R_SHELL_PATH <- 'FILEPATH/execRscript.sh'
R_IMAGE_PATH <- 'FILEPATH/ihme_rstudio_4051.img'