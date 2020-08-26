################################################################################
## DESCRIPTION: Register and launch ST-GPR Model from R ##
## INPUTS: Finalized processed anthropometric data ##
## OUTPUTS: Launched ST-GPR Model ##
## AUTHOR:  ##
## DATE: 31 January 2019 ##
################################################################################
rm(list = ls())

# System info
os <- Sys.info()[1]
user <- Sys.info()[7]

# Drives 
j <- if (os == "Linux") "FILEPATH" else if (os == "Windows") "J:/"
h <- if (os == "Linux") "FILEPATH" else if (os == "Windows") "FILEPATH"

# Base filepaths
work_dir <- paste0(j, 'FILEPATH')
share_dir <- 'FILEPATH' # only accessible from the cluster
code_dir <- if (os == "Linux") "FILEPATH" else if (os == "Windows") ""
stgpr_dir <- 'FILEPATH'


## LOAD DEPENDENCIES
source(paste0(code_dir, 'FILEPATH/primer.R'))
source(paste0(code_dir, 'FILEPATH/version_tools.R'))
source(paste0(code_dir, 'FILEPATH/anthro_utilities.R'))
setwd(stgpr_dir)
source('FILEPATH/register.R')
source('FILEPATH/sendoff.R')
source('FILEPATH/utility.R')

## PARSE ARGS
parser <- ArgumentParser()
parser$add_argument("--data_dir", help = "Site where data will be stored",
                    default = 'FILEPATH', type = "character")
parser$add_argument("--anthro_version_id", help = "version number for whole run",
                    default = 7, type = "integer")
parser$add_argument("--model_type", help = "which anthropometrics model to run, currently only height or bmi (adult vs child)",
                    default = 'bmi', type = "character")
parser$add_argument("--decomp_step", help = "decomp step we're uploading for",
                    default = "step2", type = "character")
parser$add_argument("--run_type", help = "test or production. 0 draws if test",
                    default = 'production', type = "character")
parser$add_argument("--stgpr_model_id", help = "Model id to use from config",
                    default = 1, type = "integer")
parser$add_argument("--ow_cw_version_id", help = "Crosswalk version to use for overweight model",
                    default = 1970, type = "integer")
parser$add_argument("--ob_cw_version_id", help = "Crosswalk version to use for obesity model",
                    default = 1976, type = "integer")
args <- parser$parse_args()
list2env(args, environment()); rm(args)


# Specify cluster arguments for launch
cluster_project = 'proj_covariates'
nparallel <- 50 #number of parallelizations! More parallelizations --> faster (if cluster is empty). I usually do 50-100.
logs <- sprintf('FILEPATH', user)
holdouts <- 0 # Keep at 0 unless you want to run cross-validation
draws <- ifelse(run_type == 'test', 0, 1000) #Either 0 or 1000 - run with 0 for test runs, 1000 for upload-ready runs

use_db <- !model_type %in% c('height') # for now, if not height use data uploaded to epi db


if(use_db){
  # #...or register an ST-GPR model for decomp. Do whichever you need!
  prev_ow_stgpr_id <- register_stgpr_model(me_name='metab_overweight',
                                           decomp_step = decomp_step,
                                           my_model_id = stgpr_model_id,
                                           crosswalk_version_id = ow_cw_version_id,
                                           holdouts = holdouts,
                                           draws = draws)
  
  prev_ob_stgpr_id <- register_stgpr_model(me_name='metab_obese',
                                           decomp_step = decomp_step,
                                           my_model_id = stgpr_model_id,
                                           crosswalk_version_id = ob_cw_version_id,
                                           holdouts = holdouts,
                                           draws = draws)

  
} else {
  
  # Check to make sure data path in config corresponds to most specified processing version, edit if not
  config <- fread(sprintf('%s/FILEPATH/%s_config_2019.csv', code_dir, model_type))
  
  
  # Register an ST-GPR model using a config for non-decomp run......
  prev_ow_stgpr_id <- register_stgpr_model(me_name='metab_overweight',
                                 path_to_config =  sprintf('%sFILEPATH/prev_ow_config_2019.csv', code_dir),
                                 my_model_id = 1,
                                 decomp_step = 'iterative',
                                 holdouts = holdouts,
                                 draws = draws)
  
  prev_ob_stgpr_id <- register_stgpr_model(me_name='metab_obese',
                                           path_to_config =  sprintf('%sFILEPATH/prev_ob_config_2019.csv', code_dir),
                                           my_model_id = 1,
                                           decomp_step = 'iterative',
                                           holdouts = holdouts,
                                           draws = draws)
}


# Submit ST-GPR model for your newly-created run_id!
st_gpr_sendoff(prev_ow_stgpr_id, cluster_project, logs, nparallel)
st_gpr_sendoff(prev_ob_stgpr_id, cluster_project, logs, nparallel)

# Link st_gpr run id with internal model version id (generated in model launch run all)
version_map_path <- paste0(code_dir, 'FILEPATH')
link.version(version_map_path, anthro_version_id, 'prev_ow_stgpr_id', prev_ow_stgpr_id)
link.version(version_map_path, anthro_version_id, 'prev_ob_stgpr_id', prev_ob_stgpr_id)

