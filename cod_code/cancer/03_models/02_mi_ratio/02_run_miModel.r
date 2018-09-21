#!/usr/local/bin/R
#########################################
## Description: Loads functions to process MI model inputs with centralized, IHME ST-GPR
## Input(s): See individual functions
## Output(s): Outputs for each cause are saved by run_id in the IHME ST-GPR directory
## How To Use: source script, then call run_miModel.run_models for the models of interest
##                  Check status with functions in the 'run_mi_functions' script
##                  Can also be submitted as a job (see "Run Functions" section below)
## IMPORTANT NOTE: Must be run on cluster-prod. Requires R version 3.3.1 or later
#########################################
## clear workspace and load libraries
rm(list=ls())

source(file.path(h, 'cancer_estimation/utilities.R'))
source(get_path('run_mi_functions', process="cancer_model"))

## Load Required IHME Libraries
covariates_model_root <- get_path("covariates_model_root", process="cancer_model")
setwd(covariates_model_root) # required for IHME-shared function to work
source(file.path(covariates_model_root, 'init.r'))
source(file.path(covariates_model_root, 'register_data.r'))

## Set submission specifications
# holdouts = 0 if no out of sample cross-validation, otherwise, specify the number of knockout runs. Values 0-10 (0 indicating no cross-validation)
# nparallel: maximum number of jobs to submit if running in parallel, generally 30-50 works well
# step = increment if you specified a window of scale/lambda to run over in you config using an underscore (i.e. "5_20")
submission_specs = list()
submission_specs['notes'] = "testing"  # This entry can be updated to any string without affecting the models
submission_specs['holdouts'] = 0
submission_specs['cluster_project'] = 'proj_cancer_prep'
submission_specs['nparallel'] = 40
submission_specs['slots'] = 5
submission_specs['mark_best'] = 1
submission_specs['username'] = Sys.info()["user"]
run_miModel.set_submissionSpecs(submission_specs)

################################################################################
## Run Functions
################################################################################
if (!interactive()) run_miModel.run_models(nDraws=0, cause_list=c(), model_input_number=17, cluster_is_busy=FALSE)

###########
## END
###########
