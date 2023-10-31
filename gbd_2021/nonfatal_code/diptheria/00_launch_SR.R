#----HEADER-------------------------------------------------------------------------------------------------------------
# Author:       USERNAME
# Date:         Updated Spring 2019, Februrary 2017, modified Dec 2017 for GBD 2017
# Purpose:      Launch diphtheria GBD model
# Location:     FILEPATH
# Runs code:    FILEPATH
# Qsub command (below):
# QSUB
#***********************************************************************************************************************


#----CONFIG-------------------------------------------------------------------------------------------------------------
### clear memory
rm(list=ls())

### runtime configuration
username <- Sys.info()[["user"]]
if (Sys.info()["sysname"]=="Linux") {
  j_root <- "FILEPATH"
  h_root <- file.path("/homes", username)
} else if (Sys.info()["sysname"]=="Darwin") {
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
} else {
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
}
#***********************************************************************************************************************


#----SETTINGS-----------------------------------------------------------------------------------------------------------
### set objects
custom_version <- format(lubridate::with_tz(Sys.time(), tzone="America/Los_Angeles"), "%m.%d.%y")

### save description of model run in .txt file in model version folder
description              <- "NF only with fatal codem from april central run. Fatal model with age grp, HAQI and covid free lagged DTP3 covariates, no weights. With gamma epsilon."

save_results_description <- paste0(description, ", vers. ", custom_version)
mark_model_best          <- FALSE

### set options
GAMMA_EPSILON      <- "with"   ## add extra uncertainty from gamma distribution in CFR model? ("with" or "without")
WRITE_FILES        <- "yes"    ## save files from all runs to version output folder? ("yes" or "no")
CALCULATE_NONFATAL <- "yes"    ## calculate nonfatal estimates? (i.e. run age/sex split code, save to cluster folder for upload) ("yes" or "no")
CALCULATE_FATAL    <- "no"    ## calculate fatal estimates, save to the cluster, and upload to the datagbase? ("yes" or "no")
use_lagged_covs    <- TRUE     ## TRUE to use 5-year lagged coverage covariates new GBD 2019 to effectively smooth stockout year effects; FALSE to use regular coverage time series
fatal_fit          <- "add_HAQI" ## options: "normal" (following GBD 2017); "add_HAQI" (add HAQI as fatal neg binom covariate -- used in GBD2020); "age_cov_interaction" (add age_group_id * coverage interaction); "sr_FE" to add fixed effects on super region to GBD2017/GBD2019 model ## NOTE: started to code in, but not finished; leave set to "normal" for now (09.05.19)
fatal_res          <- FALSE   ## fit custom fatal model with res? read through 01 launch to make sure REs are how you want them before selecting. ## NOTE: started to code, but cannot generate draws yet. must run interactively, OOM killed if qsub because computationally intensive
custom_coverage    <- 37263   ## use to specify a custom DTP3 coverage covariate version. Set to NULL to use the version marked best for the decomp step.
weights            <- "none"  ## use to specify weighting scheme when running model without REs. Options: "none", "sample_size", "data_star"

### set CODEm data-rich feeder model versions for fatal modeling
male_CODEm_version   <-  695393
female_CODEm_version <-  695390

### set CodCorrect version to pull for nonfatal computation, if applicable
compare_version     <- "best"
codem_estimates     <- TRUE   #if calculating nonfatal, use raw estimates from fatal model (latest or best as specfied in compare_version) or codcorrected?

### Bundle_id and xw_id for save_results_epi if running NF
bundle_id <- 407 # diphtheria cfr bundle is bundle most directly linked to nf estimates
xw_id <- 34898 # set to match the xw used in CFR model that gets pulled in modeling script

### set FauxCorrect if downsampled to only 100 draws
FauxCorrect         <- FALSE  # when pulling get_draws, note that version hard coded in get_draws

### set decomp_step (effective GBD2019, gbd_round_=6)
decomp <- TRUE
decomp_step <- "iterative"
fatal_decomp_step <- "step3"
#***********************************************************************************************************************


#----RUN CODE-----------------------------------------------------------------------------------------------------------
source(file.path("/FILEPATH/01_inverse_model.R"), echo=TRUE)
#***********************************************************************************************************************
