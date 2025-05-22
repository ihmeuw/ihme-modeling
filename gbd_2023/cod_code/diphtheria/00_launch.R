#----HEADER-------------------------------------------------------------------------------------------------------------
# Author:       REDACTED
# Date:         Updated Spring 2019, February 2017, January 2024 
# Purpose:      Launch diphtheria GBD model 

#***********************************************************************************************************************
#
#----CONFIG-------------------------------------------------------------------------------------------------------------
### clear memory
rm(list=ls())

### runtime configuration
username <- Sys.info()[["user"]]
if (Sys.info()["sysname"]=="Linux") {
  j_root <- "/home/j" 
  h_root <- file.path("/homes", username)
} else if (Sys.info()["sysname"]=="Darwin") {
  j_root <- "~/J"
  h_root <- "~/H"
} else { 
  j_root <- "J:"
  h_root <- "H:"
}
#***********************************************************************************************************************

#----SETTINGS-----------------------------------------------------------------------------------------------------------
### set objects
custom_version <- format(lubridate::with_tz(Sys.time(), tzone="America/Los_Angeles"), "%m.%d.%y")

### save description of model run in .txt file in model version folder
description              <- "confirming script runs on cluster prior to final gbd 2023 branch merge, non-fatal only"
save_results_description <- paste0(description, ", vers. ", custom_version)
mark_model_best          <- FALSE

GAMMA_EPSILON      <- "with"   ## add extra uncertainty from gamma distribution in CFR model? ("with" or "without")
WRITE_FILES        <- "yes"    ## save files from all runs to version output folder? ("yes" or "no")
CALCULATE_NONFATAL <- "yes"    ## calculate nonfatal estimates? (i.e. run age/sex split code, save to cluster folder for upload) ("yes" or "no")
CALCULATE_FATAL    <- "no"    ## calculate fatal estimates, save to the cluster, and upload to the datagbase? ("yes" or "no")
use_lagged_covs    <- TRUE     ## TRUE to use 5-year lagged coverage covariates new GBD 2019 to effectively smooth stockout year effects; FALSE to use regular coverage time series
fatal_fit          <- "add_HAQI" ## options: "normal" (following GBD 2017); "add_HAQI" (add HAQI as fatal neg binom covariate -- used in GBD2020); "age_cov_interaction" (add age_group_id * coverage interaction); "sr_FE" to add fixed effects on super region to GBD2017/GBD2019 model ## NOTE: started to code in, but not finished; leave set to "normal" for now (09.05.19)
fatal_res          <- FALSE   ## fit custom fatal model with REs? read through 01 launch to make sure REs are how you want them before selecting. ## NOTE: started to code, but cannot generate draws yet. must run interactively, OOM killed if qsub because computationally itensive
custom_coverage    <- NULL   ## use to specify a custom DTP3 coverage covariate version. Set to NULL to use the version marked best for the decomp step.
weights            <- "none"  ## use to specify weighting scheme when running model without REs. Options: "none", "sample_size", "data_star"
old_custom_vers    <- NULL ## DEFAULT is NULL, otherwise will hybridize with different codem and not rerun custom global model, specify old custom_version you want for non-DR locs. This will also work if just need to reupload because envelope changed; use NULL to rerun custom model=
run_cn_adjustment  <- TRUE # Do you want to correct the incidence estimates to never be less than WHO JRF case notifications? 

# COVID covariates
covid_inclusive_vax <- T #set to TRUE if you want lagged vaccine estimates that are adjusted for COVID, else set to FALSE
including_masking   <- F # Do you want to include the GBD masking covariate in the fatal model? 
outlier_covid_years <- T #do you want to remove post COVID COD data from fatal model, due to changes in envelope? 

### set CODEm data-rich feeder model versions for fatal modeling
male_CODEm_version   <- 749973
female_CODEm_version <- 749974

# When running non fatal code, do you want to pull fatal results from CODEm or CodCorrect
fatal_source      <- "CC" # CC for CODCorrect "CODEm" for CODEm
scale_cod_correct <- FALSE # Do you want to use a scaled version of CodCorrect, default is FALSE

### set CodCorrect version to pull for nonfatal computation, if applicable
if(CALCULATE_NONFATAL == "yes"){

  if(fatal_source == "CODEm") {
    
  codem_version_m <- 744242
  codem_version_f <- 744243
  
  codem_models <- c(codem_version_m, codem_version_f)
  
  } else if (fatal_source == "CC"){

  codcorrect_version <- 449 
  
  }
}

# CFR covariates
cfr_model_version <- 834814
interpolate_cfr <- FALSE #Default is FALSE; do not interpolate CFR, versus: TRUE interprolate CFR for all years!

### Bundle_id and xw_id for save_results_epi if running NF
bundle_id <- 407 # diphtheria CFR bundle 
xw_id     <- 45115 # most recent sex and age split CFR data

#***********************************************************************************************************************

#----RUN CODE-----------------------------------------------------------------------------------------------------------
source(file.path("/FILEPATH"), echo=TRUE)
#***********************************************************************************************************************