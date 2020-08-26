#----HEADER-------------------------------------------------------------------------------------------------------------
# Author:      "USERNAME","USERNAME"
# Purpose:      Launch diphtheria GBD model 
#***********************************************************************************************************************


#----CONFIG-------------------------------------------------------------------------------------------------------------
### clear memory
rm(list=ls())

### runtime configuration
username <- Sys.info()[["user"]]
if (Sys.info()["sysname"]=="Linux") {
  j_root <- "FILEPATH" 
  h_root <- "FILEPATH"
} else if (Sys.info()["sysname"]=="Darwin") {
  j_root <- ""FILEPATH""
  h_root <- ""FILEPATH""
} else { 
  j_root <- ""FILEPATH""
  h_root <- ""FILEPATH""
}
#***********************************************************************************************************************


#----SETTINGS-----------------------------------------------------------------------------------------------------------
### set objects
custom_version <- format(lubridate::with_tz(Sys.time(), tzone="America/Los_Angeles"), "%m.%d.%y")

### save description of model run in .txt file in model version folder
description              <- DESCRIPTION
save_results_description <- paste0(description, ", vers. ", custom_version)
mark_model_best          <- FALSE

### set options
GAMMA_EPSILON      <- "with"   ## add extra uncertainty from gamma distribution in CFR model? ("with" or "without")
WRITE_FILES        <- "yes"    ## save files from all runs to version output folder? ("yes" or "no")
CALCULATE_NONFATAL <- "yes"    ## calculate nonfatal estimates? (i.e. run age/sex split code, save to cluster folder for upload) ("yes" or "no")
CALCULATE_FATAL    <- "yes"    ## calculate fatal estimates, save to the cluster, and upload to the datagbase? ("yes" or "no")
use_lagged_covs    <- TRUE     ## TRUE to use 5-year lagged coverage covariates new GBD 2019 to effectively smooth stockout year effects; FALSE to use regular coverage time series
fatal_fit          <- "normal" ## options: "normal"; "add_HAQI"; "age_cov_interaction"

### set CODEm data-rich feeder model versions
male_CODEm_version   <- VALUE 
female_CODEm_version <- VALUE 

### set CodCorrect version to pull for nonfatal computation, if applicable
compare_version     <- "latest"

### set FauxCorrect if downsampled to only 100 draws
FauxCorrect         <- FALSE  

### set decomp_step (effective GBD2019, gbd_round_=6)
decomp <- TRUE
decomp_step <- "step4"
#***********************************************************************************************************************


#----RUN CODE-----------------------------------------------------------------------------------------------------------
source(file.path(FILEPATH), echo=TRUE)
#***********************************************************************************************************************