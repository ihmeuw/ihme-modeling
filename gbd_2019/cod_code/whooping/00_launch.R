#----HEADER-------------------------------------------------------------------------------------------------------------
# Author:      "USERNAME"
# Purpose:      Launch pertussis natural history model 
# Runs code:    "FILEPATH"01_natural_history_model.R
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
description <- "NOTE"
save_results_description <- paste0(description, ", vers. ", custom_version)
mark_model_best <- FALSE

### set options
HAQI_or_HSA        <- "use_HSA"  ## use HAQ index or ln_LDI in CFR model? ("use_HSA" or "use_HAQI" or "HAQI_only")
GAMMA_EPSILON      <- "with"      ## add extra uncertainty from gamma distribution in CFR model? ("with" or "without")
WRITE_FILES        <- "yes"       ## save files from all runs to version output folder? ("yes" or "no")
CALCULATE_NONFATAL <- "no"        ## calculate nonfatal estimates? (i.e. run age/sex split code, save to cluster folder for upload) ("yes" or "no")
CALCULATE_COD      <- "yes"       ## calculate fatal estimates? (i.e. run cfr model, calc deaths, save to cluster folder for upload) ("yes" or "no")
use_lagged_covs    <- TRUE        ## TRUE to use 5-year lagged coverage covariates new GBD 2019 to effectively smooth stockout year effects; FALSE to use regular coverage time series
CHE_RE             <- "current"   ## if "gbd2017", use GBD 2017 Swizterland RE; if "current", then use CHE RE calculated per run

### set CODEm data-rich feeder model versions
male_CODEm_version   <- 629573 
female_CODEm_version <- 629570 

### set decomp_step (effective GBD2019)
decomp <- TRUE
decomp_step <- "step4"
#***********************************************************************************************************************


#----RUN CODE-----------------------------------------------------------------------------------------------------------
cluster_proj <- "PROJECT"
source(file.path(j_root, ""FILEPATH"01_natural_history_model.R"), echo=TRUE)
#***********************************************************************************************************************