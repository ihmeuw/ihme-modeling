#----HEADER-------------------------------------------------------------------------------------------------------------
# Author:      "USERNAME"
# Purpose:      Launch measles natural history model
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
description <- "DESCRIPTION"  
save_results_description <- paste0(description, ", vers. ", custom_version)
mark_model_best <- FALSE  

### set options
include_VR_data_in_CFR <- FALSE
which_covariate    <- "only_SDI"  ## use HAQ index or ln_LDI in CFR model? ("use_LDI" uses ln_mal and ln_LDI, "use_HAQI" uses ln_mal and HAQI, "only_HAQI" uses just HAQI, "only_SDI" uses just SDI)
MCV1_or_MCV2       <- "use_MCV2"  ## run nonfatal model with MCV1 & MCV2 or just MCV1 ("use_MCV2" or "MCV1_only")
MCV_lags           <- "no"        ## incorporate lags on MCV1? ("yes" or "no")
WRITE_FILES        <- "yes"       ## save files from all runs to version output folder? ("yes" or "no")
CALCULATE_NONFATAL <- "yes"       ## calculate nonfatal estimates? (i.e. run age/sex split code, save to cluster folder for upload) ("yes" or "no")
CALCULATE_COD      <- "yes"       ## calculate fatal estimates? (i.e. run cfr model, calc deaths, save to cluster folder for upload) ("yes" or "no")
use_lagged_covs    <- TRUE        ## TRUE to use 5-year lagged coverage covariates new GBD 2019 to effectively smooth stockout year effects; FALSE to use regular coverage time series
scale_to_converge  <- FALSE       ## TRUE beginning Step 4 GBD 2019 to scale covariates of incidence model prior to running model and SE calculation
ln_sia             <- FALSE       ## TRUE if converting SIA campaign doses to ln_unvaccinated like how treating mcv1 and mcv2 coverage
late_2019          <- TRUE        ## TRUE if running models for GBD 2019 resubmission and adding in annualized 2019 outbreak cases

run_case_notification_smoothing <- FALSE   
run_shocks_adjustment           <- FALSE

### set CODEm data-rich feeder model versions
male_CODEm_version   <- 623915 
female_CODEm_version <- 623909 

### set decomp_step 
decomp <- TRUE
decomp_step <- "step4"
#***********************************************************************************************************************


#----RUN CODE-----------------------------------------------------------------------------------------------------------
cluster_proj <- PROJECT
source(file.path(FILEPATH), echo=TRUE)
#***********************************************************************************************************************
