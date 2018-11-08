#----HEADER-------------------------------------------------------------------------------------------------------------
# Purpose:      Launch measles natural history model 
#***********************************************************************************************************************


#----CONFIG-------------------------------------------------------------------------------------------------------------
### clear memory
rm(list=ls())

### runtime configuration
username <- Sys.info()[["user"]]
if (Sys.info()["sysname"]=="Linux") {
  j_root <- "FILEPATH" 
  h_root <- file.path("FILEPATH", username)
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
description <- "vax coverage with 2017 JRF data, 2017 notifs, CFR using SDI, excluding VR data"
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

run_case_notification_smoothing <- FALSE # only need to run once when updating WHO case notification dataset
run_shocks_adjustment           <- FALSE

### set CODEm data-rich feeder model versions
male_CODEm_version   <- 429275
female_CODEm_version <- 429278
#***********************************************************************************************************************


#----RUN CODE-----------------------------------------------------------------------------------------------------------
cluster_proj <- "PROJECT"
source(file.path(j_root, "FILEPATH/01_natural_history_model.R"), echo=TRUE)
#***********************************************************************************************************************