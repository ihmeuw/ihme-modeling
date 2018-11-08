#----HEADER-------------------------------------------------------------------------------------------------------------
# Purpose:      Launch pertussis natural history model 
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
custom_version <- format(lubridate::with_tz(Sys.time(), tzone="America/Los_Angeles"), "%m.%d.%y") #"05.30.17"

### save description of model run in .txt file in model version folder
description <- "use dtp3 coverage including 2017 JRF, new case notifs"
save_results_description <- paste0(description, ", vers. ", custom_version)
mark_model_best <- FALSE

### set options
HAQI_or_HSA        <- "use_HSA"   ## use HAQ index or ln_LDI in CFR model? ("use_HSA" or "use_HAQI" or "HAQI_only")
GAMMA_EPSILON      <- "with"      ## add extra uncertainty from gamma distribution in CFR model? ("with" or "without")
WRITE_FILES        <- "yes"       ## save files from all runs to version output folder? ("yes" or "no")
CALCULATE_NONFATAL <- "yes"        ## calculate nonfatal estimates? (i.e. run age/sex split code, save to cluster folder for upload) ("yes" or "no")
CALCULATE_COD      <- "yes"       ## calculate fatal estimates? (i.e. run cfr model, calc deaths, save to cluster folder for upload) ("yes" or "no")

### set CODEm data-rich feeder model versions
male_CODEm_version   <- 427322 
female_CODEm_version <- 427325 
#***********************************************************************************************************************


#----RUN CODE-----------------------------------------------------------------------------------------------------------
cluster_proj <- "PROJECT"
source(file.path(j_root, "FILEPATH/01_natural_history_model.R"), echo=TRUE)
#***********************************************************************************************************************