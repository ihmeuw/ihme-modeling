#----HEADER-------------------------------------------------------------------------------------------------------------
# Purpose:      Launch diphtheria GBD model 
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
description              <- "DM cascade fix CFR, v.310 with annual CodCorrect results"
save_results_description <- paste0(description, ", vers. ", custom_version)
mark_model_best          <- TRUE

### set options
GAMMA_EPSILON      <- "with"   ## add extra uncertainty from gamma distribution in CFR model? ("with" or "without")
WRITE_FILES        <- "yes"    ## save files from all runs to version output folder? ("yes" or "no")
CALCULATE_NONFATAL <- "yes"    ## calculate nonfatal estimates? (i.e. run age/sex split code, save to cluster folder for upload) ("yes" or "no")
CALCULATE_FATAL    <- "no"     ## calculate fatal estimates, save to the cluster, and upload to the datagbase? ("yes" or "no")

### set CODEm data-rich feeder model versions
male_CODEm_version   <- 427328
female_CODEm_version <- 427331

### set CodCorrect version to pull for nonfatal computation, if applicable
compare_version      <- "latest"
#***********************************************************************************************************************


#----RUN CODE-----------------------------------------------------------------------------------------------------------
source(file.path(j_root, "FILEPATH/01_inverse_model.R"), echo=TRUE)
#***********************************************************************************************************************