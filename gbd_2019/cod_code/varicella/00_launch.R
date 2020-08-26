#----HEADER-------------------------------------------------------------------------------------------------------------
# Author:      "USERNAME","USERNAME"
# Purpose:      Launch varicella natural history model 
# Runs code:    "FILEPATH"01_inverse_model.R
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
description <- "NOTES"
save_results_description <- paste0(description, ", vers. ", custom_version)
mark_model_best <- FALSE

### set options
GAMMA_EPSILON      <- "with"    ## add extra uncertainty from gamma distribution in cod model? ("with" or "without")
WRITE_FILES        <- "yes"     ## save files from all runs to version output folder? ("yes" or "no")
CALCULATE_NONFATAL <- "no"     ## calculate nonfatal estimates? (i.e. run age/sex split code, save to cluster folder for upload) ("yes" or "no")
SAVE_FATAL         <- "yes" 		## save fatal estimates to the cluster and upload to CodViz? ("yes" or "no")
HAQI_or_HSA        <- "HAQI" 		## which health access covariate? ("HAQI" or "HSA")
pop_run_id         <- "NULL"      ## Do you need to specify which run_id?

### set CODEm data-rich feeder model versions
male_CODEm_version   <- 633428 
female_CODEm_version <- 633431 

### set decomp_step
decomp <- TRUE
decomp_step <- "step4"
#***********************************************************************************************************************


#----RUN CODE-----------------------------------------------------------------------------------------------------------
source(file.path(j_root, ""FILEPATH"01_inverse_model.R"), echo=TRUE)
#***********************************************************************************************************************