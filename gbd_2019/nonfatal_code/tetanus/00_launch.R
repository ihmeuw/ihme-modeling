#----HEADER-------------------------------------------------------------------------------------------------------------
# Author:      "USERNAME"; "USERNAME"edits
# Purpose:      Launch tetanus GBD model 
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
description              <- "NOTE"
save_results_description <- paste0(description, ", vers. ", custom_version)
mark_model_best          <- FALSE  # FALSE when only re-running for impairments

### set options
WRITE_FILES           <- "yes"       ## save files from all runs to version output folder? ("yes" or "no")
CALCULATE_IMPAIRMENTS <- "no"       ## calculate motor impairment estimates? ("yes" or "no")  
UPLOAD_NONFATAL       <- "yes"			 ## save_results (epi) to the database? ("yes" or "no")

### set CoD results to pull for nonfatal computation
compare_version     <- "latest"

### set FauxCorrect if downsampled to only 100 draws
FauxCorrect         <- FALSE

### set decomp_step
decomp <- TRUE
decomp_step <- "step4"
#***********************************************************************************************************************


#----RUN CODE-----------------------------------------------------------------------------------------------------------
source(file.path(FILEPATH), echo=TRUE)
#***********************************************************************************************************************
