#----HEADER-------------------------------------------------------------------------------------------------------------
# Purpose:      Launch tetanus GBD model 
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
WRITE_FILES           <- "yes"       ## save files from all runs to version output folder? ("yes" or "no")
CALCULATE_IMPAIRMENTS <- "yes"       ## calculate motor impairment estimates? ("yes" or "no")
UPLOAD_NONTATAL       <- "yes"			 ## save_results (epi) to the database? ("yes" or "no")

### set CoD results to pull for nonfatal computation
# CodCorrect draws
compare_version     <- "latest"
#***********************************************************************************************************************


#----RUN CODE-----------------------------------------------------------------------------------------------------------
source(file.path(j_root, "FILEPATH/01_inverse_model.R"), echo=TRUE)
#***********************************************************************************************************************
