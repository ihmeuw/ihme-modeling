#----HEADER-------------------------------------------------------------------------------------------------------------
# Author:       REDACTED
# Date:         Februrary 2017; Spring 2019; January 2024
# Purpose:      Launch tetanus GBD model
# Qsub command: un-comment, copy, re-comment system call, paste and run call in console
# sbatch command (slurm): 
# username <- Sys.info()[["user"]]
# system(paste0("sbatch FILEPATH/tetanus/00_launch.R"))

#***********************************************************************************************************************


#----CONFIG-------------------------------------------------------------------------------------------------------------
### clear memory
rm(list=ls())

### runtime configuration
username <- Sys.info()[["user"]]
if (Sys.info()["sysname"]=="Linux") {
  j_root <- "FILEPATH"
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
description              <- "confirming script runs on cluster prior to final gbd 2023 branch merge"

save_results_description <- paste0(description, ", vers. ", custom_version)
mark_model_best          <- FALSE  # FALSE when only re-running for impairments

### set options
WRITE_FILES           <- "yes"    ## save files from all runs to version output folder? ("yes" or "no")
CALCULATE_IMPAIRMENTS <- "no"    ## calculate motor impairment estimates? ("yes" or "no")  
UPLOAD_NONFATAL       <- "yes"	 ## save_results to the database? ("yes" or "no")
bundle_id             <- 406     ## if uploading nonfatal, need to pass bundle_id and xw_id as arguments. 
xw_id                 <- 41533   ## using cfr bundle because it is most directly linked to the non-fatal results

# which CFR model to pull results from?
cfr_model_version <- 801268 

# Include neonatal encephalopathy in moderate-severe dismod model excess mortality? 
include_neonatal_encephalopathy <- T # Default is TRUE

### Get CODEm or CodCorrect estimates for non-fatal reverse calculation? 
codem_estimates <- FALSE # If pulling from CODEm set to TRUE; if pulling from CODcorrect set to FALSE

if(codem_estimates == TRUE){
  
# define codem model numbers to pull death estimates from 
codem_under_one_F <- 745585
codem_over_one_F  <- 745591
codem_under_one_M <- 745582
codem_over_one_M  <- 745588

codem_versions <- c(codem_under_one_M, codem_under_one_F, codem_over_one_M, codem_over_one_F)

} else if (codem_estimates == FALSE){
  codcorrect_version <- 449
}

# 
scale_codcorrect    <- FALSE # Default is false, if using CODcorrect, do you want to use scaled results? 

### set FauxCorrect if downsampled to only 100 draws
FauxCorrect         <- FALSE

#***********************************************************************************************************************

#----RUN CODE-----------------------------------------------------------------------------------------------------------
source(file.path("FILEPATH", "tetanus/01_inverse_model.R"), echo=TRUE)

#***********************************************************************************************************************
