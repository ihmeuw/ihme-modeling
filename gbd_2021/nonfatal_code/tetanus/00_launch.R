#----HEADER-------------------------------------------------------------------------------------------------------------
# Author:      USERNAME
# Date:         Februrary 2017; Spring 2019
# Purpose:      Launch tetanus GBD model
# Location:     FILEPATH
# Runs code:    FILEPATH
# FAIR Qsub command: un-comment, copy, re-comment system call, paste and run call in console:
# QSUB COMMAND

#***********************************************************************************************************************


#----CONFIG-------------------------------------------------------------------------------------------------------------
### clear memory
rm(list=ls())

#----SETTINGS-----------------------------------------------------------------------------------------------------------
### set objects

custom_version <- format(lubridate::with_tz(Sys.time(), tzone="America/Los_Angeles"), "%m.%d.%y")

### save description of model run in .txt file in model version folder
description              <- "Using April CODEm rerun results. NF estimation from covid free CODEm, not codcorrect. Trim in SS of CFR model. Impairments auto upload."
save_results_description <- paste0(description, ", vers. ", custom_version)
mark_model_best          <- TRUE  # FALSE when only re-running for impairments

### set options
WRITE_FILES           <- "yes"       ## save files from all runs to version output folder? ("yes" or "no")
CALCULATE_IMPAIRMENTS <- "yes"       ## calculate motor impairment estimates? ("yes" or "no")  << keep as yes for all other decomp steps other than step1
UPLOAD_NONFATAL       <- "yes"			 ## save_results (epi) to the database? ("yes" or "no")
bundle_id             <- 406       ## if uploading nonfatal, need to pass bundle_id and xw_id as arguments. update xw anytime change xw used in cfr model
xw_id                 <- 33194    ## using cfr bundle here because that is data that is most directly linked to the NF results

### set CoD results to pull for nonfatal computation
#raw CODEm model results (GBD 2017 and early GBD 2020)
codem_estimates <- TRUE

# OR CodCorrect draws
compare_version     <- "best"

### set FauxCorrect if downsampled to only 100 draws
# NOTE: MUST launch from specific shell (below, 03.26.19) to read in .py downsampling function for duration data when FauxCorrect run at 100 draws
#(FILEPATH))
FauxCorrect         <- FALSE

### set decomp_step
decomp <- TRUE
decomp_step <- "iterative"
fatal_decomp_step <- "step3"
#***********************************************************************************************************************


#----RUN CODE-----------------------------------------------------------------------------------------------------------

source(file.path("/share/code/vaccines", username, "tetanus/01_inverse_model.R"), echo=TRUE)

#***********************************************************************************************************************
