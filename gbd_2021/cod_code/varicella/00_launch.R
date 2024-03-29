#----HEADER-------------------------------------------------------------------------------------------------------------
# Author:      USERNAME
# Date:         Februrary 2017, modified Dec 2017 for GBD 2017
# Purpose:      Launch varicella natural history model
# Location:     FILEPATH
# Runs code:    FILEPATH
#***********************************************************************************************************************

#----CONFIG-------------------------------------------------------------------------------------------------------------
### clear memory
rm(list=ls())
#***********************************************************************************************************************

#----SETTINGS-----------------------------------------------------------------------------------------------------------
### set objects
custom_version <- format(lubridate::with_tz(Sys.time(), tzone="America/Los_Angeles"), "%m.%d.%y")

### save description of model run in .txt file in model version folder
description <- "New CODEm to hyb with (april 2021). Fatal only with gamma uncertainty in custom model."
save_results_description <- paste0(description, ", vers. ", custom_version)
mark_model_best <- FALSE

### set options
GAMMA_EPSILON      <- "with"    ## add extra uncertainty from gamma distribution in COD model? ("with" or "without")
WRITE_FILES        <- "yes"     ## save files from all runs to version output folder? ("yes" or "no")
CALCULATE_NONFATAL <- "no"     ## calculate nonfatal estimates? (i.e. run age/sex split code, save to cluster folder for upload) ("yes" or "no")
SAVE_FATAL         <- "yes" 		## save fatal estimates to the cluster and upload to CodViz? ("yes" or "no")
HAQI_or_HSA        <- "HAQI" 		## which health access covariate? ("HAQI" or "HSA")
pop_run_id         <- "NULL"      ## Do you need to specify which run_id? Tested for Step4 GBD 2019 so need to un-hard code '145' in modeling script; use "NULL" to pull current best run

### set CODEm data-rich feeder model versions
male_CODEm_version   <- 695354
female_CODEm_version <- 695486

### set decomp_step (effective GBD2019, gbd_round_=6)
decomp <- TRUE
decomp_step <- "iterative"
fatal_decomp_step <- "step3"

### set crosswalk version used in DisMod seroprevalence model- needed for save_results_epi to save custom varicella post processing results
bundle_id <- 49
vari_xw <- 30614
dismod_status <- "best" #which varicella seroprevalence dismod results to pull draws from? ("latest" or "best")
#***********************************************************************************************************************


#----RUN CODE-----------------------------------------------------------------------------------------------------------
cluster_proj <- "proj_cov_vpd"
source(file.path("FILEPATH/01_inverse_model.R"), echo=TRUE)
#***********************************************************************************************************************
