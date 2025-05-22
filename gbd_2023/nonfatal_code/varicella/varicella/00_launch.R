#----HEADER-------------------------------------------------------------------------------------------------------------
# Author:       REDACTED
# Date:         February 2017, September 2023
# Purpose:      Launch varicella natural history model 
# Location:     /FILEPATH/00_launch.R
# Runs code:    /FILEPATH/01_inverse_model.R
# sbatch command:
# username <- Sys.info()[["user"]]
# system(paste0("FILEPATH, "/varicella/00_launch.R"))
#***********************************************************************************************************************

#----CONFIG-------------------------------------------------------------------------------------------------------------
### clear memory
rm(list=ls())

### runtime configuration
username <- Sys.info()[["user"]]
if (Sys.info()["sysname"]=="Linux") {
REDACTED
}
#***********************************************************************************************************************

#----SETTINGS-----------------------------------------------------------------------------------------------------------
### set objects
custom_version <- format(lubridate::with_tz(Sys.time(), tzone="America/Los_Angeles"), "%m.%d.%y")

### save description of model run in .txt file in model version folder
description <- "confirming script runs on cluster prior to final gbd 2023 branch merge"
save_results_description <- paste0(description, ", vers. ", custom_version)
mark_model_best <- F

### set options
GAMMA_EPSILON      <- "with"      ## add extra uncertainty from gamma distribution in COD model? ("with" or "without")
WRITE_FILES        <- "yes"       ## save files from all runs to version output folder? ("yes" or "no")
CALCULATE_NONFATAL <- "yes"       ## calculate nonfatal estimates? (i.e. run age/sex split code, save to cluster folder for upload) ("yes" or "no")
SAVE_FATAL         <- "yes" 	    ## save fatal estimates to the cluster and upload to CodViz? ("yes" or "no")
HAQI_or_HSA        <- "HAQI" 	    ## which health access covariate? ("HAQI" or "HSA")
pop_run_id         <- "NULL"      ## Do you need to specify which run_id? Tested for Step4 GBD 2019 so need to un-hard code '145' in modeling script; use "NULL" to pull current best run
old_custom_vers    <- NULL        ## Default is NULL; to hybridize with different codem and not rerun custom global model, specify old custom_version you want for non-DR locs; use NULL to rerun custom model. If envelope updates, must rerun custom model because model calls envelope directly (not just in save_results_cod)

### set CODEm data-rich feeder model versions
male_CODEm_version   <- 749979
female_CODEm_version <- 749980

### COVID variables
outlier_covid_years <-  T #do you want to remove post COVID COD data from model, due to changes in mortality envelope for these years? 

### VARICELLA VACCINE
apply_vaccine_adjustment <- T
ve_run_date <- "2024_08_13" # Date you ran the vaccine efficacy model in MRBRT 

### set crosswalk version used in DisMod seroprevalence model
bundle_id <- 49
vari_xw <- 47322
dismod_seroprevalence_version <- 880351
#***********************************************************************************************************************

#----RUN CODE-----------------------------------------------------------------------------------------------------------
cluster_proj <- "proj_cov_vpd"
source(file.path("/FILEPATH/01_inverse_model.R"), echo=TRUE)
#***********************************************************************************************************************