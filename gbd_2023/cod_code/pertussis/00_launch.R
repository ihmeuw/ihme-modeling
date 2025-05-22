#----HEADER-------------------------------------------------------------------------------------------------------------
# Author:       REDACTED
# Purpose:      Launch pertussis natural history model 
# sbatch command:
# username <- Sys.info()[["user"]]
# system(paste0("sbatch -J pertussis_model --mem 200G -c 5 -C archive -t 08:00:00 -p long.q -A proj_cov_vpd -o /FILEPATH/", username, "/%x.o%j ",
#              "/FILEPATH/execRscript.sh -s /FILEPATH/", username, "/pertussis/00_launch.R"))
#***********************************************************************************************************************


#----CONFIG-------------------------------------------------------------------------------------------------------------
### clear memory
rm(list=ls())

### runtime configuration
username <- Sys.info()[["user"]]
if (Sys.info()["sysname"]=="Linux") {
  j_root <- "/home/j" 
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
description <- "confirming script runs on cluster prior to final gbd 2023 branch merge"
save_results_description <- paste0(description, ", vers. ", custom_version)
mark_model_best <- F

### set options
HAQI_or_HSA        <- "HAQI_only" ## use HAQ index or ln_LDI in CFR model? ("use_HSA" or "use_HAQI" or "HAQI_only")
GAMMA_EPSILON      <- "with"      ## add extra uncertainty from gamma distribution in CFR model? ("with" or "without")
WRITE_FILES        <- "yes"        ## save files from all runs to version output folder? ("yes" or "no")
run_for_rrs        <- FALSE       ## set to TRUE when running model to calculate RRs. will run special incidence model with coverage (not ln(1-coverge)) as covariate and then stop (does not predict out)
drop_early_jrf     <- TRUE        ## set to TRUE to drop data from 1980 and 1981. Model ran as if always TRUE before GBD 2021
CALCULATE_NONFATAL <- "yes"        ## calculate nonfatal estimates? (i.e. run age/sex split code, save to cluster folder for upload) ("yes" or "no")
CALCULATE_COD      <- "yes"       ## calculate fatal estimates? (i.e. run cfr model, calc deaths, save to cluster folder for upload) ("yes" or "no")
use_lagged_covs    <- TRUE        ## TRUE to use 5-year lagged coverage covariates new GBD 2019 to effectively smooth stockout year effects; FALSE to use regular coverage time series
custom_coverage    <- NULL        ## do you need to specify the coverage model version to use? set to NULL to use the current best coverage covariate version for the decomp step

# underreporting scalar application
scalar_method <- "predictions" #select "predictions" if the RE scalar is to be applied to model predictions (usual); select "incidence" if apply scalar to case notifications then modeling in STGPR; 

# random effect, currently this only applies if scalar method is set to "predictions"]
which_re    <- "CHE" #choose "CHE" for Switzerland, "top_5" for top 5 locations, "drq" to use mean of data star locations
CHE_RE      <- "current"   #use CHE RE calculated per run

# Covid related vectors
use_covid_inclusive_vax <- TRUE
use_mask_covariate      <- TRUE

### set CODEm data-rich feeder model versions when running fatal code
male_CODEm_version      <- 749976
female_CODEm_version    <- 749975

# Set which CFR data and model to run
which_cfr_data <- "GBD2021" # which round CFR data to use in model can be "GBD2023" versus "GBD2021"
which_cfr_model <- "linear" # "linear" or "logit"

# If only running fatal part of model, indicate non fatal run date to pull in incidence predictions
non_fatal_run_date <- "07.17.24"

## set bundle_id and xw_id as needed for save_results_epi
incidence_bundle_id <- 7874 #stgpr shaped pertussis incidence bundle
incidence_bundle_version <- 43242
incidence_xw_id <- 41327
cfr_bundle_id <- 8246 #dismod shaped cfr bundle
cfr_bundle_version_id <- 46656 #query epi uploader GUI for most recent bundle version
cfr_xw_id <-  41615 
#***********************************************************************************************************************

#----RUN CODE-----------------------------------------------------------------------------------------------------------
cluster_proj <- "proj_cov_vpd"
source(file.path("/FILEPATH", "pertussis/01_natural_history_model.R"), echo=TRUE)

#***********************************************************************************************************************
