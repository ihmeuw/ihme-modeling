#----HEADER-------------------------------------------------------------------------------------------------------------
# Author:       USERNAME
# Purpose:      Launch pertussis natural history model
# Location:     FILEPATH
# Runs code:    FILEPATH
# Qsub command: QSUB
#***********************************************************************************************************************


#----CONFIG-------------------------------------------------------------------------------------------------------------
### clear memory
rm(list=ls())

### runtime configuration
username <- Sys.info()[["user"]]
if (Sys.info()["sysname"]=="Linux") {
  j_root <- "/FILEPATH"
  h_root <- file.path("/FILEPATH", username)
} else if (Sys.info()["sysname"]=="Darwin") {
  j_root <- "~/FILEPATH"
  h_root <- "~/FILEPATH"
} else {
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
}
#***********************************************************************************************************************


#----SETTINGS-----------------------------------------------------------------------------------------------------------
### set objects
custom_version <- format(lubridate::with_tz(Sys.time(), tzone="America/Los_Angeles"), "%m.%d.%y") 

### save description of model run in .txt file in model version folder
description <- "Covid adjusting 2020 estimates. Covid free coverage. Only fatal upload. Data from 2020 JRF release, no data from year 2020. Hyb with April 2021 ST only covidfree coverage CODEm."
save_results_description <- paste0(description, ", vers. ", custom_version)
mark_model_best <- FALSE

### set options
HAQI_or_HSA        <- "HAQI_only"  ## use HAQ index or ln_LDI in CFR model? ("use_HSA" or "use_HAQI" or "HAQI_only")
# ^ NOTE: swtich from "use_HSA" Step 3 GBD 2019 to not use MCI
GAMMA_EPSILON      <- "with"      ## add extra uncertainty from gamma distribution in CFR model? ("with" or "without")
WRITE_FILES        <- "yes"       ## save files from all runs to version output folder? ("yes" or "no")
run_for_rrs        <- FALSE        ## set to TRUE when running model to calculate RRs. will run special incidence model with coverage (not ln(1-coverge)) as covariate and then stop (does not predict out)
drop_early_jrf     <- TRUE        ## set to TRUE to drop data from 1980 and 1981. Model ran as if always TRUE before GBD 2021
CALCULATE_NONFATAL <- "yes"        ## calculate nonfatal estimates? (i.e. run age/sex split code, save to cluster folder for upload) ("yes" or "no")
CALCULATE_COD      <- "no"       ## calculate fatal estimates? (i.e. run cfr model, calc deaths, save to cluster folder for upload) ("yes" or "no")
use_lagged_covs    <- TRUE        ## TRUE to use 5-year lagged coverage covariates new GBD 2019 to effectively smooth stockout year effects; FALSE to use regular coverage time series
custom_coverage    <- 37263       ## do you need to specify the coverage model version to use? set to NULL to use the current best coverage covariate version for the decomp step
CHE_RE             <- "current"   ## if "gbd2017", use GBD 2017 Swizterland RE; if "current", then use CHE RE calculated per run

covid_adjustment   <- TRUE        ## new at end GBD 2020, if TRUE, should only be calculating NF and will read in covid adjusted cases (prepped separately) and replace NF est with those vals for 2020 before upload. Stops after NF, CoD team does fatal adj. Need to run full model with identical settings except this toggle F before running with this toggle T so will have counterfactual for CoD to adjust.
adjusted_inc_draws_path <- "/FILEPATH/2021_06_22_pertussis.csv" # file path to covid adjusted pertussis case estimates, loc, age,sex

### set CODEm data-rich feeder model versions
male_CODEm_version   <- 695399
female_CODEm_version <- 695396

decomp <- TRUE
fatal_decomp_step <- "step3"
decomp_step <- "iterative"

## set bundle_id and xw_id for save_results_epi
bundle_id <- 7874 #stgpr shape pertussis incidence bundle
xw_id <- 24893 #xw is idenitcal to bundle_vers, but required argument for save_results_epi so need to have xw
#***********************************************************************************************************************


#----RUN CODE-----------------------------------------------------------------------------------------------------------
cluster_proj <- "proj_cov_vpd"
source(file.path("FILEPATH/01_natural_history_model.R"), echo=TRUE)

#***********************************************************************************************************************
