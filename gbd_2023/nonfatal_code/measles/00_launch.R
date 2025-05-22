#----HEADER-------------------------------------------------------------------------------------------------------------
# MEASLES LAUNCH SCRIPT
#***********************************************************************************************************************

#----CONFIG-------------------------------------------------------------------------------------------------------------
### clear memory
rm(list=ls())

### runtime configuration
username <- Sys.info()[["user"]]
if (Sys.info()["sysname"]=="Linux") {
  REDACTED
} else if (Sys.info()["sysname"]=="Darwin") {
  REDACTED
} else {
  REDACTED
}
#***********************************************************************************************************************

#----SETTINGS-----------------------------------------------------------------------------------------------------------
### set objects
custom_version <- format(lubridate::with_tz(Sys.time(), tzone="America/Los_Angeles"), "%m.%d.%y")

description <- "confirming script runs on cluster prior to final gbd 2023 branch merge"

save_results_description <- paste0(description, ", vers. ", custom_version)
mark_model_best <- F

### Out of sample validation
run_oos <- F                  
if (run_oos == F) {
  n_folds <- 0
}else{
  n_folds <- 5 
}
set.seed(42)        ## set seed for OOS validation model
run_for_rrs <- F    ## set to TRUE when running model to calculate relative risks. Will run special incidence model with coverage (not ln(1-coverge)) as covariate and then stop (does not predict out)

MCV1_or_MCV2<- "use_MCV2"         ## Options: "use_MCV2" or "MCV1_only". Choose separate coverage modeling strategy run nonfatal model with MCV1 & MCV2 or just MCV1 ("use_MCV2" or "MCV1_only")
MCV_lags           <- "no"        ## incorporate lags on MCV1? ("yes" or "no")
WRITE_FILES        <- "yes"       ## save files from all runs to version output folder? ("yes" or "no")
CALCULATE_NONFATAL <- "yes"       ## calculate nonfatal estimates? (i.e. run age/sex split code, save to cluster folder for upload) ("yes" or "no")
CALCULATE_COD      <- "yes"       ## calculate fatal estimates? (i.e. run cfr model, calc deaths, save to cluster folder for upload) ("yes" or "no")
which_covariate    <- "only_SDI"  ## use HAQ index or ln_LDI in CFR model? ("use_LDI" uses ln_mal and ln_LDI, "use_HAQI" uses ln_mal and HAQI, "only_HAQI" uses just HAQI, "only_SDI" uses just SDI)
use_lagged_covs    <- TRUE        ## inc model: TRUE to use 5-year lagged coverage covariates new GBD 2019 to effectively smooth stockout year effects; FALSE to use regular coverage time series
scale_to_converge  <- FALSE       ##  F/T
sia_lag            <- 5           ## how many years to lag SIA coverage
ln_sia             <- FALSE       ## TRUE if converting SIA campaign doses to ln_unvaccinated 
custom_mcv1_coverage <- NULL      ## do you need to specify the coverage model version to use? set to NULL to use the current best coverage covariate version 
custom_mcv2_coverage <- NULL      ## do you need to specify the coverage model version to use? set to NULL to use the current best coverage covariate version 
trusted_case_distrib <- "binom"   ## "norm" or "binom)
cap_draws            <- TRUE      ## cap incidence draws at 0.9 and backcalculate cases

# Sex and age splitting
cod_or_mrbrt <- "cod" # Use cod weights or MRBRT incidence age pattern to sex and age split incidence estimates

# Case notification adjustments
include_outbreaks           <- TRUE # include adjusted case notification estimates in outbreak locations in trusted locations?
include_monthly_cases       <- TRUE ## Include averaged monthly WHO reports for year following the current JRF?

# Define measles elimination locs, outside of trusted case notificaton superregions
elimination_locs   <- c("BTN", "LKA", "MDV", "CHN_354", "CHN_361", "TLS", "BHR", "KHM", "PRK", "OMN", "IRN", "JOR", "CHN") 

# Covid inclusive settings
covid_inclusive_vax <- T #use covid inclusive vaccine coverage estimates?
add_masks <- T # do you want to use masking covariates in the model? 

# Case notification fill settings: how to impute missing data for trusted locations
run_case_notification_smoothing <- FALSE  # Default is false, should be set to true when new JRF data is added to the non-fatal model. 
st_gpr_version                  <- 215488 # if not running new case notification smoothing model, indicate which version of STGPR case notification smoothing to use

# CFR data toggles
manual_sdi         <- FALSE       ## manually set to pull SDI 
collapsed_cfr      <- TRUE        ## CFR model: collapse age specific data to generate one all age row per NID/location/year/hospital,rural,outbreak value?
sex_age_spec_cfr   <- FALSE       ## Use sex and age split CFR estimates generated from MRBRT model? Or use non age non sex specific CFR inputs?
pop_level_lit_cfr  <- TRUE        ## include literature cfr data that is at the population level for nontrusted CN locations?
include_VR_data_in_CFR <- FALSE   ## include vital registration data in case fatality ratio model?
cfr_model_type     <- "negbin"    ## run mrbrt or negbin CFR model
cfr_weights        <- "cases"     ## what weights in negbin cfr model, if applicable? options: cases, median, none
inlier_pct         <- 0.95        ## if using mrbrt, how much to trim (inlier_pct = 1 - trim_pct)
use_2019_cfr_locs  <- FALSE       ## 
cfr_use_re         <- FALSE       ## Include location random effects in CFR negative binomial model

# If running fatal code only, which date of non-fatal run do you want use? 
non_fatal_run_date <- "DATE" 

### set CODEm data-rich feeder model versions
female_CODEm_version <- 749978
male_CODEm_version   <- 749977

#set bundle and xw id 
nf_bundle_id <- 6041 # measles case notification bundle smoothing bundle
nf_xw_id <- 44954 # get crosswalk version
sia_bundle_id <- 10074 # SIA bundle 
sia_bundle_version <- 46901
cn_smoothing_bundle_id <-  6041
cfr_bundle_version <- 47321 

#***********************************************************************************************************************
#----RUN CODE-----------------------------------------------------------------------------------------------------------
cluster_proj <- "PROJ_NAME"
source(file.path("/FILEPATH/01_natural_history_model.R"), echo=TRUE)
#***********************************************************************************************************************
