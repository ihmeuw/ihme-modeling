#----HEADER-------------------------------------------------------------------------------------------------------------
# Author:       
# Date:         Februrary 2017, modified November 2017 for GBD 2017, modified for Buster the cluster April 2019 for GBD 2019
# Purpose:      Launch measles natural history model
# Location:     FILEPATH
# Runs code:    FILEPATH
#***********************************************************************************************************************


#----CONFIG-------------------------------------------------------------------------------------------------------------
### clear memory
rm(list=ls())

### runtime configuration
username <- Sys.info()[["user"]]
if (Sys.info()["sysname"]=="Linux") {
  j_root <- "ADDRESS"
  h_root <- "ADDRESS"
} else if (Sys.info()["sysname"]=="Darwin") {
  j_root <- "ADDRESS"
  h_root <- "ADDRESS"
} else {
  j_root <- "ADDRESS"
  h_root <- "ADDRESS"
}
#***********************************************************************************************************************


#----SETTINGS-----------------------------------------------------------------------------------------------------------
### set objects
custom_version <- format(lubridate::with_tz(Sys.time(), tzone="America/Los_Angeles"), "%m.%d.%y")

### save description of model run in .txt file in model version folder
description <- "Insert brief description"
save_results_description <- paste0(description, ", vers. ", custom_version)
mark_model_best <- FALSE

### set options
run_oos <- FALSE                   ## run out of sample validation in incidence model?
n_folds <- 0                      ## if run_oos == F, set to 0. Otherwise set to number of folds to split data for OOS validation
set.seed(42)                      ## set seed for OOS validation model
run_for_rrs <- FALSE               ## set to TRUE when running model to calculate RRs. 
include_VR_data_in_CFR <- FALSE
which_covariate    <- "only_SDI"  ## use HAQ index or ln_LDI in CFR model? ("use_LDI" uses ln_mal and ln_LDI, "use_HAQI" uses ln_mal and HAQI, "only_HAQI" uses just HAQI, "only_SDI" uses just SDI)
MCV1_or_MCV2       <- "use_MCV2"  ## run nonfatal model with MCV1 & MCV2 or just MCV1 ("use_MCV2" or "MCV1_only")
MCV_lags           <- "no"        ## incorporate lags on MCV1? ("yes" or "no")
WRITE_FILES        <- "yes"       ## save files from all runs to version output folder? ("yes" or "no")
CALCULATE_NONFATAL <- "yes"        ## calculate nonfatal estimates? (i.e. run age/sex split code, save to cluster folder for upload) ("yes" or "no")
CALCULATE_COD      <- "yes"       ## calculate fatal estimates? (i.e. run cfr model, calc deaths, save to cluster folder for upload) ("yes" or "no")
use_lagged_covs    <- TRUE        ## inc model: TRUE to use 5-year lagged coverage covariates new GBD 2019 to effectively smooth stockout year effects; FALSE to use regular coverage time series
scale_to_converge  <- FALSE       ## should be FALSE. (TRUE for part of Step 4 GBD 2019 to scale covariates of incidence model prior to running model and SE calculation)
sia_lag            <- 5          ## how many years to lag SIA coverage 
ln_sia             <- FALSE       ## TRUE if converting SIA campaign doses to ln_unvaccinated like how treating mcv1 and mcv2 coverage
custom_mcv1_coverage <- 37265   ## do you need to specify the coverage model version to use? set to NULL to use the current best coverage covariate version for the decomp step
custom_mcv2_coverage <- 37262   ## do you need to specify the coverage model version to use? set to NULL to use the current best coverage covariate version for the decomp step
bias_model_run_date <- "2020_08_14_3"  ## run date of dtp3 admin bias model you want to use to adjust SIA
trusted_case_distrib <- "binom"       # GBD 2020 switched from norm to binom
cap_draws          <- TRUE     ## cap incidence draws at 0.9 and backcalculate cases so fatal results are consistent?

elimination_locs   <- c("BTN", "LKA", "MDV", "CHN_354", "CHN_361", "TLS", "BHR", "KHM", "PRK", "OMN", "IRN", "JOR", "CHN") # elimination locations outside of trusted CN super-regions and other locs with good surveillance

manual_sdi         <- FALSE        ## manually set to pull SDI version used in GBD2019 best model to evaluate CFR model update changes
collapsed_cfr      <- TRUE        ## CFR model: collapse age specific data to generate one all age row per NID/location/year/hospital,rural,outbreak value
pop_level_lit_cfr  <- FALSE       ## include literature cfr data that is at the population level for nontrusted CN or data poor locations
cfr_model_type     <- "negbin"     ##  mrbrt or negbin CFR model
cfr_weights        <- "median"    ## weights in negbin cfr model, if applicable? options: cases, median (apply median cases of non population level rows to be weights of the population level rows), none
inlier_pct         <- 0.95        ## if using mrbrt, how much to trim (inlier_pct = 1 - trim_pct)
use_2019_cfr_locs  <- TRUE        ## CFR model: use nationally tagged data or use subnational

## include subnational
new_subnat_data    <- TRUE        ## include additional subnat admin data from 2020

## GBD 2020 covid adjustment settings
covid_adjustment <- TRUE # calculate covid adjusted NF cases and upload, calc scalars for CoD team? no fatal upload when set to TRUE
adjusted_case_draws_path <- FILEPATH # file path to covid adjusted measles case estimates as produced 
nocovid_codcorrect_vers <- VERSION # version of codcorrect that is covid free to calculate implied CFR 
nocovid_custom_version <- DATEVERSION # date version of custom incidence model with identical settings to this run other than covid adjustment; used to calculate implied CFR

## GBD 2019 Resubmission: 2019 case correction -- only ONE of the options below can be set to TRUE at a time
late_2019          <- FALSE        ## Used in final GBD2019 and for any outbreak correction. TRUE if running models for GBD 2019 resubmission and adding in annualized 2019 outbreak cases 
late_2019_ii       <- FALSE        ## TRUE if running models for GBD 2019 resubmission and adding in annualized 2019 outbreak cases 
outbreak_loc_ids <- c(27,72,97,125,133,135,102) # which locs to leave out last yr of data from stgpr because outbreak and inflate future estimates

run_case_notification_smoothing <- FALSE   ##  TRUE when updating WHO case notification dataset or adding trusted locs or addl years of estimation or central tools updated
run_shocks_adjustment           <- FALSE
manual_stgpr_offset             <- TRUE   ## if running st-gpr, should the manual offset be used or let it pick its own?
st_gpr_version                  <- "latest" ## which version of ST-GPR CN smoothing results to use? latest uses most recent run, specify path to use earlier run

### set CODEm data-rich feeder model versions
male_CODEm_version   <- 695405 
female_CODEm_version <- 695402 

### set decomp_step (effective GBD2019, gbd_round_=6)
decomp <- TRUE
decomp_step <- "iterative"
nf_xw_id <- 34904 #includes 2020 outbreak data
nf_bundle_id <- 7868
fatal_decomp_step <- "step3"
#***********************************************************************************************************************


#----RUN CODE-----------------------------------------------------------------------------------------------------------
cluster_proj <- "proj_cov_vpd"
source(FILEPATH)
#***********************************************************************************************************************
