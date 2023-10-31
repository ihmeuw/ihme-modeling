###########################################################################################################
### Project: ST-GPR
### Purpose: Launch custom first stage prior testing and assemble for upload to ST-GPR
###########################################################################################################

######################################
############### SET-UP ###############
######################################

rm(list=ls())
Sys.umask(mode = 002)
os <- .Platform$OS.type
if (os=="windows") {
  j <- "FILEPATH"
  h <- "FILEPATH"
  k <- "FILEPATH"
} else {
  lib_path <- "FILEPATH"
  username <- Sys.info()[["user"]]
  j <- "FILEPATH"
  h <- paste0("FILEPATH",username)
  k <- "FILEPATH"
}
date<-gsub("-", "_", Sys.Date())

# Load Packages and Functions
pacman::p_load(data.table, ini)
source("FILEPATH/launch.R")

######################################
############## SET ARGS ##############
######################################

## Getting normal QSub arguments
args <- commandArgs(trailingOnly = TRUE)
param_map_filepath <- args[1]

## Retrieving array task_id
task_id <- as.integer(Sys.getenv("SGE_TASK_ID"))
param_map <- fread(param_map_filepath)

## Required
me <- param_map[task_id, me_names]    # ST-GPR me name
decomp_step <- param_map[task_id, step]    # Decomp step (for pulling covariates)
crosswalk_version_id <- param_map[task_id, crosswalk_version_id] # Crosswalk version id (for input pulling data)

path_to_data <- NA              # Path to data (custom data csv if not using a crosswalk version)
data_transform <- ifelse(me %in% c("hemoglobin","hemoglobin_pregnant"),"log","logit")      # Transform function for your data

message(me, crosswalk_version_id)

## Functions
test_mods <- TRUE               # Whether to run the test_prior() function.  
average <- TRUE                 # Whether to run the assemble_prior() function. 
plot_aves <- FALSE               # Whether to plot diagnostics for model averages
age_trend <- FALSE              # Whether to plot diagnostics for model averages by age instead of year
plot_betas <- FALSE              # Whether to plot diagnostics for ensemble betas

## Model Covariates

## ASFR, HIV prev, underweight SEV, wasting SEV, malaria prev, sickle cell traits (2), SDI, impaired kidney function SEV, HAQI, modern contraception prev, GDP per cap
cov_ids <- c(13, 49, 1229, 1233, 1072, 2271, 2272, 881, 2160, 1099, 18)   # List of covariate IDs to test for predictive validity in ensemble
if (me %like% "pregnant") cov_ids <- c(13,49,1072,2271,2272,881,2160,1099,18) # remove child-level covs for pregnant women
custom_covs <- list(c("normal_hemoglobin","FILEPATH/normal_hemoglobin.csv"),
                    c("log_gdp_per_capita","FILEPATH/log_gdp_per_capita.csv"))            
prior_sign <- c(-1, -1, -1, -1, -1, -1, -1, 1, -1, 1, 1, 1, 1)     # Prior directions for covariates, in order of list cov_ids() (-1 is negative correlation, 1 is positive correlation, 0 is no prior)
if (me %like% "pregnant") prior_sign <- c(-1, -1, -1, -1, -1, 1, -1, 1, 1, 1, 1)
if (!(me %in% c("hemoglobin","hemoglobin_pregnant"))) prior_sign <- prior_sign * -1
polynoms <- NULL                # String of polynomial transformations for covariates using covariate_name_short (e.g. c("sdi^2"))
ban_pairs <- NULL               # List of covariate IDs you wish to ban from occurring together in a sub-model (list of covariate IDs)
fixed_covs <- NULL              # Any covariates you want included in every model (i.e. age fixed effects)
random_effects <- c("(1|super_region_name/region_name/location_name)")  # Any random effects you want included in every model

## Model Settings
modtype <- "lmer"               # Linear model type (lmer if using random effects, lm otherwise)
rank_method <- "oos.rmse"       # OOS error method by which to rank sub-models. Options are Out-of-Sample RMSE ("oos.rmse") or Akaike Information Criterion ("aic")
n_mods <- 50                    # Number of top models to average over.
forms_per_job <- 15             # Number of model forms to test for each parallelized cluster job
pred_ages <- c(2:3, 388:389, 238, 34, 6:20, 30:32, 235)
if (me %like% "pregnant") pred_ages <- c(7:15)
by_sex <- TRUE                  # Whether your model is sex-specific
kos <- 5                       # Number of knock-outs for each sub-model
ko_prop <- 0.15                 # Proportion of data you want held out for each ko

## Cluster
proj <- "PROJECT"         # Cluster project for launching jobs
m_mem_free <- 4                 # Gigabytes of memory per job launched (depends on forms_per_job)

######################################
############### LAUNCH ###############
######################################

launch_prior(me=me, decomp_step=decomp_step, crosswalk_version_id=crosswalk_version_id, test_mods=test_mods, path_to_data=path_to_data,
             pred_ages=pred_ages, average=average, n_mods=n_mods, plot_aves=plot_aves, plot_betas=plot_betas, age_trend=age_trend,
             cov_ids=cov_ids, prior_sign=prior_sign, ban_pairs=ban_pairs, polynoms=polynoms, modtype=modtype,
             rank_method=rank_method, forms_per_job=forms_per_job, drop_nids=FALSE, fixed_covs=fixed_covs,
             custom_covs=custom_covs, random_effects=random_effects, username=username, by_sex=by_sex)
# END