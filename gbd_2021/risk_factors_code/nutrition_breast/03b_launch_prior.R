###########################################################################################################
### Project: ST-GPR
### Purpose: Launch custom first stage prior testing and assemble for upload to ST-GPR
### Overview: This script launches the function launch_prior() that in turn runs two functions: test_prior() and assemble_prior(). 
###           Together they allow the creation of an ensemble linear prior for STGPR.
###########################################################################################################

######################################
############### SET-UP ###############
######################################
rm(list=ls())
os <- .Platform$OS.type
if (os=="Windows") {
  j <- "FILEPATH"
  h <- "FILEPATH"
  k <- "FILEPATH"
} else {
  lib_path <- "FILEPATH"
  username <- Sys.info()[["user"]]
  j <- "FILEPATH"
  h <- paste0("FILEPATH/",username)
  k <- "FILEPATH"
}
date<-gsub("-", "_", Sys.Date())

# Load Packages and Functions
pacman::p_load(data.table, ini)
source("FILEPATH/launch.R")
  
######################################
############## SET ARGS ##############
######################################

## Required
me <- "abfrate"             # ST-GPR me name
decomp_step <- "iterative"          # Decomp step (for pulling covariates)
crosswalk_version_id <- 36191       # Crosswalk version id (for pulling input data)
path_to_data <- NA              # Path to data (custom data csv if not using a crosswalk version)

## Functions
test_mods <- TRUE               # Whether to run the test_prior() function.  
average <- TRUE                 # Whether to run the assemble_prior() function. 
plot_aves <- FALSE              # Whether to plot diagnostics for model averages
age_trend <- FALSE              # Whether to plot diagnostics for model averages by age instead of year
plot_betas <- FALSE             # Whether to plot diagnostics for ensemble betas

## Model Covariates
cov_ids <- c(881, 2038, 149, 463, 8)   # List of covariate IDs to test for predictive validity in ensemble
custom_covs <- list(c("hiv_mort_repro","FILEPATH/hiv_mort_repro.csv"),
                    c("high_bmi_repro", "FILEPATH/high_bmi_repro.csv"), 
                    c("underweight_repro","FILEPATH/underweight_repro.csv"))             
prior_sign <- c(-1,1,1,1,1,-1,-1,-1)     # Prior directions for covariates, in order of list cov_ids() (-1 is negative correlation, 1 is positive correlation, 0 is no prior)
                                                         # Custom covariate prior directions should be included in order _after_ the cov_ids covariate
polynoms <- NULL                # String of polynomial transformations for covariates using covariate_name_short (e.g. c("sdi^2"))
ban_pairs <- NULL               # List of covariate IDs you wish to ban from occurring together in a sub-model (list of covariate IDs)
fixed_covs <- c("as.factor(age_group_id)")              # Any covariates you want included in every model (i.e. age fixed effects)
random_effects <- c("(1|super_region_name/region_name/location_name)")  # Any random effects you want included in every model

## Model Settings
modtype <- "lmer"               # Linear model type (lmer if using random effects, lm otherwise)
rank_method <- "oos.rmse"       # OOS error method by which to rank sub-models. Options are Out-of-Sample RMSE ("oos.rmse") or Akaike Information Criterion ("aic")
n_mods <- 50                    # Number of top models to average over.
forms_per_job <- 15             # Number of model forms to test for each parallelized cluster job
by_sex <- FALSE                 # Whether your model is sex-specific
pred_ages <- c(388, 389, 238)
data_transform <- "logit"       # Transform function for your data (usually "logit" for proportions, "log" for continuous variables)
kos <- 5                        # Number of knock-outs for each sub-model
ko_prop <- 0.15                 # Proportion of data you want held out for each ko

## Cluster
proj <- "PROJECT"         # Cluster project for launching jobs
m_mem_free <- 3                 # Gigabytes of memory per job launched (depends on forms_per_job)

######################################
############### LAUNCH ###############
######################################

launch_prior(me=me, decomp_step=decomp_step, crosswalk_version_id=crosswalk_version_id, test_mods=test_mods, pred_ages=pred_ages,
             average=average, n_mods=n_mods, plot_aves=plot_aves, plot_betas=plot_betas, age_trend=age_trend,
             cov_ids=cov_ids, prior_sign=prior_sign, ban_pairs=ban_pairs, polynoms=polynoms, modtype=modtype,
             rank_method=rank_method, forms_per_job=forms_per_job, drop_nids=FALSE, fixed_covs=fixed_covs,
             custom_covs=custom_covs, random_effects=random_effects, username=username, by_sex=by_sex)

# END