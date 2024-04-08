####################################################################################################################################
##                                                                                                                                ##
## Overview: This script launches the function launch_prior() that in turn runs two functions: test_prior() and assemble_prior(). ##
##           Together they allow the creation of an ensemble linear prior for ST-GPR.                                             ##
##                                                                                                                                ##
## launch_prior()                                                                                                                 ##
##   - This function takes in the arguments assigned below and sets directories                                                   ##
##   - It runs test_prior() and/or assemble_prior() depending on whether "test_mods" and "average" are set to TRUE, respectively  ##
##   - It can also output a plot of the betas if "plot_betas" is set to TRUE                                                      ##
##                                                                                                                                ##
####################################################################################################################################

############
## SET-UP ##
############

rm(list = ls())
Sys.umask(mode = "0002")

library(data.table)
library(ini)

library(plyr)
library(dplyr)

args <- commandArgs(trailingOnly = TRUE)
new_settings_dir <- args[1]

if (interactive()) { # when interactive, only one definition can be run at a time
  version_estimate <- 999
  main_std_def <- "28_weeks"
  new_settings_dir <- "FILEPATH"
}

load(new_settings_dir)
list2env(new_settings, envir = environment())

source(paste0(working_dir, "launch_prior.R"))

set.seed(145153)

date <- gsub("-", "_", Sys.Date())
def <- gsub("_", " ", main_std_def)

username <- Sys.getenv("USER")

##############
## SET ARGS ##
##############

## Required
me <- paste0("Stillbirth >", def) # ST-GPR me name
decomp_step <- "iterative"        # Decomp step (for pulling covariates)
crosswalk_version_id <- NA        # Crosswalk version id (for pulling input data)

## Path to data (custom data csv if not using a crosswalk version)
if (model == "SBR") path_to_data <- "FILEPATH"
if (model == "SBR/NMR") path_to_data <- "FILEPATH"
if (model == "SBR + NMR") path_to_data <- "FILEPATH"

## Functions
test_mods  <- TRUE                # Whether to run the test_prior() function.  Not necessary if you've saved a previous run and just want to run assemble_prior()
average    <- TRUE                # Whether to run the assemble_prior() function. Not necessary if you just want to run test_prior()
plot_aves  <- TRUE                # Whether to plot diagnostics for model averages
age_trend  <- FALSE               # Whether to plot diagnostics for model averages by age instead of year
plot_betas <- TRUE                # Whether to plot diagnostics for ensemble betas

## Model Covariates -> list of covariate IDs to test for predictive validity in ensemble
cov_ids    <- c( 7,  8, 51, 143, 208, 463, 881, 1099, 2017, 2020) # antenatal care (1 visit) coverage = 7, antenatal care (4 visits) coverage = 8,
                                                                  # in-facility delivery = 51, skilled birth attendance = 143, maternal care and immunization = 208,
                                                                  # maternal education = 463, sdi = 881, haqi = 1099, prop 12 years of edu = 2017, education gini = 2020

prior_sign <- c(-1, -1, -1,  -1,  -1,  1,    1,    1,    1,    1) # Prior directions for covariates, in order of list cov_ids()
                                                                  # (-1 is negative correlation, 1 is positive correlation, 0 is no prior)


custom_covs <- NULL   # List of character vectors containing 1) the custom covariate_name_short and 2) the filepath where the custom covariate is found
                      # ex: custom_covs=list(c("FILEPATH"), c("FILEPATH"))

polynoms   <- NULL                # String of polynomial transformations for covariates using covariate_name_short (e.g. c("sdi^2"))
ban_pairs  <- NULL                # List of covariate IDs you wish to ban from occurring together in a sub-model (list of covariate IDs)
fixed_covs <- NULL                # Any covariates you want included in every model (i.e. age fixed effects)
random_effects <- c("(1|super_region_name/region_name/location_name)")  # Any random effects you want included in every model

## Model Settings
modtype        <- "lmer"          # Linear model type (lmer if using random effects, lm otherwise)
rank_method    <- "oos.rmse"      # OOS error method by which to rank sub-models. Options are Out-of-Sample RMSE ("oos.rmse") or Akaike Information Criterion ("aic")
n_mods         <- 50              # Number of top models to average over
forms_per_job  <- 16              # Number of model forms to test for each parallelized cluster job
by_sex         <- FALSE           # Whether your model is sex-specific
pred_ages      <- c(161)
data_transform <- "log"           # Transform function for your data (usually "logit" for proportions, "log" for continuous variables)
kos            <- 5               # Number of knock-outs for each sub-model
ko_prop        <- 0.15            # Proportion of data you want held out for each ko

## Cluster
proj           <- "proj_fertilitypop"     # Cluster project for launching jobs
m_mem_free     <- 3                       # Gigabytes of memory per job launched (depends on forms_per_job)

############
## LAUNCH ##
############

launch_prior(
  me = me,
  decomp_step = decomp_step,
  crosswalk_version_id = crosswalk_version_id,
  test_mods = test_mods,
  path_to_data = path_to_data,
  pred_ages = pred_ages,
  average = average,
  n_mods = n_mods,
  plot_aves = plot_aves,
  plot_betas = plot_betas,
  age_trend = age_trend,
  cov_ids = cov_ids,
  prior_sign = prior_sign,
  ban_pairs = ban_pairs,
  polynoms = polynoms,
  modtype = modtype,
  rank_method = rank_method,
  forms_per_job = forms_per_job,
  drop_nids = FALSE,
  fixed_covs = fixed_covs,
  custom_covs = custom_covs,
  random_effects = random_effects,
  username = username,
  by_sex = by_sex,
  version_estimate = version_estimate
)
