#-------------------Header------------------------------------------------
# SECONHAND SMOKE - GBD 2020
# Purpose: Run loglinear model
#------------------Set-up--------------------------------------------------

library(dplyr)
library(mrbrt001, lib.loc = "FILEPATH")

## Set seed
np <- import("numpy")
np$random$seed(as.integer(1987))

## Arguments
args <- commandArgs(trailingOnly = TRUE)
ro_pair <- args[1]
out_dir <- args[2]
WORK_DIR <- args[3]

setwd(WORK_DIR)
source("./config.R")


## Read in data file
data <- readRDS(paste0(out_dir, "FILEPATH", ro_pair, ".RDS"))
df_data <- data$df_data

mrdata <- MRData()


## Use the column names from MR-BRT
mrdata$load_df(
  data = df_data, 
  col_obs = c('obs'),
  col_obs_se = c('obs_se'), 
  col_study_id = c('study_id'), 
  col_covs = as.list(c("signal"))
)


## Fit Linear Cov model with signal
cov_models <- list(LinearCovModel(
  alt_cov = "signal",
  use_re = TRUE,
  prior_beta_uniform=array(c(1.0, 1.0))
))


## No trimming
model <- MRBRT(
  data = mrdata,
  cov_models = cov_models,
  inlier_pct = 1.0 # Data points were already trimmed in step 1. No additional trimming wanted. 
)

model$fit_model(inner_print_level=5L, inner_max_iter=200L, 
                outer_step_size=200L, outer_max_iter=100L) 


## Sample betas to use as priors for covariate selection.
sampling <- import("mrtool.core.other_sampling")
beta_samples <- sampling$sample_simple_lme_beta(1000L, model)
beta_std <- sd(beta_samples)


## Save data and model
py_save_object(object = model, 
               filename = paste0(out_dir, "FILEPATH", ro_pair, ".pkl"), 
               pickle = "dill")

out <- append(data, list(beta_std = beta_std))
saveRDS(out, paste0(out_dir, "FILEPATH", ro_pair, ".RDS"))
