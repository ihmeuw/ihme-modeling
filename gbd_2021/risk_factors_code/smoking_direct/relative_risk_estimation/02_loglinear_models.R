#
# 02_loglinear_models.R
#
#
rm(list = ls())
library(dplyr)
library(mrbrt001, lib.loc = "FILEPATH")
np <- import("numpy")
np$random$seed(as.integer(123))

args <- commandArgs(trailingOnly = TRUE)

if(interactive()){
  ro_pair <- "afib_and_flutter"
  out_dir <- "FILEPATH"
  WORK_DIR <- 'FILEPATH'
  inlier_pct <- 0.9
  setwd(WORK_DIR)
  source("FILEPATH/config.R")
} else {
  ro_pair <- args[1]
  out_dir <- args[2]
  WORK_DIR <- args[3]
  inlier_pct <- as.numeric(args[4])
  setwd(WORK_DIR)
  source("./config.R")
}

data <- readRDS(paste0(out_dir, "01_template_models/", ro_pair, ".RDS"))
df_data <- data$df_data

mrdata <- MRData()

mrdata$load_df(
  data = df_data, 
  col_obs = c('obs'),
  col_obs_se = c('obs_se'), 
  col_study_id = c('study_id'), 
  col_covs = as.list(c("signal"))
)

# Fit Linear Cov model with signal
cov_models <- list(LinearCovModel(
  alt_cov = "signal",
  use_re = TRUE,
  prior_beta_uniform=array(c(1.0, 1.0))
))

# No trimming
model <- MRBRT(
  data = mrdata,
  cov_models = cov_models,
  inlier_pct = 1.0
)

model$fit_model(inner_print_level=5L, inner_max_iter=200L, 
                outer_step_size=200L, outer_max_iter=100L) 

# Sample betas to use as priors for covariate selection.
sampling <- import("mrtool.core.other_sampling")
beta_samples <- sampling$sample_simple_lme_beta(1000L, model)
beta_std <- sd(beta_samples)

# Save data and model
py_save_object(object = model, 
               filename = paste0(out_dir, "02_loglinear_pkl_files/", ro_pair, ".pkl"), 
               pickle = "dill")

out <- append(data, list(beta_std = beta_std))
saveRDS(out, paste0(out_dir, "02_loglinear_models/", ro_pair, ".RDS"))

