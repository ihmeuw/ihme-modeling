#
# 04_mixed_effects_models.R
#
#
rm(list = ls())
library(dplyr)
library(mrbrt001, lib.loc = "FILEPATH")
library(data.table)
library(ggplot2)
library(tidyr)
np <- import("numpy")
np$random$seed(as.integer(123))

args <- commandArgs(trailingOnly = TRUE)

if(interactive()){
  # the ro_pair of this script can be age-specific for CVD outcomes
  ro_pair <- "afib_and_flutter"
  out_dir <- "FILEPATH"
  WORK_DIR <- 'FILEPATH'
  #inlier_pct <- 0.9
  setwd(WORK_DIR)
  source("FILEPATH/config.R")
} else {
  ro_pair <- args[1]
  out_dir <- args[2]
  WORK_DIR <- args[3]
  #inlier_pct <- as.numeric(args[4])
  setwd(WORK_DIR)
  source("./config.R")
}

# Extract selected covariates
data <- readRDS(paste0(out_dir, "03_covariate_selection_models/", ro_pair, ".RDS"))
df_data <- data$df_data
df_tmp <- data$df
# Only keep rows that are not trimmed(WOW! Does this work? )
df_tmp <- df_tmp[as.numeric(rownames(df_data)),]

cov_names <- data$selected_covs
bias_covs <- cov_names[!cov_names == "exposure_linear"]

# Add interaction
for (cov in bias_covs) df_data[, cov] <- df_data$signal * df_tmp[, cov]

# Selected bias covariates plus signal
covs <- c("signal", bias_covs)

mrdata <- MRData()
mrdata$load_df(
  df_data,
  col_obs = c('obs'),
  col_obs_se = c('obs_se'), 
  col_study_id = c('study_id'),
  col_covs=as.list(covs)
)

# Combine cov models
# add prior to covs 
cov_models <- list()
for (cov in bias_covs) cov_models <- append(cov_models, 
                                            list(
                                              do.call(
                                                LinearCovModel, 
                                                list(
                                                  prior_beta_gaussian=array(c(0, BETA_PRIOR_MULTIPLIER * data$beta_std)),
                                                  use_re = F,
                                                  alt_cov=cov
                                                  
                                                )
                                              )
                                            )
)

# Mixed effects model
cov_models <- append(cov_models, LinearCovModel('signal', use_re=TRUE, 
                                                prior_beta_uniform=array(c(1.0, 1.0))))

model <- MRBRT(
  data=mrdata,
  cov_models = cov_models,
  inlier_pct = 1.0
)

model$fit_model(inner_print_level=5L, inner_max_iter=200L, 
                outer_step_size=200L, outer_max_iter=100L)

# Load signal model and data in Stage 1
signal_model <- py_load_object(filename=paste0(out_dir, "01_template_pkl_files/", ro_pair, ".pkl"), 
                               pickle = "dill")
orig_data <- readRDS(paste0(out_dir, "01_template_models/", ro_pair, ".RDS"))
df <- orig_data$df

# This should be provided by the user
NUM_POINTS <- 1001L
exposure_lower <- min(df[,c(REF_EXPOSURE_COLS, ALT_EXPOSURE_COLS)])
exposure_upper <- max(df[,c(REF_EXPOSURE_COLS, ALT_EXPOSURE_COLS)])
exposure <- seq(0, 100, length.out=NUM_POINTS)
min_cov <- rep(exposure_lower, NUM_POINTS)

if ('a_0' %in% REF_EXPOSURE_COLS){
  df_signal_pred <- data.frame(a_0=min_cov, a_1=min_cov, b_0=exposure, b_1=exposure)
} else {
  df_signal_pred <- data.frame(a_0=min_cov, b_0=exposure)
  names(df_signal_pred) <- c(REF_EXPOSURE_COLS, ALT_EXPOSURE_COLS)
}

# Predict using signal model and gridded exposure
data_signal_pred <- MRData()
data_signal_pred$load_df(
  df_signal_pred,
  col_covs = as.list(c(REF_EXPOSURE_COLS, ALT_EXPOSURE_COLS))
)
signal_pred <- signal_model$predict(data_signal_pred)

df_final_pred <- data.table(signal=signal_pred)

# add bias covs in the dataset
df_final_pred[, (bias_covs) := 0]

data_final_pred <- MRData()
data_final_pred$load_df(
  df_final_pred,
  col_covs = as.list(c("signal", bias_covs))
)

# create draws and prediction
sampling <- import("mrtool.core.other_sampling")
num_samples <- 1000L
beta_samples <- sampling$sample_simple_lme_beta(num_samples, model)
gamma_samples <- rep(model$gamma_soln, num_samples) * matrix(1, num_samples)

curve <- model$predict(data_final_pred)
draws <- model$create_draws(
  data_final_pred,
  beta_samples=beta_samples,
  gamma_samples=gamma_samples
)

# create dataset for predictions
df_pred <- data.table("pred" = curve)
df_pred$pred_lo <- apply(draws, 1, function(x) quantile(x, 0.025))
df_pred$pred_hi <- apply(draws, 1, function(x) quantile(x, 0.975))
df_pred$exposure <- exposure

# add exposure and weights to df
df$exposure <- apply(df[, c("b_0", "b_1")], 1, mean) - apply(df[, c("a_0", "a_1")], 1, mean)

# Extract weights of data point
weight <- t(do.call(rbind, 
                    lapply(1:length(signal_model$sub_models), 
                           function(i){signal_model$sub_models[[i]]$w_soln}))
) %*% signal_model$weights

df$w <- as.numeric(weight >= 0.1)

# Save model
py_save_object(object = model, 
               filename = paste0(out_dir, "04_mixed_effects_pkl_files/", ro_pair, ".pkl"), 
               pickle = "dill")


