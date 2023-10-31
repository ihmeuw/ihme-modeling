# STEP 4: Run linear model with bias covariates

rm(list = ls())

# System info
os <- Sys.info()[1]
user <- Sys.info()[7]

# Drives
j <- if (os == "Linux") "FILEPATH" else if (os == "Windows") "FILEPATH"
h <- if (os == "Linux") paste0("FILEPATH", user, "/") else if (os == "Windows") "FILEPATH"

code_dir <- if (os == "Linux") paste0("FILEPATH", user, "/") else if (os == "Windows") ""
work_dir <- "FILEPATH"

library(dplyr)
source(paste0(code_dir, "FILEPATH")) # Loads functions like add_ui and get_knots
library(ggplot2)
library(data.table)
source("FILEPATH")
source(paste0(code_dir, 'FILEPATH'))
cause_ids <- get_ids("cause")
# Set up arguments
args <- commandArgs(trailingOnly = TRUE)

cause_num <- args[1]
save_dir <- args[2]
fit_model <- args[3]
bias_covs_only <- args[4]
lower_bound <- as.numeric(args[5])
upper_bound <- as.numeric(args[6])


##############################################################################
# Read in bias covariates, data, and priors

if(save_dir %like% "FILEPATH"){
  
  df <- fread(paste0(save_dir, "FILEPATH"))
  mod2 <- py_load_object(filename = paste0(save_dir, "FILEPATH"), pickle = "dill")
  all_covs <- readRDS(paste0(save_dir, "FILEPATH"))
  
}

if(bias_covs_only){
  bias_covs <- unlist(all_covs[1])
} else {
  bias_covs <- unlist(all_covs[2])
}

cov_cols <- c(bias_covs, "signal")

##############################################################################
if(fit_model){
  np <- import("numpy")
  np$random$seed(as.integer(3197))
  
  # Sample the beta to get the SD
  sampling <- import("mrtool.core.other_sampling")
  
  beta_samples <- sampling$sample_simple_lme_beta(1000L, mod2)
  beta_std <- sd(beta_samples)
  
  # Create data object
  dat4 <- MRData()
  dat4$load_df(
    data = df,
    col_obs = "obs",
    col_obs_se = "obs_se",
    col_study_id = "study_id",
    col_covs = as.list(cov_cols)
  )
  
  cov_models <- list()
  for (cov in bias_covs){
    cov_models <- append(cov_models, 
                         list(
                           do.call(
                             LinearCovModel, 
                             list(
                               alt_cov = cov,
                               prior_beta_gaussian = array(c(0, BETA_PRIOR_MULTIPLIER * beta_std))))))
  }
  
  
  # Mixed effects model
  cov_models <- append(cov_models, 
                       LinearCovModel(
                         alt_cov = 'signal', 
                         use_re = TRUE,
                         prior_beta_uniform = array(c(1.0, 1.0))))
  
  model <- MRBRT(
    data = dat4,
    cov_models = cov_models,
    inlier_pct = 1.0
  )
  
  model$fit_model(inner_print_level=5L, inner_max_iter=200L, outer_step_size=200L, outer_max_iter=100L)
  print(paste0("Saving model to ", save_dir, "FILEPATH"))
  py_save_object(model, filename = paste0(save_dir, "FILEPATH"), pickle = "dill")
  print("success")
  
} else {
  
  model <- py_load_object(paste0(save_dir, "FILEPATH"), pickle = "dill")
  
}