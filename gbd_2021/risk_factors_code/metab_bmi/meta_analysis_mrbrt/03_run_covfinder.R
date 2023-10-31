# STEP 3: Run CovFinder

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

set.seed(3197)
##############################################################################

# Read in data, step 2 model, and candidate covariates
if(save_dir %like% "FILEPATH"){
  df <- fread(paste0(save_dir, "FILEPATH"))
  df[, a_mid := (a_0 + a_1)/2][, b_mid := (b_0 + b_1)/2][, linear := b_mid - a_mid]
  df_meta <- fread(paste0(save_dir, "FILEPATH"))
  mod2 <- py_load_object(filename = paste0(save_dir, "FILEPATH"), pickle = "dill")
  covs <- readRDS(paste0(save_dir, "FILEPATH"))
}


##############################################################################

# Pull out the candidate covs
bias_and_alpha_cols <- covs[[1]] # This set includes both alpha and bias covariates
bias_cols <- covs[[2]] # This set should only include standard bias covariates

# Combine the bias cols with the trimmed data
df_merge <- merge(df, df_meta[, c(bias_and_alpha_cols, "seq"), with = F], by = "seq")
if(nrow(df_merge) != nrow(df)){stop(message("The rows of the meta data and the trimmed data are not equal."))}

# Multiply bias columns by signal covariate value
df_merge <- make.eff.mod(df_merge, bias_and_alpha_cols, "signal")

e_m_cols <- grep("e_m_", names(df_merge), value = T)
e_m_bias <- e_m_cols[e_m_cols %like% paste(bias_cols, collapse = "|")]
cov_cols <- c("signal", e_m_cols)

# Sample the beta to get the SD
sampling <- import("mrtool.core.other_sampling")
   
beta_samples <- sampling$sample_simple_lme_beta(1000L, mod2)
beta_std <- sd(beta_samples)

# ##############################################################################
if(fit_model){
  
  # Create data object
  cov_dat <- MRData()
  cov_dat$load_df(
    data = df_merge,
    col_obs = "obs",
    col_obs_se = "obs_se",
    col_study_id = "study_id",
    col_covs = as.list(cov_cols)
  )
  
  ##### Covfinder with just bias covs -----
  covfinder_bias <- do.call(
    CovFinder,
    c(COV_FINDER_CONFIG,
      list(
        data = cov_dat,
        pre_selected_covs = list("signal"),
        covs = as.list(e_m_bias)),
      beta_gprior_std = BETA_PRIOR_MULTIPLIER * beta_std
    )
  )
  
  covfinder_bias$select_covs(verbose = TRUE)
  selected_bias_cols <- setdiff(covfinder_bias$selected_covs, "signal")
  
  ##### Covfinder with bias and alpha covs -----
  covfinder_alpha <- do.call(
    CovFinder,
    c(COV_FINDER_CONFIG,
      list(
        data = cov_dat,
        pre_selected_covs = list("signal"),
        covs = as.list(e_m_cols)),
      beta_gprior_std = BETA_PRIOR_MULTIPLIER * beta_std
    )
  )
  
  covfinder_alpha$select_covs(verbose = TRUE)
  selected_bias_alpha_cols <- setdiff(covfinder_alpha$selected_covs, "signal")
  
  ##### Make list of selected covs -----
  
  selected_covs <- list(
    
    selected_bias_cols,
    selected_bias_alpha_cols
    
  )
  
  
  print(paste0("Saving to ", save_dir, "FILEPATH"))
  saveRDS(selected_covs,
          file = paste0(save_dir, "FILEPATH"))
  fwrite(df_merge,
         file = paste0(save_dir, "FILEPATH"))
  
  print("success")
  
}