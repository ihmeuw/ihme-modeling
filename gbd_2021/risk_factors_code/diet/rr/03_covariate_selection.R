#
# 03_covariate_selection.R
#
#
library(dplyr)
library(mrbrt001, lib.loc = "FILEPATH")

# set seed
np <- import("numpy")
np$random$seed(as.integer(2738))
## 

if(interactive()){
  
  version <- "VERSION"
  ro_pair <- "redmeat_hemstroke"
  
  out_dir <- paste0("FILEPATH", version, "/")
  WORK_DIR <- "FILEPATH"
  
}else{
  args <- commandArgs(trailingOnly = TRUE)
  ro_pair <- args[1]
  out_dir <- args[2]
  WORK_DIR <- args[3]
  
}
setwd(WORK_DIR)
source(paste0(out_dir, "/config.R"))

# Read data
data <- readRDS(paste0(out_dir, "02_loglinear_models/", ro_pair, ".RDS"))
df_data <- data$df_data
df_tmp <- data$df
df_tmp <- df_tmp[as.numeric(rownames(df_data)),]


# Use config to determine if there are any pre_selected covs
ro_pre_selected_covs <- "signal"
if(ro_pair %in% PRE_SELECTED_COVS$ro_pair){
  
  tmp_covs <- as.vector(PRE_SELECTED_COVS[PRE_SELECTED_COVS$ro_pair == ro_pair,]$cov)
  
  ro_pre_selected_covs <- c(ro_pre_selected_covs, tmp_covs)
  
}


cov_names <- unique(c(ro_pre_selected_covs, data$x_covs))
candidate_covs <- cov_names[!(cov_names %in% ro_pre_selected_covs)]
non_sig_covs <- cov_names[!(cov_names %in% "signal")]

# Interaction with signal for all non-signal covariates
if (BIAS_COVARIATES_AS_INTX){
  for (cov in non_sig_covs) df_data[, cov] <- df_data$signal * df_tmp[, cov]
}else{
  for (cov in non_sig_covs) df_data[, cov] <-  df_tmp[, cov]
}

mrdata <- MRData()
  
mrdata$load_df(
  data = df_data, 
  col_obs = c('obs'),
  col_obs_se = c('obs_se'), 
  col_study_id = c('study_id'), 
  col_covs = as.list(cov_names)
)

loglinear_model <- readRDS(paste0(out_dir, "02_loglinear_models/", ro_pair, ".RDS"))

# Beta prior from first loglinear model results.
beta_gprior_std <- loglinear_model$beta_std
covfinder <- do.call(
    CovFinder,
    c(COV_FINDER_CONFIG, 
      list(
        pre_selected_covs = as.list(ro_pre_selected_covs),
        data = mrdata, 
        covs = as.list(candidate_covs)),
        beta_gprior_std = BETA_PRIOR_MULTIPLIER * beta_gprior_std
      )
    )
  
covfinder$select_covs(verbose = TRUE)

selected_covs <- covfinder$selected_covs
selected_covs

# Save data and selected covariates
out <- append(data, list(df_cov_selection=df_data, selected_covs=selected_covs))
saveRDS(out, paste0(out_dir, "03_covariate_selection_models/", ro_pair, ".RDS"))
