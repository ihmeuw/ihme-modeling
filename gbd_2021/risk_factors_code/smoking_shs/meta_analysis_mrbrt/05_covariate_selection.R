#-------------------Header------------------------------------------------
# SECONHAND SMOKE - GBD 2020
# Purpose: Covariate selection
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


## Read data
data <- readRDS(paste0(out_dir, "FILEPATH", ro_pair, ".RDS"))
df_data <- data$df_data
df_tmp <- data$df
df_tmp <- df_tmp[as.numeric(rownames(df_data)),]

## Identifying covariates 
cov_names <- c("exposure_linear", data$x_covs)
candidate_covs <- cov_names[!cov_names == "exposure_linear"]

## Interaction with signal
if (BIAS_COVARIATES_AS_INTX){
  for (cov in candidate_covs) df_data[, cov] <- df_data$signal * df_tmp[, cov]
}else{
  for (cov in candidate_covs) df_data[, cov] <-  df_tmp[, cov]
}

df_data$exposure_linear <- df_data$signal
mrdata <- MRData()

mrdata$load_df(
  data = df_data, 
  col_obs = c('obs'),
  col_obs_se = c('obs_se'), 
  col_study_id = c('study_id'), 
  col_covs = as.list(cov_names)
)

loglinear_model <- readRDS(paste0(out_dir, "FILEPATH", ro_pair, ".RDS"))

## Beta prior from first loglinear model results.
beta_gprior_std <- loglinear_model$beta_std
covfinder <- do.call(
  CovFinder,
  c(COV_FINDER_CONFIG, 
    list(
      data = mrdata, 
      covs = as.list(candidate_covs)),
    beta_gprior_std = BETA_PRIOR_MULTIPLIER * beta_gprior_std
  )
)

covfinder$select_covs(verbose = TRUE)

selected_covs <- covfinder$selected_covs
print(selected_covs)

## Save data and selected covariates
out <- append(data, list(df_cov_selection=df_data, selected_covs=selected_covs))
saveRDS(out, paste0(out_dir, "FILEPATH", ro_pair, ".RDS"))
#}


