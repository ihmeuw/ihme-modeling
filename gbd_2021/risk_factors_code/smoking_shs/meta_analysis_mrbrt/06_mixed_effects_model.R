#-------------------Header------------------------------------------------
# SECONHAND SMOKE - GBD 2020
# Purpose: Run mixed effects model
#------------------Set-up--------------------------------------------------

library(dplyr)
library(data.table)
library(ggplot2)
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

# Extract selected covariates
data <- readRDS(paste0(out_dir, "FILEPATH", ro_pair, ".RDS"))
df_data <- data$df_data
df_tmp <- data$df

# Only keep rows that are not trimmed
df_tmp <- df_tmp[as.numeric(rownames(df_data)),]

# Check selected covs
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
cov_models <- list()
for (cov in bias_covs) cov_models <- append(cov_models, 
                                            list(
                                              do.call(
                                                LinearCovModel, 
                                                list(
                                                  alt_cov=cov,
                                                  use_re = F,
                                                  prior_beta_gaussian=array(c(0, BETA_PRIOR_MULTIPLIER * data$beta_std)) #NEW: REDUCE THE EFFECTS OF BIAS COVARIATES
                                                  
                                                )
                                              )
                                            )
)

# Mixed effects model
cov_models <- append(cov_models, 
                     LinearCovModel(
                       alt_cov = 'signal', 
                       #use_re = FALSE, # change to False if using linear random effect
                       use_re = TRUE,
                       prior_beta_uniform = array(c(1.0, 1.0))))

model <- MRBRT(
  data=mrdata,
  cov_models = cov_models,
  inlier_pct = 1.0
)

model$fit_model(inner_print_level=5L, inner_max_iter=200L, 
                outer_step_size=200L, outer_max_iter=100L)

# Save model
py_save_object(object = model, 
               filename = paste0(out_dir, "FILEPATH", ro_pair, ".pkl"), 
               pickle = "dill")


# --------------------------------
# Create predictions
# --------------------------------

## Load signal model and data in Stage 1
signal_model <- py_load_object(filename=paste0(out_dir, "01_signal_pkl_files/", ro_pair, ".pkl"), 
                               pickle = "dill")

orig_data <- readRDS(paste0(out_dir, "01_signal_models/", ro_pair, ".RDS"))
df <- orig_data$df

# Distribution of exposure (PM2.5) across all draws and years
median <- 28.18601
per_10 <- 16.49607
per_90 <- 40.62846

NUM_POINTS <- 1000L
exposure_lower <- min(df[,c(REF_EXPOSURE_COLS, ALT_EXPOSURE_COLS)])
exposure_upper <- 180
exposure <- seq(exposure_lower, exposure_upper, length.out=NUM_POINTS)

# Set up prediction data frame
min_cov <- rep(exposure_lower, NUM_POINTS)

# Deal with Sarah's data
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


df_signal_pred$signal <- signal_model$predict(data_signal_pred)

df_final_pred <- as.data.table(df_signal_pred)

for(var in bias_covs){
  print(var)
  df_final_pred[, paste0(var):= 0]
}


data_final_pred <- MRData()
data_final_pred$load_df(
  df_final_pred,
  col_covs = as.list(c("signal", bias_covs))
)


# --------------------------------
# Create and save draws
# --------------------------------

# create draws and prediction
sampling <- import("mrtool.core.other_sampling")
num_samples <- 1000L
beta_samples <- sampling$sample_simple_lme_beta(num_samples, model)
gamma_samples <- rep(model$gamma_soln, num_samples) * matrix(1, num_samples)

## Draws with gamma (between study heterogeneity)
y_draws = as.data.table(model$create_draws(data_final_pred, beta_samples, gamma_samples, random_study=T))
setnames(y_draws, colnames(y_draws), paste0("draw_",0:999))
y_draws_fe = as.data.table(model$create_draws(data_final_pred, beta_samples, gamma_samples, random_study=F))
setnames(y_draws_fe, colnames(y_draws_fe), paste0("draw_",0:999))

# --------------------------------
# Create and save summary
# --------------------------------

## Summarize for quick plotting (exponentiated)
df_final_pred$mean_predict <- exp(model$predict(data_final_pred))
df_final_pred$mean_fe <- exp(apply(y_draws_fe, 1, function(x) mean(x)))
df_final_pred$lo_fe <- exp(apply(y_draws_fe, 1, function(x) quantile(x, 0.025)))
df_final_pred$hi_fe <- exp(apply(y_draws_fe, 1, function(x) quantile(x, 0.975)))

df_final_pred$mean <- exp(apply(y_draws, 1, function(x) mean(x)))
df_final_pred$lo <- exp(apply(y_draws, 1, function(x) quantile(x, 0.025)))
df_final_pred$hi <- exp(apply(y_draws, 1, function(x) quantile(x, 0.975)))

df_final_pred[, ro_pair := ro_pair]

## Save summary 
dir.create(paste0(out_dir,"FILEPATH"),recursive = T)
dir.create(paste0(out_dir,"FILEPATH"),recursive = T)
write.csv(df_final_pred,paste0(out_dir,"FILEPATH",ro_pair,"_summary.csv"),row.names = F)
print("summary saved!")

# --------------------------------
# Save draws
# --------------------------------

## Save draws
colnames(y_draws) <- paste0("draw_", 0:(num_samples-1))
y_draws <- cbind(data.table("exposure" = exposure), as.data.table(y_draws))

colnames(y_draws_fe) <- paste0("draw_", 0:(num_samples-1))
y_draws_fe <- cbind(data.table("exposure" = exposure), as.data.table(y_draws_fe))

write.csv(y_draws_fe, paste0(out_dir,"FILEPATH", ro_pair, "_y_draws_fe.csv"), row.names = F)
write.csv(y_draws, paste0(out_dir,"FILEPATH", ro_pair, "_y_draws.csv"), row.names = F)
print("draws saved!")





