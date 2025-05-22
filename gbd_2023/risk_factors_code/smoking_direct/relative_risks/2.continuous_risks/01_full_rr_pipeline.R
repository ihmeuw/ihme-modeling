#---------------------------------------------------
# Purpose: estimating rr curves using mr-bert
# Date: 11/01/2020
#---------------------------------------------------

#-------------------------------------------------------------------------------------------------------------------------------------------------
#
# 01_create_template.R
#
#
rm(list = ls())
library(data.table)
library(ggplot2)
library(dplyr)
library(tidyr)
library(mrbrt001, lib.loc = FILEPATH)
np <- import("numpy")
np$random$seed(as.integer(123))

args <- commandArgs(trailingOnly = TRUE)

if(interactive()){
  # the ro_pair of this script can be age-specific for CVD outcomes
  ro_pair <- "stroke" # cataracts
  out_dir <- FILEPATH
  WORK_DIR <- FILEPATH
  inlier_pct <- 0.9
  setwd(WORK_DIR)
  source(FILEPATH)
} else {
  ro_pair <- args[1]
  out_dir <- args[2]
  WORK_DIR <- args[3]
  inlier_pct <- as.numeric(args[4])
  setwd(WORK_DIR)
  source("./config.R")
}

# read in data
data <- readRDS(paste0(out_dir, "00_prepped_data/", ro_pair, ".RDS"))
df <- data$df

# get 25, 50, 75th quantiles of exposure
quants <- quantile(df$age_midpoint, probs=seq(0,1,0.25), na.rm = T)


# Specify all the columns you need for your application
cov_names <- c(REF_EXPOSURE_COLS, ALT_EXPOSURE_COLS)

mrdata <- MRData()

mrdata$load_df(
  data = df, 
  col_obs = OBS_VAR,
  col_obs_se = OBS_SE_VAR, 
  col_study_id = STUDY_ID_VAR, 
  col_covs = as.list(cov_names)
)

monotonicity <- 'increasing'
if (ro_pair=="parkinson") monotonicity <- 'decreasing'


PRIOR_VAR_RSLOPE = 1e-6
PRIOR_VAR_MAXDER <- 1e-4


# all ro pairs use 2nd degree spline. The shape is simpler, makes more sense and the CI is in general smaller
degree <- 2L

# NOTE: all models need the constrains. 

# here are ROs that need 3 internal knots
pairs_3knots <- c("colon_and_rectum_cancer",
                  "copd",
                  "diabetes",
                  "nasopharyngeal_cancer",
                  "prostate_cancer",
                  "stroke",
                  "tb")

# set internal knots to be 2 or 3 based on ro_pair

N_I_KNOTS <- 3L

ensemble_cov_model <- LogCovModel(
  alt_cov = ALT_EXPOSURE_COLS,
  ref_cov = REF_EXPOSURE_COLS,
  use_spline = TRUE,
  use_re = FALSE,
  spline_degree = degree,
  spline_knots_type = 'domain',
  spline_r_linear = TRUE,
  prior_spline_funval_uniform = array(c(-1 + 1e-6, 19)),
  prior_spline_num_constraint_points = 150L,
  spline_knots = array(seq(0, 1, length.out = N_I_KNOTS + 2)), # if specify the spline knots here, will that overwrite the ensemble knots latter?
  prior_spline_maxder_gaussian = cbind(rbind(rep(0, N_I_KNOTS),
                                             rep(sqrt(PRIOR_VAR_MAXDER), N_I_KNOTS)), c(0, sqrt(PRIOR_VAR_RSLOPE))),
  prior_spline_der2val_gaussian_domain = array(c(0.0, 1.0)),
  prior_spline_monotonicity = monotonicity
)

# Create knot samples
utils <- import("mrtool.core.utils")

# Question: when setting the knots, is the domain [0,1] the range over exposure, e.g. [35, 99], or from 0 to maximum exposure, e.g. [0,99]
# set boundaries for the knots. 
if(N_I_KNOTS==2L){
  knot_bounds = matrix(c(1.5/6, 2.5/6, 3.5/6, 4.5/6), ncol = 2, byrow = TRUE)
} else {
  knot_bounds = matrix(c(0.2, 0.3, 0.45, 0.55, 0.7, 0.8), ncol = 2, byrow = TRUE)
}

knots_samples <- utils$sample_knots(
  num_intervals = N_I_KNOTS+1L,
  knot_bounds = knot_bounds,
  num_samples = 50L
)


# Ensemble model with exposure only 
signal_model <- MRBeRT(mrdata,
                       ensemble_cov_model=ensemble_cov_model,
                       ensemble_knots=knots_samples,
                       inlier_pct=inlier_pct)

signal_model$fit_model(inner_print_level=2L, inner_max_iter=200L, 
                       outer_step_size=200L, outer_max_iter=100L)

# create "new covariates" for later use
signal <- signal_model$predict(mrdata, predict_for_study=FALSE)

# plot the signal to check for shape
df_signal_pred <- data.table(a_0 = 0,
                             a_1 = 0,
                             b_0 = 0:100,
                             b_1 = 0:100)
data_signal_pred <- MRData()
data_signal_pred$load_df(
  data = df_signal_pred,
  col_covs = as.list(names(df_signal_pred))
)

signal_plot <- signal_model$predict(data_signal_pred, predict_for_study=FALSE)
plot(exp(signal_plot), type="l" )

# Extract weights of data point
w <- t(do.call(rbind, 
               lapply(1:length(signal_model$sub_models), 
                      function(i){signal_model$sub_models[[i]]$w_soln}))
) %*% signal_model$weights

df_data <-  mrdata$to_df()
# Assign signal to data for use in later stage
df_data$signal <-  signal

# Drop data trimmed
df_data <-  df_data[w >= 0.1,]

# Save data and model
py_save_object(object = signal_model, 
               filename = paste0(FILEPATH, "01_template_pkl_files/", ro_pair, "_knots_", N_I_KNOTS, ".pkl"), 
               pickle = "dill")

out <- append(data, list(df_data=df_data))
saveRDS(out, paste0(FILEPATH, "01_template_models/", ro_pair, "_knots_", N_I_KNOTS, ".RDS"))

#-------------------------------------------------------------------------------------------------------------------------------------------------
#
# 02_loglinear_models.R
#
#

data <- readRDS(paste0(FILEPATH, "01_template_models/", ro_pair, "_knots_", N_I_KNOTS, ".RDS"))
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
  prior_beta_uniform=array(c(1.0, 1.0)) # fix the coef for the signal to be 1. 
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
               filename = paste0(FILEPATH, "02_loglinear_pkl_files/", ro_pair, "_knots_", N_I_KNOTS, ".pkl"), 
               pickle = "dill")

out <- append(data, list(beta_std = beta_std))
saveRDS(out, paste0(FILEPATH, "02_loglinear_models/", ro_pair, "_knots_", N_I_KNOTS, ".RDS"))

#-------------------------------------------------------------------------------------------------------------------------------------------------
#
# 03_covariate_selection.R
#
#

# Read data
data <- readRDS(paste0(FILEPATH, "02_loglinear_models/", ro_pair, "_knots_", N_I_KNOTS, ".RDS"))
df_data <- data$df_data
df_tmp <- data$df
df_tmp <- df_tmp[as.numeric(rownames(df_data)),] # this line drops the outliered data in the signal model

# indentify the bias covariates that are all 0 and remove it from the cov_name
cv_excl <- names(df_tmp)[sapply(df_tmp, setequal, 0) & grepl('cv_',names(df_tmp))]
cov_names <- c("exposure_linear", data$x_covs[!data$x_covs %in% eval(cv_excl)])

candidate_covs <- cov_names[!cov_names == "exposure_linear"]

# Interaction with signal
if (BIAS_COVARIATES_AS_INTX){
  for (cov in candidate_covs) df_data[, cov] <- df_data$signal * df_tmp[, cov]
}

# Change the name of signal to exposure_linear, since some
# underlying code deal with column name `exposure_linear`
df_data$exposure_linear <- df_data$signal
mrdata <- MRData()

mrdata$load_df(
  data = df_data, 
  col_obs = c('obs'),
  col_obs_se = c('obs_se'), 
  col_study_id = c('study_id'), 
  col_covs = as.list(cov_names)
)

loglinear_model <- readRDS(paste0(FILEPATH, "02_loglinear_models/", ro_pair, "_knots_", N_I_KNOTS, ".RDS"))

# Beta prior from first loglinear model results.
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
selected_covs

# Save data and selected covariates
out <- append(data, list(df_cov_selection=df_data, selected_covs=selected_covs))
saveRDS(out, paste0(FILEPATH, "03_covariate_selection_models/", ro_pair, "_knots_", N_I_KNOTS, ".RDS"))

#-------------------------------------------------------------------------------------------------------------------------------------------------
#
# 04_mixed_effects_models.R
#
#

# Extract selected covariates
data <- readRDS(paste0(FILEPATH, "03_covariate_selection_models/", ro_pair, "_knots_", N_I_KNOTS, ".RDS"))

df_data <- data$df_data
df_tmp <- data$df
# Only keep rows that are not trimmed
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
signal_model <- py_load_object(filename=paste0(FILEPATH, "01_template_pkl_files/", ro_pair, "_knots_", N_I_KNOTS, ".pkl"), 
                               pickle = "dill")
orig_data <- readRDS(paste0(FILEPATH, "01_template_models/", ro_pair, "_knots_", N_I_KNOTS, ".RDS"))
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

# TODO: data of selected covariates to be added

df_final_pred <- data.table(signal=signal_pred)

# add bias covs in the dataset (do not need this?)
df_final_pred[, (bias_covs) := 0]

data_final_pred <- MRData()
data_final_pred$load_df(
  df_final_pred,
  col_covs = as.list(c("signal", bias_covs))
)

# create draws and prediction
set.seed(123)
sampling <- import("mrtool.core.other_sampling")
num_samples <- 1000L
beta_samples <- sampling$sample_simple_lme_beta(num_samples, model)
gamma_samples <- rep(model$gamma_soln, num_samples) * matrix(1, num_samples)


curve <- model$predict(data_final_pred)
draws <- model$create_draws(
  data_final_pred,
  beta_samples=beta_samples,
  gamma_samples=gamma_samples,
  random_study=T
)

# create dataset for predictions
df_pred <- data.table("pred" = curve)
df_pred$pred_lo <- apply(draws, 1, function(x) quantile(x, 0.025))
df_pred$pred_hi <- apply(draws, 1, function(x) quantile(x, 0.975))
df_pred$exposure <- exposure

df_pred[exposure %in% c(0,5,10,15,20,25,30,40,60,80,100), .(exposure, exp(pred), exp(pred_lo),  exp(pred_hi))]

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
               filename = paste0(FILEPATH, "04_mixed_effects_pkl_files/", ro_pair, "_knots_", N_I_KNOTS, ".pkl"), 
               pickle = "dill")

##########################
## read in previous RRs ##
##########################

age <- 15
ro <- ro_pair
sex <- ifelse(ro_pair %in% c("breast_cancer", "cervical_cancer"), 2, 1)

rr_folder <- list.files(FILEPATH)
rr_folder <- rr_folder[grepl(ro, rr_folder)]
# read in exposure def file
exp_def <- fread(FILEPATH)
pair_cig <- exp_def[current_exp_def=="amt", cause]
if(ro %in% pair_cig){
  expo_unit <- "Cig/Day"
  RR <- fread(paste0(FILEPATH,rr_folder, "/draws_cig.csv"))
} else{
  expo_unit <- "Pack-Year"
  RR <- fread(paste0(FILEPATH,rr_folder, "/draws_pack.csv"))
}

rr <- RR[,mean(rr), by=c("exposure", "sex_id","age_group_id")]
setnames(rr, "V1", "rr")

quan_hi <- function(x) quantile(x, 0.975)
quan_lo <- function(x) quantile(x, 0.025)

rr_hi <- RR[,quan_hi(rr), by=c("exposure", "sex_id", "age_group_id")]
rr_lo <- RR[,quan_lo(rr), by=c("exposure", "sex_id", "age_group_id")] 
setnames(rr_hi, "V1", "rr_hi")
setnames(rr_lo, "V1", "rr_lo")

rr_old <- merge(rr, rr_hi, by=c("exposure", "sex_id", "age_group_id"))
rr_old <- merge(rr_old, rr_lo, by=c("exposure", "sex_id", "age_group_id"))


if(ro_pair == "parkinson"){
  mono_tmp <- "decreasing"
} else {
  mono_tmp <- "increasing"
}

df <- data.table(df)

