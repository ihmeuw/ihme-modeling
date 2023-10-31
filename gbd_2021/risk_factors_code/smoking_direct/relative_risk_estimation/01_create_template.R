#
# 01_create_template.R
#
#
rm(list = ls())
library(dplyr)
library(mrbrt001, lib.loc = "FILEPATH")
np <- import("numpy")
np$random$seed(as.integer(123))

args <- commandArgs(trailingOnly = TRUE)

if(interactive()){
  # the ro_pair of this script can be age-specific for CVD outcomes
  ro_pair <- "ihd"
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
# diet example
data <- readRDS(paste0(out_dir, "00_prepped_data/", ro_pair, ".RDS"))
df <- data$df
df$age_midpoint <- (df$age_start + df$age_end)/2

# calculate the weighted mean of age midpoint as 
df$age_ref <- weighted.mean(df$age_midpoint, 1/df$ln_se)

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

N_I_KNOTS <- 3
PRIOR_VAR_RSLOPE = 1e-6
PRIOR_VAR_MAXDER <- 1e-4

# if there is a wiggle in the curve, change the degree to 2
pair_3d <- c("rheumatoid_arthritis")

if(ro_pair %in% pair_3d){
  degree <- 3L
} else {
  degree <- 2L
}

# for these ro pairs, use constrains on the first and second order derivatives.
pairs_constr <- c("alzheimer_other_dementia", "bladder_cancer",
                  "cataracts",
                  "cervical_cancer",
                  "leukemia",
                  "macular_degeneration",
                  "multiple_sclerosis",
                  "peripheral_artery_disease",
                  "rheumatoid_arthritis",
                  "stomach_cancer",
                  "parkinson")

if(ro_pair %in% pairs_constr){
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
    spline_knots = array(seq(0, 1, length.out = N_I_KNOTS + 2)),
    prior_spline_maxder_gaussian = cbind(rbind(rep(0, N_I_KNOTS), 
                                               rep(sqrt(PRIOR_VAR_MAXDER), N_I_KNOTS)), c(0, sqrt(PRIOR_VAR_RSLOPE))),
    prior_spline_der2val_gaussian_domain = array(c(0.0, 1.0)),
    prior_spline_monotonicity = monotonicity
  )
} else {
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
    spline_knots = array(seq(0, 1, length.out = N_I_KNOTS + 2)),
    prior_spline_monotonicity = monotonicity
  )
}

utils <- import("mrtool.core.utils")
knots_samples <- utils$sample_knots(
  num_intervals = 4L,
  knot_bounds = matrix(c(0.2, 0.3, 0.45, 0.55, 0.7, 0.8), ncol = 2, byrow = TRUE),
  num_samples = 50L
)

# Ensemble model with exposure only 
signal_model <- MRBeRT(mrdata,
                       ensemble_cov_model=ensemble_cov_model,
                       ensemble_knots=knots_samples,
                       inlier_pct=0.9)

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

# add age_ref to the dataset
df_data$age_ref <- weighted.mean(df$age_midpoint, 1/df$ln_se)

# Drop data trimmed
df_data <-  df_data[w >= 0.1,]

# Save data and model
py_save_object(object = signal_model, 
               filename = paste0(out_dir, "01_template_pkl_files/", ro_pair, ".pkl"), 
               pickle = "dill")

out <- append(data, list(df_data=df_data))
saveRDS(out, paste0(out_dir, "01_template_models/", ro_pair, ".RDS"))
