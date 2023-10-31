#-------------------Header------------------------------------------------
# SECONHAND SMOKE - GBD 2020
# Purpose: Run signal model
#------------------Set-up--------------------------------------------------

library(dplyr)
library(data.table)
library(ggplot2) 
library(mrbrt001, lib.loc = "FILEPATH")


## Set seed
np <- import("numpy")
np$random$seed(as.integer(1987))


##Arguments
args <- commandArgs(trailingOnly = TRUE)
ro_pair <- args[1]
out_dir <- args[2]
WORK_DIR <- args[3]

setwd(WORK_DIR)
source("./config.R")

#-------------------------------------------------------------------------
## Read in data file 
data <- readRDS(paste0(OUT_DIR, "FILEPATH", ro_pair, ".RDS"))
df <- data$df

## Calculate the weighted mean of age midpoint  
df$age_midpoint <- (df$age_start + df$age_end)/2
df$age_ref <- weighted.mean(df$age_midpoint, 1/df$log_se)
df_age <- df[!is.na(df$age_midpoint),]
df_age$age_ref <- weighted.mean(df_age$age_midpoint, 1/df_age$log_se)
df$age_ref <- unique(df_age$age_ref)

cov_names <- c(REF_EXPOSURE_COLS, ALT_EXPOSURE_COLS)

mrdata <- MRData()

mrdata$load_df(
  data = df, 
  col_obs = OBS_VAR,
  col_obs_se = OBS_SE_VAR, 
  col_study_id = STUDY_ID_VAR, 
  col_covs = as.list(cov_names)
)

monotonicity <- DIRECTION[ro_pair][[1]]

## Signal model config: different spline degress for cvd_ihd
if (ro_pair == "cvd_ihd"){
  ensemble_cov_model <- LogCovModel(
    alt_cov = ALT_EXPOSURE_COLS,
    ref_cov = REF_EXPOSURE_COLS,
    use_spline = TRUE,
    use_re = FALSE,
    spline_degree = 2L,
    spline_knots_type = 'domain',
    spline_r_linear = TRUE,
    prior_spline_funval_uniform = array(c(-1 + 1e-6, 19)),
    prior_spline_num_constraint_points = 150L,
    spline_knots = array(seq(0, 1, length.out = N_I_KNOTS + 2)),
    prior_spline_maxder_gaussian = cbind(rbind(rep(0, N_I_KNOTS),
       rep(sqrt(PRIOR_VAR_MAXDER), N_I_KNOTS)), c(0, sqrt(PRIOR_VAR_RSLOPE))),
    prior_spline_der2val_gaussian = NULL,
    prior_spline_der2val_gaussian_domain = array(c(0.0, 1.0)),
    prior_spline_monotonicity = monotonicity
  )
  
  } else {
    ensemble_cov_model <- LogCovModel(
      alt_cov = ALT_EXPOSURE_COLS,
      ref_cov = REF_EXPOSURE_COLS,
      use_spline = TRUE,
      use_re = FALSE,
      spline_degree = 3L,
      spline_knots_type = 'domain',
      spline_r_linear = TRUE,
      prior_spline_funval_uniform = array(c(-1 + 1e-6, 19)),
      prior_spline_num_constraint_points = 150L,
      spline_knots = array(seq(0, 1, length.out = N_I_KNOTS + 2)),
      prior_spline_maxder_gaussian = cbind(rbind(rep(0, N_I_KNOTS),
         rep(sqrt(PRIOR_VAR_MAXDER), N_I_KNOTS)), c(0, sqrt(PRIOR_VAR_RSLOPE))),
      prior_spline_der2val_gaussian = NULL,
      prior_spline_der2val_gaussian_domain = array(c(0.0, 1.0)),
      prior_spline_monotonicity = monotonicity
    )
    
 }
  
## Create knot samples
knots <- import("mrtool.core.model")
knots_samples <- knots$create_knots_samples(
  data = mrdata,
  l_zero = TRUE,
  num_splines = 50L,
  num_knots = as.integer(N_I_KNOTS + 2), width_pct = 1/(N_I_KNOTS + 2),
  alt_cov_names = ALT_EXPOSURE_COLS,
  ref_cov_names = REF_EXPOSURE_COLS
)


## Fit ensemble model with exposure only 
if (nrow(df) >= 10){
signal_model <- MRBeRT(mrdata,
                       ensemble_cov_model=ensemble_cov_model,
                       ensemble_knots=knots_samples,
                       inlier_pct=0.9)

} else {
signal_model <- MRBeRT(mrdata,
                       ensemble_cov_model=ensemble_cov_model,
                       ensemble_knots=knots_samples,
                       inlier_pct=1)
}

signal_model$fit_model(inner_print_level=5L, inner_max_iter=200L, 
                       outer_step_size=200L, outer_max_iter=100L)
print(signal_model$ensemble_knots)

df_data <-  mrdata$to_df()


## Extract weights of data point
w <- t(do.call(rbind, 
               lapply(1:length(signal_model$sub_models), 
                      function(i){signal_model$sub_models[[i]]$w_soln}))
) %*% signal_model$weights


## Assign signal to data for use in later stages
signal <- signal_model$predict(mrdata, predict_for_study=FALSE)
df_data$signal <-  signal

## Add age_ref to the dataset
df_data$age_ref <- unique(df$age_ref)


# Drop data trimmed
df_data <-  df_data[w >= 0.1,]

#save_data$w <- w
#write.csv(save_data, paste0(out_dir, "01_signal_models/", ro_pair,".csv"), row.names=FALSE)

# Save model and visualize curve
py_save_object(object = signal_model, 
               filename = paste0(out_dir, "FILEPATH", ro_pair, ".pkl"), 
               pickle = "dill")

out <- append(data, list(df_data=df_data))
saveRDS(out, paste0(out_dir, "FILEPATH", ro_pair, ".RDS"))

