## ******************************************************************************
##
## Purpose: Model relationship of HAQi on proportion of kernicterus
##
## ******************************************************************************

rm(list = ls())

os <- .Platform$OS.type
if (os == "windows") {
  j <- "J:/"
  h <- "H:/"
} else {
  j <- "FILEPATH/j/"
  h <- paste0("FILEPATH", Sys.getenv("USER"),"/")
}

library('openxlsx')
library('data.table')
library(ggplot2)
source("FILEPATH/get_covariate_estimates.R")

fp <- paste0(j, 'FILEPATH/kernicterus_risk_best_version.xlsx')
raw_dt <- as.data.table(read.xlsx(fp, sheet = 1, na.strings = "NA"))

dt <- raw_dt[group_review == 1 | is.na(group_review)]
dt <- dt[!is.na(mean)]

dt$mean <- as.numeric(dt$mean)
dt$sample_size <- as.numeric(dt$sample_size)
dt$location_id <- as.numeric(dt$location_id)
dt$year_start <- as.numeric(dt$year_start)
dt$year_end <- as.numeric(dt$year_end)
dt[, year_id := ceiling((year_start + year_end)/2)]

dt[sample_name == "extreme hyperbilirubinemia", from_jaundice := 0]
dt[sample_name == "jaundice", from_jaundice := 1]

dt[sex == 'Male', sex_id := 1]
dt[sex == 'Female', sex_id := 2]
dt[sex == 'Both', sex_id := 3]

setnames(dt, 'sample.TSB.threshold.(mg/dL)', 'TSB')


#load haqi
haqi <- get_covariate_estimates(covariate_id=1099, gbd_round_id=6, decomp_step='step4')
setnames(haqi, 'mean_value', 'haqi')
dt3 <- merge(dt, haqi, by = c('location_id', 'year_id'), all.x = FALSE)
dt3 <- dt3[!is.na(TSB)]
dt3 <- dt3[sample_name != "extreme hyperbilirubinemia from Rh"]

dt3[haqi < 40, haqi_level := '<40']
dt3[haqi >= 40 & haqi < 55, haqi_level := '40-54']
dt3[haqi >= 55 & haqi < 70, haqi_level := '55-69']
dt3[haqi >= 70, haqi_level := '70+']


#fit a meta-regression using MR-BRT
repo_dir <- "FILEPATH/run_mr_brt/"
source(paste0(repo_dir, "run_mr_brt_function.R"))
source(paste0(repo_dir, "cov_info_function.R"))
source(paste0(repo_dir, "check_for_outputs_function.R"))
source(paste0(repo_dir, "load_mr_brt_outputs_function.R"))
source(paste0(repo_dir, "predict_mr_brt_function.R"))
source(paste0(repo_dir, "check_for_preds_function.R"))
source(paste0(repo_dir, "load_mr_brt_preds_function.R"))
library(metafor, lib.loc = "FILEPATH/R_libraries/")
library(msm, lib.loc = "FILEPATH/R_libraries/")
library(dplyr)

z <- qnorm(0.975)
dt3[(is.na(standard_error) | standard_error == 0), 
    standard_error := (1/(1+z^2/sample_size)) * sqrt(mean*(1-mean)/sample_size + z^2/(4*sample_size^2))]

data <- dt3[case_name == "acute bilirubin encephalopathy", .(mean, standard_error, haqi, TSB, from_jaundice)]

data$standard_error <- as.numeric(data$standard_error)

data[, mean := mean - 0.000001]
data$mean_logit <- log(data$mean / (1-data$mean))
data$se_logit <- sapply(1:nrow(data), function(i) {
  mean_i <- as.numeric(data[i, "mean"])
  se_i <- as.numeric(data[i, "standard_error"])
  deltamethod(~log(x1/(1-x1)), mean_i, se_i^2)
})

setnames(data, c('mean', 'standard_error'), c('mean_linear', 'standard_error_linear'))
setnames(data, c('mean_logit', 'se_logit'), c('mean', 'standard_error'))


fit1 <- run_mr_brt(
  output_dir = paste0('FILEPATH'), # user home directory
  model_label = "ehb_haqi_TSB_maxL_exclude_outliers_trim10_monotonic_3knot_v2",
  data = data,
  mean_var = "mean",
  se_var = "standard_error",
  method = 'trim_maxL',
  overwrite_previous = TRUE,
  trim_pct = 0.10,
  covs = list(cov_info("haqi", "X", degree = 3, n_i_knots = 3, r_linear = TRUE, l_linear = TRUE, bspline_mono = "decreasing"),
              cov_info("TSB", "X"))
)

check_for_outputs(fit1)

#load model object
results <- load_mr_brt_outputs(fit1)
train_data <- data.table(results$train_data)

#make predictions for hemolytic
haqi <- haqi[, .(location_id, location_name, year_id, haqi)]
df_pred <- haqi[, TSB := 25]

#make predictions based on the model
df_pred <- expand.grid(haqi = seq(30,90,0.2), TSB = seq(0,40,1))
pred1 <- predict_mr_brt(fit1, newdata = df_pred)
check_for_preds(pred1)

pred_object <- load_mr_brt_preds(pred1)
preds <- data.table(pred_object$model_summaries)


#transform back into linear space
preds[, Y_mean_linear := (exp(Y_mean)/(1 + exp(Y_mean))) + 0.000001]
preds[, Y_mean_hi_linear := (exp(Y_mean_hi)/(1 + exp(Y_mean_hi))) + 0.000001]
preds[, Y_mean_lo_linear := (exp(Y_mean_lo)/(1 + exp(Y_mean_lo))) + 0.000001]

