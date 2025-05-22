
rm(list=ls())
pacman::p_load(data.table)
os <- .Platform$OS.type
if (os == "windows") {
  j <- "PATHNAME"
  h <- "PATHNAME"
} else {
  j <- "PATHNAME"
  user <- Sys.info()[["user"]]
  h <- "PATHNAME"
}
## Source MR-BRT Functions
repo_dir <- "PATHNAME"
source(paste0(repo_dir, "run_mr_brt_function.R"))
source(paste0(repo_dir, "cov_info_function.R"))
source(paste0(repo_dir, "check_for_outputs_function.R"))
source(paste0(repo_dir, "load_mr_brt_outputs_function.R"))
source(paste0(repo_dir, "predict_mr_brt_function.R"))
source(paste0(repo_dir, "check_for_preds_function.R"))
source(paste0(repo_dir, "load_mr_brt_preds_function.R"))
source(paste0(repo_dir, "plot_mr_brt_function.R"))
source("PATHNAME/plot_mr_brt_custom.R")


fit_mrbrt <- function(df, me, folder, trim) {
  folder_name <- folder
  
  # Fit model - specify options below
  message(paste0("Fitting MR-BRT model for me ",me))
  fit1 <- run_mr_brt(
    output_dir = paste0("PATHNAME",me), # user directory
    model_label = folder_name,
    data = df,
    mean_var = "mean_logit",
    se_var = "se_logit",
    method = 'trim_maxL',
    trim_pct = trim,
    covs = list(cov_info("age_group_id","X"),
                cov_info("female","X"),
                cov_info(cv,"X", degree=3,
                         #knot_placement_procedure = 'frequency', n_i_knots = 2,
                         i_knots = "60,80",
                         bspline_mono="decreasing"
                ),
                cov_info("age_group_id","Z"),
                cov_info("female","Z"),
                cov_info(cv,"Z")),
    overwrite_previous = TRUE,
    max_iter=500
  )
  
  # Check for MR-BRT outputs
  message(paste0("Checking for MR-BRT outputs for me ",me))
  if (check_for_outputs(fit1)){
    message(paste0("Outputs for me ",me," present. Moving on to predictions."))
  } else {
    break(paste0("MR-BRT outputs missing for me ",me,"."))
  }
  
  # Predict out model results (may need to edit below if you change model parameters/inputs)
  df_pred <- expand.grid(intercept = 1, age_group_id=c(164,2,3), female=c(0,1), haqi=seq(15,95,by=1))
  #haqi=seq(15,95,by=1)
  #ifd=seq(0,1,by=0.01)
  pred1 <- predict_mr_brt(fit1, newdata = df_pred)
  
  # Check for MR-BRT predictions
  message(paste0("Checking for MR-BRT predictions for me ",me))
  if (check_for_preds(pred1)){
    message(paste0("Predictions for me ",me," present. Moving on to final processing."))
  } else {
    break(paste0("MR-BRT predictions missing for me ",me,"."))
  }
  
  # Store predictions
  pred_object <- load_mr_brt_preds(pred1)
  preds <- pred_object$model_summaries
  beta0 <- preds$Y_mean
  beta0_se_tau <- (preds$Y_mean_hi - preds$Y_mean_lo) / 3.92
  message(paste0("Predictions saved for MR-BRT run for me ",me,"."))
  
  return(pred1)
}

#RUN
args <- commandArgs(trailingOnly = TRUE)
cv <- args[1]
trim_percent <- args[2]
bundle <- 338 

df <- fread("PATHNAME")

pred1 <- fit_mrbrt(df,me="enceph",folder="PATHNAME", trim=as.numeric(trim_percent)/100)

plot_mr_brt_custom(pred1, dose_vars=cv, bundle=bundle, cv=cv)