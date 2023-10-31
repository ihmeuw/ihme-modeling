
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
source(paste0("PATHNAME/plot_mr_brt_custom.R"))


fit_mrbrt <- function(df, me, folder, trim) {
  folder_name <- folder
  
  # Fit model - specify options below
  message(paste0("Fitting MR-BRT model for me ",me))
  fit1 <- run_mr_brt(
    output_dir = paste0(outdir,'/'), 
    model_label = folder_name,
    data = df,
    mean_var = "mean_log",
    se_var = "se_log",
    method = 'trim_maxL',
    trim_pct = trim,
    covs = list(cov_info("early_neonatal","X"),
                cov_info("male","X"),
                cov_info("haqi","X",
                         degree=2,n_i_knots = 2,
                         bspline_mono="decreasing"
                ),
                cov_info("early_neonatal","Z"),
                cov_info("male","Z"),
                cov_info("haqi","Z")),
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
  
  #saveRDS(fit1, file=paste0(outdir,'/',folder_name,'fit1.RData'))
  
  # Predict out model results (may need to edit below if you change model parameters/inputs)
  df_pred <- fread(file = paste0(outdir, "/gbd2020_mrbrt_predict_template.csv"))
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
outdir <- args[3]
bundle <- 337

df <- fread(paste0(outdir,"/gbd2020_mrbrt_data_with_outliers.csv"))

pred1 <- fit_mrbrt(df,me="enceph",folder=paste0("PATHNAME"),
                   trim=as.numeric(trim_percent)/100)

plot_mr_brt_custom(pred1, dose_vars="haqi", bundle = 337, cv='haqi')
