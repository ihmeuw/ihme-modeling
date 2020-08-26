###########################################################
### Project: RF: Suboptimal Breastfeeding
### Purpose: Conduct crosswalk between ABF6-11 and ABF12-23
###########################################################

###################
### Setting up ####
###################
pacman::p_load(data.table, dplyr, ggplot2, stats, boot)

os <- .Platform$OS.type
if (os == "windows") {
  j <- "FILEPATH"
  h <- "FILEPATH"
} else {
  j <- "FILEPATH"
  user <- Sys.info()[["user"]]
  h <- paste0("FILEPATH", user)
}

## Source General Functions
source("FILEPATH/merge_on_location_metadata.R")
source("FILEPATH/get_covariate_estimates.R")
source("FILEPATH/get_bundle_data.R")

## Source MR-BRT Functions
repo_dir <- "FILEPATH"
source(paste0(repo_dir, "run_mr_brt_function.R"))
source(paste0(repo_dir, "cov_info_function.R"))
source(paste0(repo_dir, "check_for_outputs_function.R"))
source(paste0(repo_dir, "load_mr_brt_outputs_function.R"))
source(paste0(repo_dir, "predict_mr_brt_function.R"))
source(paste0(repo_dir, "check_for_preds_function.R"))
source(paste0(repo_dir, "load_mr_brt_preds_function.R"))
source(paste0(repo_dir, "plot_mr_brt_function.R"))

## Define Functions

prep_data <- function(df) {
  # prep data
  df[, standard_error := sqrt((val*(1-val))/sample_size)]
  df <- df[val != 0]
  df <- df[measure == 'proportion', 
           c('seq', 'nid', 'location_id', 'year_start', 'year_end', 'sex','val', "group",'standard_error'),
           with = FALSE]
  setnames(df, c('standard_error','val'),c('se','mean'))
  df[, year_mean := (year_start + year_end)/2]
  df <- merge_on_location_metadata(df)
  df[, country := location_id]
  df[level > 3, country := parent_id]
  df[, country_name := location_name]
  df[ level > 3, country_name := parent_name]
  # return
  return(df)
}

match_data <- function(df) {
  # match reference and alternative definitions
  alt <- df[group == "abf611"]
  ref <- df[group == "abf1223"]
  paired_data <- merge(ref, alt, by = c('location_id','sex','nid', 'year_start'), all.x = FALSE, suffixes = c('.ref', '.alt'))
  paired_data <- paired_data[abs(year_mean.ref - year_mean.alt) < 6]
  # calculate ratio within matches
  paired_data[, ratio := mean.alt / mean.ref]
  paired_data[, ratio_se := sqrt((mean.alt^2 / mean.ref^2) * (se.alt^2/mean.alt^2 + se.ref^2/mean.ref^2))]
  paired_data[, year_mean := (year_mean.ref + year_mean.alt)/2]
  
  paired_data <- paired_data[ratio_se > .00001 & ratio_se < 10]
  paired_data <- paired_data[!(mean.ref==1 | mean.alt==1)]
  return(paired_data)
}

save_matches <- function(df) {
  write.csv(df[,.(seq.ref, seq.alt, location_id, sex, ratio, ratio_se, year_mean)],
            file=paste0("FILEPATH"),
            row.names=F)
}

prep_mrbrt <- function(df) {
  df$logit_prev_alt <- logit(df$mean.alt)
  df$logit_se_alt <- sapply(1:nrow(df), function(i){
    mean_i <- df[i, mean.alt]
    se_i <- df[i, se.alt]
    deltamethod(~log(x1/(1-x1)), mean_i, se_i^2)
  })
  df$logit_prev_ref <- logit(df$mean.ref)
  df$logit_se_ref <- sapply(1:nrow(df), function(i){
    mean_i <- df[i, mean.ref]
    se_i <- df[i, se.ref]
    deltamethod(~log(x1/(1-x1)), mean_i, se_i^2)
  })
  
  df[, logit_diff := logit_prev_alt - logit_prev_ref]
  df[, se_logit_diff := sqrt(logit_se_alt^2 + logit_se_ref^2)]
  return(df)
}

fit_mrbrt <- function(df, me, folder) {
  folder_name <- folder
  
  # Fit model - specify options below
  message(paste0("Fitting MR-BRT model for me ",me))
  fit1 <- run_mr_brt(
    output_dir = paste0('FILEPATH',me), 
    model_label = folder_name,
    data = df,
    mean_var = "logit_diff",
    se_var = "se_logit_diff",
    method = 'trim_maxL',
    max_iter = 5000,
    #study_id = "nid",
    overwrite_previous = TRUE
  )
  
  # Check for MR-BRT outputs
  message(paste0("Checking for MR-BRT outputs for me ",me))
  if (check_for_outputs(fit1)){
    message(paste0("Outputs for me ",me," present! Moving on to predictions."))
  } else {
    break(paste0("MR-BRT outputs missing for me ",me,"!"))
  }
  
  # Predict out model results 
  df_pred <- data.frame(intercept=1)
  pred1 <- predict_mr_brt(fit1, newdata = df_pred)
  
  # Check for MR-BRT predictions
  message(paste0("Checking for MR-BRT predictions for me ",me))
  if (check_for_preds(pred1)){
    message(paste0("Predictions for me ",me," present! Moving on to final processing."))
  } else {
    break(paste0("MR-BRT predictions missing for me ",me,"!"))
  }
  
  # Store predictions
  pred_object <- load_mr_brt_preds(pred1)
  preds <- pred_object$model_summaries
  beta0 <- preds$Y_mean
  beta0_se_tau <- (preds$Y_mean_hi - preds$Y_mean_lo) / 3.92
  message(paste0("Predictions saved for MR-BRT run for me ",me,"!"))
  
  # Plot MR-BRT results
  plot_mr_brt(pred1)
  
  return(fit1)
}

