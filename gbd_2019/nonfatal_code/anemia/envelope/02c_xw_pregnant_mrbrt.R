########################################################################
### Project: Anemia 
### Purpose: Prep data and run MR-BRT for hemoglobin pregnancy crosswalk
########################################################################

## Source General Functions
source("FILEPATH/merge_on_location_metadata.R")
source("FILEPATH/get_covariate_estimates.R")

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
  # read in data
  df <- df[sex == "Female" & age_start > 10 & age_end < 50 & sample_size >= 10]
  # prep data
  df <- df[measure == 'continuous', 
                   c('seq', 'nid', 'location_id', 'year_start', 'year_end', 'age_start', 'age_end','sex','val', "cv_pregnant",'standard_error'),
                   with = FALSE]
  setnames(df, c('standard_error','val'),c('se','mean'))
  df[, year_mean := (year_start + year_end)/2]
  df[, age_mean := (age_start + age_end)/2]
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
  alt <- df[cv_pregnant == 1]
  ref <- df[cv_pregnant == 0]
  paired_data <- merge(ref, alt, by = c('location_id','sex','age_start', 'nid'), all.x = FALSE, suffixes = c('.ref', '.alt'))
  paired_data <- paired_data[abs(year_mean.ref - year_mean.alt) < 6]
  # calculate ratio within matches
  paired_data[, ratio := mean.alt / mean.ref]
  paired_data[, ratio_se := sqrt((mean.alt^2 / mean.ref^2) * (se.alt^2/mean.alt^2 + se.ref^2/mean.ref^2))]
  paired_data[, year_mean := (year_mean.ref + year_mean.alt)/2]
 
  return(paired_data)
}

save_matches <- function(df,me) {
  write.csv(df[,.(seq.ref, seq.alt, location_id, sex, ratio, ratio_se, year_mean)],
            file=paste0("FILEPATH"),
            row.names=F)
}

prep_mrbrt <- function(df) {
  df$ratio_log <- log(df$ratio)
  df$ratio_se_log <- sapply(1:nrow(df), function(i) {
    ratio_i <- df[i, "ratio"]
    ratio_se_i <- df[i, "ratio_se"]
    deltamethod(~log(x1), ratio_i, ratio_se_i^2)
  })
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
    mean_var = "ratio_log",
    se_var = "ratio_se_log",
    method = 'trim_maxL',
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
  df_pred <- expand.grid(intercept = 1)
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
}

apply_mrbrt <- function(df,me,folder){ 
  data <- copy(df)
  # read in crosswalk value
  preds <- fread(paste0("FILEPATH/model_summaries.csv"))
  beta0 <- exp(preds$Y_mean)
  beta0_se <- exp((preds$Y_mean_hi - preds$Y_mean_lo) / 3.92)
  # separate cv_pregnant = 0 and 1
  ref <- data[cv_pregnant==0]
  alt <- data[cv_pregnant==1]
  # subset cv_pregnant = 0 by nid
  nids <- ref$nid
  # pull cv_pregnant = 1 not equal to the above
  alt <- alt[!(nid %in% nids)]
  # crosswalk the remaining
  alt[, orig.val := val]
  alt[, val := val/beta0]
  alt[, variance := variance * (beta0_se^2)]
  alt[, standard_error := sqrt(variance)]
  # append to cv_pregnant = 0
  data <- rbind(ref,alt,fill=TRUE)
  data <- data[group_review == 1 | is.na(group_review)]
  data[, seq := ""]
  data[!is.na(upper), uncertainty_type_value := 95]
  # write-out
  message(paste0("Crosswalk ratios applied! Writing out to FILEPATH"))
  write.csv(data,paste0("FILEPATH"),row.names=F) 
  return(data)
  }