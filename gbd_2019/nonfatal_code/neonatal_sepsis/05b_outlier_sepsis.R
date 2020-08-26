###########################################################
### Project: Neonatal Sepsis & Other Neonatal Infections
### Purpose: Investigate MR-BRT Outliering
###########################################################

###################
### Setting up ####
###################
rm(list=ls())
pacman::p_load(data.table, dplyr, ggplot2, stats, boot, openxlsx)

os <- .Platform$OS.type

## Source General Functions
source("FILEPATH/merge_on_location_metadata.R")
source("FILEPATH/get_covariate_estimates.R")
source("FILEPATH/get_crosswalk_version.R")
source("FILEPATH/save_bulk_outlier.R")

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
prep_data <- function(crosswalk_version_id) {
  # read in data
  df <- get_crosswalk_version(crosswalk_version_id)
  df <- df[!nid==336847]
  # prep data
  df <- df[measure == 'incidence', 
           c('seq', 'nid', 'location_id', 'year_start', 'year_end', 'age_start', 'age_end','sex','mean','standard_error'),
           with = FALSE]
  setnames(df, 'standard_error','se')
  df[, mean := mean + 0.0001]
  df[, year_id := floor((year_start + year_end)/2)]
  df <- merge_on_location_metadata(df)
  df[, country := location_id]
  df[level > 3, country := parent_id]
  df[, country_name := location_name]
  df[ level > 3, country_name := parent_name]
  df[, sex_id := ifelse(sex=="Male",1,2)]
  df[, age_group_id := ifelse(age_start==0,2,3)]
  df[, lnn := ifelse(age_group_id==3,1,0)]
  # add HAQi
  haqi <- get_covariate_estimates(1099, decomp_step='step4')[,.(location_id,year_id,mean_value)]
  setnames(haqi,"mean_value","haqi")
  # merge
  df <- merge(df,haqi,by=c("location_id","year_id"),all.x=TRUE)
  # return
  return(df)
}

prep_mrbrt <- function(df) {
  df$mean_log <- log(df$mean)
  df$se_log <- sapply(1:nrow(df), function(i) {
    mean_i <- df[i, "mean"]
    se_i <- df[i, "se"]
    deltamethod(~log(x1), mean_i, se_i^2)
  })
  return(df)
}

fit_mrbrt <- function(df, me, folder, trim) {
  folder_name <- folder
  
  # Fit model - specify options below
  message(paste0("Fitting MR-BRT model for me ",me))
  fit1 <- run_mr_brt(
    output_dir = paste0('FILEPATH',me),
    model_label = folder_name,
    data = df,
    mean_var = "mean_log",
    se_var = "se_log",
    method = 'trim_maxL',
    trim_pct = trim,
    covs = list(cov_info("lnn","X"),
                cov_info("haqi","X", degree=3,
                         i_knots="60,80,95",
                         bspline_mono="decreasing"),
                cov_info("lnn","Z"),
                cov_info("haqi","Z")),
    overwrite_previous = TRUE,
    max_iter=500
  )
  
  # Check for MR-BRT outputs
  message(paste0("Checking for MR-BRT outputs for me ",me))
  if (check_for_outputs(fit1)){
    message(paste0("Outputs for me ",me," present! Moving on to predictions."))
  } else {
    break(paste0("MR-BRT outputs missing for me ",me,"!"))
  }
  
  # Predict out model results 
  df_pred <- expand.grid(intercept = 1, lnn=c(0,1), haqi=seq(25,100,by=1))
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
  plot_mr_brt(pred1, dose_vars="haqi")
}

outlier_sepsis <- function(crosswalk_version_id, mrbrt_path, outpath){
  
  # pull MR-BRT data weights
  dt <- fread(paste0(mrbrt_path,"/train_data.csv"))[,.(age_start,mean,location_id,nid,ihme_loc_id,seq,w)]
  dt[,is_outlier := ifelse(w==0,1,0)]
  dt[nid==336847, is_outlier := 1]
  dt[ihme_loc_id %like% "MEX", is_outlier := 1]
  dt[ihme_loc_id %like% "GBR", is_outlier := 1]
  dt[ihme_loc_id %like% "JPN", is_outlier := 1]
  dt[ihme_loc_id %like% "ITA", is_outlier := 1]
  dt[ihme_loc_id %like% "NOR", is_outlier := 1]
  
  income <- get_location_metadata(26, gbd_round_id=5)
  ids <- income[region_name=="World Bank High Income"]$ihme_loc_id
  dt[location_id %in% c(4856,43923,43887,4862,43929,43893),is_outlier:=1]
  dt[age_start==0 & mean > 5.2, is_outlier := 1]
  dt[age_start==0.01917808 & mean > 1.7, is_outlier := 1]
  dt[, loc_id := substr(ihme_loc_id,1,3)]
  dt[age_start==0 & loc_id %in% ids & mean > 0.52, is_outlier := 1]
  dt[age_start==0.01917808 & loc_id %in% ids & mean >0.17, is_outlier := 1]
  
  dt <- dt[,.(seq,is_outlier)]
  
  # save dataset
  write.xlsx(dt,outpath,sheetName="extraction")
  
  # run save bulk outlier function
  save <- save_bulk_outlier(crosswalk_version_id,"step4",outpath, description="MR-BRT outliers + MEX/GBR/JPN/ITA/NOR + NIC EMR")
  message(paste0("Success! Your new crosswalk_version_id with outliers is ",save$crosswalk_version_id))
}


## RUN
df <- prep_data(9953)
df <- prep_mrbrt(df)
fit_mrbrt(df,"neonatal_sepsis","sepsis_outlier_haqi",0.5)
