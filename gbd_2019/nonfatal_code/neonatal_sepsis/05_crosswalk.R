#############################################################################################################
### Project: Neonatal Sepsis and Other Neonatal Infections
### Purpose: Crosswalk claims and literature data to hospital data
### Inputs: df: dataframe of bundle data to be crosswalked
#############################################################################################################

## Source Helper Functions
source("FILEPATH/merge_on_location_metadata.R")
source("FILEPATH/get_population.R")

## Define Functions

prep_data <- function(df) {
  
  # prep data
  df <- df[measure == 'incidence', 
           c('seq', 'nid', 'location_id', 'year_start', 'year_end', 'age_start', 'age_end','sex','mean', "cv_hospital","cv_marketscan","cv_literature",'standard_error'),
           with = FALSE]
  setnames(df, c('standard_error'),c('se'))
  df[, year_mean := (year_start + year_end)/2]
  df[, age_mean := (age_start + age_end)/2]
  df <- merge_on_location_metadata(df)
  df[, country := location_id]
  df[level > 3, country := parent_id]
  df[, country_name := location_name]
  df[ level > 3, country_name := parent_name]
  # aggregate locations for getting matches
  gbr <- df[ihme_loc_id %like% "GBR" & cv_hospital==1]
  setnames(gbr,"year_mean","year_id")
  pops <- get_population(gbd_round_id=6, location_id=unique(gbr$location_id),year_id=unique(gbr$year_id),age_group_id=c(2,3,4),sex_id=c(1,2),decomp_step='step2')
  pops[,sex := ifelse(sex_id==1,"Male","Female")]
  pops[,age_start := ifelse(age_group_id==2,0.000,ifelse(age_group_id==3,0.01917808,0.07671233))]
  pops <- pops[,c("location_id","year_id","population","sex","age_start")]
  gbr <- merge(gbr,pops,by=c("location_id","year_id","sex","age_start"),all.x=T)
  gbr[,mean := weighted.mean(mean,w=population),by=c("year_id","sex","age_start")]
  gbr[,se:=weighted.mean(se,w=population),by=c("year_id","sex","age_start")]
  gbr <- gbr[!duplicated(gbr[,c("age_start","year_id","sex")])]
  gbr[,location_id := 4749]
  gbr[,ihme_loc_id := "GBR_4749"]
  gbr[,location_name := "England"]
  gbr <- merge_on_location_metadata(gbr)
  gbr[,year_mean := (year_start + year_end)/2]
  gbr[,age_mean := (age_start + age_end)/2]
  # append
  df <- rbind(df,gbr,fill=TRUE)
  # return
  return(df)
}

match_data <- function(df) {
  # match reference and alternative definitions
  alt_marketscan <- df[cv_marketscan == 1]
  alt_literature <- df[cv_literature == 1]
  ref <- df[cv_hospital == 1]
  paired_data_marketscan <- merge(ref, alt_marketscan, by = c('location_id','sex','age_start'), all.x = FALSE, suffixes = c('.ref','.alt'))
  paired_data_literature <- merge(ref, alt_literature, by = c('location_id','sex','age_start'), all.x = FALSE, suffixes = c('.ref','.alt'))
  paired_data <- rbind(paired_data_marketscan, paired_data_literature, fill=TRUE)
  paired_data <- paired_data[abs(year_mean.ref - year_mean.alt) < 6]
  # calculate ratio within matches
  paired_data[, ratio := mean.alt / mean.ref]
  paired_data[, ratio_se := sqrt((mean.alt^2 / mean.ref^2) * (se.alt^2/mean.alt^2 + se.ref^2/mean.ref^2))]
  paired_data[, year_mean := (year_mean.ref + year_mean.alt)/2]
  paired_data[, cv_literature := ifelse(cv_literature.alt==1,1,0)]
  paired_data[, cv_marketscan := ifelse(cv_marketscan.alt==1,1,0)]
  message("Data matched! Saving matches to FILEPATH/bundle_92.csv")
  write.csv(paired_data,"FILEPATH/bundle_92.csv",row.names=F)
          
  return(paired_data)
}

prep_mrbrt <- function(df) {
  df$ratio_log <- log(df$ratio)
  df$ratio_se_log <- sapply(1:nrow(df), function(i) {
    ratio_i <- df[i, "ratio"]
    ratio_se_i <- df[i, "ratio_se"]
    deltamethod(~log(x1), ratio_i, ratio_se_i^2)
  })
  df <- df[!is.na(ratio_log)]
  return(df)
}

fit_mrbrt <- function(df, folder) {
  folder_name <- folder
  
  # Fit model - specify options below
  message(paste0("Fitting MR-BRT model for neonatal sepsis!"))
  fit1 <- run_mr_brt(
    output_dir = paste0('FILEPATH'),
    model_label = folder_name,
    data = df,
    mean_var = "ratio_log",
    se_var = "ratio_se_log",
    method = 'trim_maxL',
    covs = list(cov_info("cv_marketscan","X")),
    overwrite_previous = TRUE,
    study_id = "nid.alt"
  )
  
  # Check for MR-BRT outputs
  message(paste0("Checking for MR-BRT outputs for neonatal sepsis"))
  if (check_for_outputs(fit1)){
    message(paste0("Outputs for me neonatal sepsis present! Moving on to predictions."))
  } else {
    break(paste0("MR-BRT outputs missing for neonatal sepsis!"))
  }
  
  # Predict out model results
  df_pred <- expand.grid(intercept = 1, cv_marketscan=unique(df$cv_marketscan), cv_literature=unique(df$cv_literature))
  pred1 <- predict_mr_brt(fit1, newdata = df_pred)
  
  # Check for MR-BRT predictions
  message(paste0("Checking for MR-BRT predictions for neonatal sepsis!"))
  if (check_for_preds(pred1)){
    message(paste0("Predictions for me neonatal sepsis present! Moving on to final processing."))
  } else {
    break(paste0("MR-BRT predictions missing for neonatal sepsis!"))
  }
  
  # Store predictions
  pred_object <- load_mr_brt_preds(pred1)
  preds <- pred_object$model_summaries
  beta0 <- preds$Y_mean
  beta0_se_tau <- (preds$Y_mean_hi - preds$Y_mean_lo) / 3.92
  message(paste0("Predictions saved for MR-BRT run for me neonatal sepsis!"))
  
  # Plot MR-BRT results
  plot_mr_brt(pred1)
}

apply_mrbrt <- function(df,folder){ 
  data <- copy(df)
  # read in crosswalk value
  preds <- fread(paste0("FILEPATH/model_summaries.csv"))
  beta0_literature <- exp(unique(preds[X_cv_marketscan==0]$Y_mean))
  beta0_marketscan <- exp(unique(preds[X_cv_marketscan==1]$Y_mean))
  beta0_se_literature <- exp((unique(preds[X_cv_marketscan==0]$Y_mean_hi) - unique(preds[X_cv_marketscan==0]$Y_mean_lo)) / 3.92)
  beta0_se_marketscan <- exp((unique(preds[X_cv_marketscan==1]$Y_mean_hi) - unique(preds[X_cv_marketscan==1]$Y_mean_lo)) / 3.92)
  # separate alt/ref
  ref <- data[cv_marketscan!=1 & cv_literature != 1]
  alt_literature <- data[cv_literature==1]
  alt_marketscan <- data[cv_marketscan==1]
  # apply crosswalk
  alt_literature[, orig.mean := mean]
  alt_literature[, mean := mean/beta0_literature]
  alt_literature[, variance := (standard_error^2) * (beta0_se_literature^2)]
  alt_literature[, standard_error := sqrt(variance)]
  alt_marketscan[, orig.mean := mean]
  alt_marketscan[, mean := mean/beta0_marketscan]
  alt_marketscan[, variance := (standard_error^2) * (beta0_se_marketscan^2)]
  alt_marketscan[, standard_error := sqrt(variance)]
  # append to reference
  alt <- rbind(alt_literature,alt_marketscan,fill=TRUE)
  data <- rbind(ref,alt,fill=TRUE)
  data <- data[group_review == 1 | is.na(group_review)]
  data[, seq := ""]
  data[!is.na(upper), uncertainty_type_value := 95]
  # write-out
  message(paste0("Crosswalk ratios applied! Writing out to FILEPATH/bundle_92_",format(lubridate::with_tz(Sys.time(), tzone="America/Los_Angeles"), "%Y-%m-%d"),".xlsx"))
  write.xlsx(data,paste0("FILEPATH/crosswalked_bundle_92_",format(lubridate::with_tz(Sys.time(), tzone="America/Los_Angeles"), "%Y-%m-%d"),".xlsx"),sheetName="extraction") 
  return(data)
}
