#############################################################################################################
### Project: Neonatal Sepsis and Other Neonatal Infections
### Purpose: Crosswalk claims and literature data to hospital data
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
  pops <- get_population(gbd_round_id=7, location_id=unique(gbr$location_id),year_id=unique(gbr$year_id),age_group_id=c(2,3,4),sex_id=c(1,2),decomp_step='iterative')
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
  paired_data[,refvar:="hospital"]
  paired_data[cv_marketscan.alt==1,altvar:="claims"]
  paired_data[cv_literature.alt==1,altvar:="lit"]
  paired_data[mean.ref==0,mean.ref:=.0000000000001]
  # calculate logit_diff
  dat_diff <- as.data.frame(cbind(
    delta_transform(
      mean = paired_data$mean.alt, 
      sd = paired_data$se.alt,
      transformation = "linear_to_log" ),
    delta_transform(
      mean = paired_data$mean.ref, 
      sd = paired_data$se.ref,
      transformation = "linear_to_log")
  ))
  names(dat_diff) <- c("mean_alt", "mean_se_alt", "mean_ref", "mean_se_ref")
  paired_data[, c("log_diff", "log_diff_se")] <- calculate_diff(
    df = dat_diff, 
    alt_mean = "mean_alt", alt_sd = "mean_se_alt",
    ref_mean = "mean_ref", ref_sd = "mean_se_ref" )
  
  message("Data matched! Saving matches to FILEPATH/bundle_92.csv")
  write.csv(paired_data,"FILEPATH/bundle_92.csv",row.names=F)
          
  return(paired_data)
}

run_mrbrt <- function(matched_df, data) {
  ## Run MR-BRT functions
  # Claims adjustment
  claims <- matched_df[altvar=="claims"]
  df_claims <- CWData(
    df = claims,          # dataset for metaregression
    obs = "log_diff",       # column name for the observation mean
    obs_se = "log_diff_se", # column name for the observation standard error
    alt_dorms = "altvar",     # column name of the variable indicating the alternative method
    ref_dorms = "refvar",     # column name of the variable indicating the reference method
    study_id = "nid.alt",    # name of the column indicating group membership, usually the matching groups
    add_intercept = TRUE    
  )
  
  fit_claims <- CWModel(
    cwdata = df_claims,            
    obs_type = "diff_log", 
    cov_models = list(       
      CovModel(cov_name = "intercept")),
    gold_dorm = "hospital"   
  )
  
  # Literature adjustment
  lit <- matched_df[altvar=="lit"]
  df_lit <- CWData(
    df = lit,          # dataset for metaregression
    obs = "log_diff",       # column name for the observation mean
    obs_se = "log_diff_se", # column name for the observation standard error
    alt_dorms = "altvar",     # column name of the variable indicating the alternative method
    ref_dorms = "refvar",     # column name of the variable indicating the reference method
    study_id = "nid.alt",    # name of the column indicating group membership, usually the matching groups
    add_intercept = TRUE      # adds a column called "intercept" that may be used in CWModel()
  )
  
  fit_lit <- CWModel(
    cwdata = df_lit,            
    obs_type = "diff_log", 
    cov_models = list(       
      CovModel(cov_name = "intercept")),
    gold_dorm = "hospital"  
  )
  
  ## Apply MR-BRT coefficients
  # set obs_method var for identifying within adjust_orig_vals
  locs <- fread("FILEPATH/locs.csv")[,.(ihme_loc_id,location_id)]
  data$ihme_loc_id <- NULL
  data <- merge(data,locs,by="location_id",all.x=TRUE)
  data[,obs_method:="hospital"]
  data[cv_marketscan==1 & ihme_loc_id %like% "USA", obs_method:="claims"]
  data[cv_literature==1, obs_method:="lit"]
  data[,data_id:=.I]
  # run adjust_orig_vals
  preds_claims <- adjust_orig_vals(
    fit_object = fit_claims, df = data[obs_method=="claims"], orig_dorms = "obs_method",
    orig_vals_mean = "mean", orig_vals_se = "standard_error", data_id="data_id"
  )
  preds_lit <- adjust_orig_vals(
    fit_object = fit_lit, df = data[obs_method=="lit"], orig_dorms = "obs_method",
    orig_vals_mean = "mean", orig_vals_se = "standard_error", data_id="data_id"
  )
  preds <- rbind(preds_claims,preds_lit)
  data <- merge(data,preds,by="data_id",all=TRUE)
  data[!is.na(ref_vals_mean), `:=` (mean = ref_vals_mean,
                                    standard_error = ref_vals_sd)]
  data[,c("obs_method","data_id","ref_vals_mean","ref_vals_sd","pred_diff_mean","pred_diff_sd") := NULL]
  return(data)
}
