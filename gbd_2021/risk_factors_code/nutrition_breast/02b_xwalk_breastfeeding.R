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
  h <- paste0("FILEPATH/", user)
}

## Source General Functions
source("FILEPATH/merge_on_location_metadata.R")
source("FILEPATH/get_covariate_estimates.R")
source("FILEPATH/get_bundle_data.R")

## Source MR-BRT Functions
library(crosswalk, lib.loc = "FILEPATH")

## Define Functions
## Prep data for matching
prep_data <- function(df) {
  # prep data
  df[, standard_error := sqrt(variance)]
  df <- df[val != 0 & is_outlier!=1]
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
## Match data
match_data <- function(df) {
  # match reference and alternative definitions
  alt <- df[group == "abf611"]
  ref <- df[group == "abf1223"]
  paired_data <- merge(ref, alt, by = c('location_id','sex','nid', 'year_start'), all.x = FALSE, suffixes = c('.ref', '.alt'))
  paired_data <- paired_data[abs(year_mean.ref - year_mean.alt) < 6]
  paired_data[, year_mean := (year_mean.ref + year_mean.alt)/2]
  paired_data[, ratio := mean.alt / mean.ref]
  paired_data[, ratio_se := sqrt((mean.alt^2 / mean.ref^2) * (se.alt^2/mean.alt^2 + se.ref^2/mean.ref^2))]
  
  paired_data <- paired_data[ratio_se > .00001 & ratio_se < 10]
  paired_data <- paired_data[!(mean.ref==1 | mean.alt==1)]
  return(paired_data)
}
## Save Matches
save_matches <- function(df) {
  write.csv(df[,.(seq.ref, seq.alt, location_id, sex, ratio, ratio_se, year_mean)],
            file=paste0("FILEPATH.csv"),
            row.names=F)
}
## Prep for MR-BRT
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
  df[, altvar:="abf611"]
  df[, refvar:="abf1223"]
  df[, study:=nid]
  
  # create MR data object
  dat <- CWData(
    df = df,          # dataset for metaregression
    obs = "logit_diff",       # column name for the observation mean
    obs_se = "se_logit_diff", # column name for the observation standard error
    alt_dorms = "altvar",     # column name of the variable indicating the alternative method
    ref_dorms = "refvar",     # column name of the variable indicating the reference method
    study_id = "study",    # name of the column indicating group membership
    add_intercept = TRUE      
  )
  
  # return
  return(dat)
}
## Run MR-BRT
fit_mrbrt <- function(data,trim=0.1,me,folder,xwalk_df) {
  
  # Fit model - specify options below
  message(paste0("Fitting MR-BRT model for ABF imputation"))
  output_dir = paste0('FILEPATH/',me,"/",folder); if (!dir.exists(output_dir)) dir.create(output_dir,recursive=TRUE)
  message(paste0("Output folder is ",output_dir))
  
  fit <- CWModel(
    cwdata = data,            
    obs_type = "diff_logit", 
    cov_models = list(      
      CovModel(cov_name = "intercept")),
    gold_dorm = "abf1223",   
    inlier_pct = 1-trim
  )
  
  # Coefficient results
  message("Model complete! Saving summaries")
  df_result <- fit$create_result_df()
  write.csv(df_result,paste0(output_dir,"/coef_summary.csv"),row.names=F)
  py_save_object(object = fit, filename = paste0(output_dir,"/cw_model.pkl"), pickle = "dill")
  
  # Adjust values
  message("Applying crosswalk to ABF611 values")
  xwalk_df[,dorms := "abf611"]
  xwalk_df[,se:=sqrt(variance)]
  xwalk_df[val==1,val:=0.9999]
  preds <- adjust_orig_vals(fit, df=xwalk_df, orig_dorms="dorms", 
                            orig_vals_mean="val", orig_vals_se="se")
  
  xwalk_df[,`:=` (dorms=NULL,
                  group="abf1223",
                  orig_val=val,
                  orig_se=se)]
  xwalk_df[,`:=` (val=preds$ref_vals_mean,
                  standard_error=preds$ref_vals_sd)]
  xwalk_df[, variance:=standard_error^2]
  
  # Run funnel plot
  message("Crosswalk applied! Generating funnel plot")

  plots <- import("crosswalk.plots")
  
  plots$funnel_plot(
    cwmodel = fit, 
    cwdata = data,
    continuous_variables = list(),
    obs_method = 'abf611',
    plot_note = 'ABF Imputation Funnel Plot', 
    plots_dir = output_dir, 
    file_name = "abf_imputation_funnel",
    write_file = TRUE
  )
  
  return(xwalk_df)
}
