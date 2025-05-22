###########################################################
### Date: 08/26/2019
### Project: Neonatal Encephalopathy
### Purpose: Investigate MR-BRT Outliering
###########################################################
###################
### Setting up ####
###################
rm(list=ls())
pacman::p_load(data.table, dplyr, ggplot2, stats, boot, msm, openxlsx)
os <- .Platform$OS.type
if (os == "windows") {
  j <- "PATHNAME"
  h <- "PATHNAME"
} else {
  j <- "PATHNAME"
  user <- Sys.info()[["user"]]
  h <- "PATHNAME"
}
## Source General Functions
source("/PATHNAME/merge_on_location_metadata.R")
source("/PATHNAME/get_covariate_estimates.R")
source("/PATHNAME/get_crosswalk_version.R")
source("/PATHNAME/get_age_metadata.R")
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
## Define Functions
prep_data <- function(crosswalk_version_id) {
  # read in data
  df <- data.table(read.xlsx("PATHNAME"))
  
  # prep data
  df <- df[measure == 'prevalence', 
           c('seq', 'nid', 'location_id', 'year_start', 'year_end', 'age_start', 'age_end','sex','mean','standard_error',
             'is_outlier', 'cv_literature'),
           with = FALSE]

  setnames(df, 'standard_error','se')
  
  df[, year_id := floor((year_start + year_end)/2)]
  df <- df[year_id < 1980, year_id := 1980]
  #recode the Brazil data to have a year_id of 1997
  #df[nid == 104246, year_id := 1997]
  
  df <- merge_on_location_metadata(df)
  df[, country := location_id]
  df[level > 3, country := parent_id]
  df[, country_name := location_name]
  df[ level > 3, country_name := parent_name]
  df[, sex_id := ifelse(sex=="Male",1,2)]
  df[, female := ifelse(sex=="Female",0,1)]

  df[age_end == 0, age_group_id := 164]
  df[age_end == 0.01917808, age_group_id := 2]
  df[age_end == 0.07671233, age_group_id := 3]
  
  #remove new Brazil data because it leads to spline errors
  #df <- df[!(parent_name == 'Brazil' & year_id < 1995)]
  
  #offset to allow logit transform of means of zero
  df[, mean := mean + 0.001]
  
  # add LBW
  # lbw <- get_covariate_estimates(2094, decomp_step='step4')[,.(location_id,age_group_id,sex_id,year_id,mean_value)]
  # setnames(lbw,"mean_value","lbw")
  # add SG
  # preterm <- get_covariate_estimates(2092, decomp_step='step4')[,.(location_id,age_group_id,sex_id,year_id,mean_value)]
  # setnames(preterm,"mean_value","preterm")
  # add HAQi
  haqi <- get_covariate_estimates(1099, decomp_step='iterative')[,.(location_id,year_id,mean_value)]
  setnames(haqi,"mean_value","haqi")
  # # add MCI
  # mci <- get_covariate_estimates(208, decomp_step='step4')[,.(location_id,year_id,mean_value)]
  # setnames(mci,"mean_value","mci")
  # merge
  # add IFD
  ifd <- get_covariate_estimates(51, decomp_step='iterative')[,.(location_id,year_id,mean_value)]
  setnames(ifd,"mean_value","ifd")

  # add SBA
  sba <- get_covariate_estimates(143, decomp_step='iterative')[,.(location_id,year_id,mean_value)]
  setnames(sba,"mean_value","sba")
  
  # add BMI
  bmi <- get_covariate_estimates(2157, decomp_step='iterative')[,.(location_id,year_id,mean_value)]
  setnames(bmi,"mean_value","bmi")
  
  #df <- merge(df,lbw,by=c("location_id","year_id","age_group_id","sex_id"),all.x=TRUE)
  # df <- merge(df,preterm,by=c("location_id","year_id","age_group_id","sex_id"),all.x=TRUE)
  df_cov <- merge(df,haqi,by=c("location_id","year_id"),all.x=TRUE)
  # df_cov <- merge(df,ifd,by=c("location_id","year_id"),all.x=TRUE)
  # df_cov <- merge(df,sba,by=c("location_id","year_id"),all.x=TRUE)
  # df_cov <- merge(df,bmi,by=c("location_id","year_id"),all.x=TRUE)
  #df <- df[haqi > 60]
  # return
  return(df)
}

prep_mrbrt <- function(df) {
  df$mean_logit <- log(df$mean / (1- df$mean))
  df$se_logit <- sapply(1:nrow(df), function(i) {
    mean_i <- as.numeric(df[i, "mean"])
    se_i <- as.numeric(df[i, "se"])
    deltamethod(~log(x1/(1-x1)), mean_i, se_i^2)
  })
  return(df)
}




## RUN
bundle <- 338 
cv <- 'haqi'
trim_percent_list <- c(20,30,40,50)

df <- prep_data(crosswalk_version_id = 17210)
df_cov <- prep_mrbrt(df_cov)

#pre-outlier China, Mexico, Japan, Norway 
#df_new <- df[!(country %in% c(6, 44533, 130, 67, 90))]

#pre-outlier clinical data for locs where the country-level HAQI is <= 60
# haqi <- get_covariate_estimates(1099, decomp_step='step4')[,.(location_id,year_id,mean_value)]
# setnames(haqi,c("mean_value", "location_id"),c("haqi_country","country"))
# df_new <- merge(df, haqi, by=c("country","year_id"),all.x=TRUE)
# df_new <- df_new[cv_literature == 1 | (cv_literature == 0 & haqi_country > 60)]

#save data to file
write.csv(df, file = "PATHNAME", row.names = FALSE)

df_no_outliers <- df_cov[is_outlier != 1]
write.csv(df_no_outliers, 
          file = paste0("PATHNAME"),
          row.names = FALSE)

#Job specifications
username <- Sys.getenv("USER")
m_mem_free <- "-l m_mem_free=2G"
fthread <- "-l fthread=2"
runtime_flag <- "-l h_rt=00:12:00"
jdrive_flag <- "-l archive"
queue_flag <- "-q long.q"
shell_script <- "-cwd PATHNAME/SHELL"

### change to your own repo path if necessary
script <- "PATHNAME"
errors_flag <- "PATHNAME"
outputs_flag <- "PATHNAME"

for (i in trim_percent_list) {
  trim_percent <- i
  job_name <- paste0("-N", " mrbrt", trim_percent, "_", cv)
  job <- paste("qsub", m_mem_free, fthread, runtime_flag,
               jdrive_flag, queue_flag,
               job_name, "-P proj_neonatal", outputs_flag, errors_flag, shell_script, script, cv, trim_percent)
  
  system(job)
}