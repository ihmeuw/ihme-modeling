###########################################################
### Purpose: MR-BRT Outliering
###########################################################

###################
### Setting up ####
###################
rm(list=ls())
pacman::p_load(data.table, dplyr, stats, boot, msm)

os <- .Platform$OS.type
if (os == "windows") {
  j <- "J:/"
  h <- "H:/"
} else {
  j <- "FILEPATH/j/"
  h <- paste0("FILEPATH", Sys.getenv("USER"),"/")
}

## Source General Functions
source("/FILEPATH/merge_on_location_metadata.R")

## Define Functions
prep_data <- function(crosswalk_version_id) {
  # read in data
  df <- get_crosswalk_version(FILEPATH)
  # prep data
  df <- df[measure == 'prevalence', 
           c('seq', 'nid', 'location_id', 'year_start', 'year_end', 'age_start', 'age_end','sex','mean','standard_error',
             'is_outlier', 'cv_literature'),
           with = FALSE]
  
  setnames(df, 'standard_error','se')
  
  df[, year_id := floor((year_start + year_end)/2)]
  df <- df[year_id < 1980, year_id := 1980]
  
  df <- merge_on_location_metadata(df)
  df[, country := location_id]
  df[level > 3, country := parent_id]
  df[, country_name := location_name]
  df[ level > 3, country_name := parent_name]
  df[, sex_id := ifelse(sex=="Male",1,2)]
  df[, female := ifelse(sex=="Female",0,1)]
  df[, age_group_id := ifelse(age_start==0,2,3)]
  df[, lnn := ifelse(age_group_id==3,1,0)]
  
  #offset to allow logit transform of means of zero
  df[, mean := mean + 0.001]

  # add HAQi
  haqi <- get_covariate_estimates(FILEPATH)[,.(location_id,year_id,mean_value)]
  setnames(haqi,"mean_value","haqi")
 
  # merge
  df <- merge(df,haqi,by=c("location_id","year_id"),all.x=TRUE)

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

df <- prep_data(crosswalk_version_id = 17210)
df <- prep_mrbrt(df)

#save data to file
write.csv(df, file = "FILEPATH/gbd2020_d2_data.csv", row.names = FALSE)

# Launch do_mrbrt model script
script <- paste0(h, "FILEPATH/02b_do_mrbrt.R")
trim_percent <- 40

job <- paste("qsub", script, cv, trim_percent)
system(job)
