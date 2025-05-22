# date: 08/07/2020
# author: 
# purpose: use for SEX SPLIT crosswalk versions of etiologies for GBD 2020

rm(list=ls())
date <- gsub("-", "_", Sys.Date())
ds <- 'iterative'
gbd_round_id = 7
# sync = T

library(openxlsx)
library(data.table)
library(ggplot2)
library(parallel)
library(magrittr)
library(plyr)
library(crosswalk, lib.loc = "/filepath/")
source("/filepath/save_crosswalk_version.R")
source("/filepath/get_bundle_version.R")

source('/filepath/bundle_age_split_etiology.R')

helper_dir <- "filepath"
source(paste0(helper_dir, "sex_split_group_review.R" ))
source(paste0(helper_dir, "bundle_sex_split.R" ))
source(paste0(helper_dir, "rm_zeros.R" ))
source(paste0(helper_dir, "graph_xwalk.R" ))
source(paste0(helper_dir, "clean_mr_brt.R" ))
source(paste0(helper_dir, "graph_sex_split.R" ))
source(paste0(helper_dir, "etio_age_split_helpers.R" ))
source(paste0(helper_dir, "find_nondismod_locs.R" ))
source(paste0(helper_dir, "get_cases_sample_size.R" ))
source(paste0(helper_dir, "save_crosswalk_RDS.R" ))

bundle_map <- as.data.table(read.xlsx(paste0('filepath/bundle_map.xlsx')))

dir_2020 <- 'filepath'
bv_tracker <- fread(paste0(dir_2020, 'bundle_version_tracking.csv'))

data_dir <- 'filepath'
bundles <- c(29, 33, 37, 41, 7958)
bundle_map <- bundle_map[bundle_id %in% bundles]
age_split <- T
age_start_vector <- c(0, 1.0, 5.0, 20.0, 40.0, 60.0)
age_end_vector <- c(0.99, 4.99, 19.99, 39.99, 59.99, 99.99)

for (i in 1:nrow(bundle_map)) {
  id <- bundle_map$bundle_id[i] 
  if (id == 37){
    age_start_vector <- c(0, 5.0, 20.0, 40.0, 60.0)
    age_end_vector <- c(4.99, 19.99, 39.99, 59.99, 99.99)
  } else {
    age_start_vector <- c(0, 1.0, 5.0, 20.0, 40.0, 60.0)
    age_end_vector <- c(0.99, 4.99, 19.99, 39.99, 59.99, 99.99)
  }
  bv_id <- bv_tracker[bundle_id == id & current_best == 1]$bundle_version
  bv_df <- get_bundle_version(bundle_version_id = bv_id,
                              fetch = "all", 
                              transform = T, 
                              export = T)
  
  out_dir <- "filepath"
  drop_threshold <- 10
  
  # drop any rows that have a sample size less than 10
  # fill in cases, sample size, mean - including getting SS from effective SS!
  bv_df <- get_cases_sample_size(bv_df)
  drop <- nrow(bv_df[sample_size < drop_threshold | effective_sample_size < drop_threshold])
  message(paste("dropping", drop, "rows with a sample size less than", drop_threshold))
  bv_df$drop <- 0
  bv_df[sample_size < drop_threshold | effective_sample_size < drop_threshold, drop := 1]
  
  # sex split the bundle data
  # Specify model names to uses a prefixes for the MR-BRT output directories
  sex_model_name   <- paste0(date, "_bundle_",id, "_logit_sexsplit_on_bv_", bv_id)
  plot_out_dir <- paste0(out_dir, date, "_bundle_",id)
  group_review_split <- sex_split_group_review(bv_df, plot_out_dir, bv_id, plot = F)
  
  dem_sex_dt <- copy(group_review_split)
  
  plot <- T
  offset <- F # apply an offset to data that is 0 or 1?
  drop_zeros <- T # drop any values that are 0 or 1? 
  fix_ones <- T # since we are modeling in log space
  xw_measure <- "proportion"
  
  dem_sex_final_list <- run_sex_split(dem_sex_dt = dem_sex_dt, # grp review split
                                      data = bv_df,# not grp review split, for matching
                                      out_dir = out_dir, 
                                      sex_model_name = sex_model_name,
                                      offset = offset, 
                                      drop_zeros = drop_zeros,
                                      fix_ones = fix_ones,
                                      plot = plot)
  dem_sex_final_dt <- dem_sex_final_list$data
  
  # subset to only non-drop rows
  dem_sex_final_dt <- dem_sex_final_dt[drop == 0]
  ## Save sex-split data as a CSV
  fwrite(dem_sex_final_dt, paste0(out_dir, sex_model_name, ".csv"))
  
  ## Apply age splitting
  if (age_split == T){
    age_split_dt <- age_split_data(xwalk_final_dt = dem_sex_final_dt, 
                                   gbd_round_id = gbd_round_id, 
                                   ds = ds, 
                                   bundle = id,
                                   age_start_vector,
                                   age_end_vector)
    age_model_name <-  paste0(date, "_bundle_",id, "_logit_sexsplit_and_agesplit_threshold_", drop_threshold, "_on_bv_", bv_id)
  } else if (age_split == F){
    age_split_dt <- copy(dem_sex_final_dt)
    age_model_name <-  paste0(date, "_bundle_",id, "_logit_sexsplit_and_agesplit_threshold_", drop_threshold, "_on_bv_", bv_id)
  }
  
  # misc data processing and save excel sheet
  dem_sex_final_dt <- copy(age_split_dt)
  dem_sex_final_dt <- find_nondismod_locs(dem_sex_final_dt)
  message(paste("you have", nrow(dem_sex_final_dt[standard_error > 1]), "rows with SE > 1, 
                likely due to small sample size and/or closeness to 0 or 1 value.  these are being coerced to 1"))
  dem_sex_final_dt[standard_error > 1, standard_error := 1]
  dem_sex_final_dt <- dem_sex_final_dt[group_review == 1 | is.na(group_review)]
  write.xlsx(dem_sex_final_dt, paste0(out_dir, age_model_name, "_for_upload.xlsx"), sheetName = "extraction")
    
  # save a crosswalk version
  description <- paste('logit sex split, age split on overall meningitis MV588950, age_start_vector', 
                       paste(age_start_vector, collapse= ","), 
                       'drop ss threshold', drop_threshold, 
                       'from bundle version', bv_id)
  result <- save_crosswalk_version(bundle_version_id = bv_id,
                                   paste0(out_dir, age_model_name, "_for_upload.xlsx"), 
                                   description = description
                                   )
  
  print(sprintf('Request status: %s', result$request_status))
  print(sprintf('Request ID: %s', result$request_id))
  print(sprintf('Crosswalk version ID: %s', result$crosswalk_version_id))
  
  if (result$request_status == "Successful") {
    df_tmp <- data.table(bundle_id = id,
                         bundle_version_id = bv_id,
                         crosswalk_version = result$crosswalk_version_id, 
                         parent_crosswalk_version = NA,
                         is_bulk_outlier = 0,
                         filepath = paste0(out_dir, sex_model_name, "_for_upload.xlsx"),
                         current_best = 1,
                         date = date,
                         description = description)

    cv_tracker <- as.data.table(read.xlsx(paste0(dir_2020, 'crosswalk_version_tracking.xlsx')))
    cv_tracker[bundle_id == id, current_best := 0]
    cv_tracker <- rbind(cv_tracker, df_tmp, fill = T)
    write.xlsx(cv_tracker, paste0(dir_2020, 'crosswalk_version_tracking.xlsx'))
  }
}

