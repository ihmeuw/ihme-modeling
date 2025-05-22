
#' @description Main script for GBD meningitis crosswalking,
#'              for MARKETSCAN AND MARKETSCAN 2000 ONLY.
#'              Creates model object for MS and applies it.
#'              Sex-split: See bundle_sex_split
#'              Crosswalk:
#'              Reference: inpatient data with correction factor 2
#'              Alternatives: inpatient only marketscan from 2000
#'                            all other inpatient only claims data
#'              MR-BRT model used logit difference (logit(alt) - logit(ref)) and
#'              used Delta method to calculate standard error in logit space
#'              Age-split: Used clinical data global age pattern to 
#'                         split numerator and population to split denominator 
#'                         for rows with age ranges greater than 25 years
#'               
   

rm(list=ls())

if (Sys.info()["sysname"] == "Linux") {
  j <- "FILEPATH" 
  h <- paste0("FILEPATH", Sys.info()["user"], "/")
  l <- "FILEPATH"
} else { 
  j <- "FILEPATH"
  h <- "FILEPATH"
  l <- "FILEPATH"
}

pacman::p_load(openxlsx, pbapply, ggplot2, data.table, boot, msm, metafor)

# SOURCE FUNCTIONS --------------------------------------------------------

# Source all GBD shared functions at once
invisible(sapply(list.files("FILEPATH", full.names = T), source))

# Set objects
bundle <- 28
cause <- 332
name_short <- "meningitis"
acause <- "meningitis"
release <- 16
save_cv <- F

helper_dir_2 <- paste0(h, "FILEPATH")
source(paste0(helper_dir_2, "bundle_age_split.R" ))
source(paste0(helper_dir_2, "get_cases_sample_size.R" ))
source(paste0(helper_dir_2, "get_closest_age.R" ))
source(paste0(helper_dir_2, "get_gbd_age_group_id.R" ))
source(paste0(helper_dir_2, "split_numerator.R" ))
source(paste0(helper_dir_2, "split_denominator.R" ))
source(paste0(helper_dir_2, "get_clinical_age_pattern.R" ))
source(paste0(helper_dir_2, "find_nondismod_locs.R" ))

# need study id
study_id <- "ihme_loc_abv" # this is only for surveillance

bundle_version_dir <- paste0(j, 'FILEPATH')
bv_tracker <- fread(paste0(bundle_version_dir, 'gbd2021_bundle_version_tracking.csv'))
bv_row <- bv_tracker[bundle_id == bundle & current_best == 1]

bv_id <- bv_row$bundle_version
bv_id <- 46237

# combine marketscan and surveillance crosswalks
source_date_surv <- "2024_06_07"
source_date_ms <- "2024_06_07"
dir <- "/FILEPATH"

surveillance <- fread(paste0("/FILEPATH"))

marketscan <- fread(paste0("FILEPATH"))

# remove the reference data which is included in both 
# data with no definition is also included in both (such as measure is not incidence)
marketscan <- marketscan[measure == "incidence" & definition != "cv_inpatient"] # this will remove duplicates
# remove columns that are surplus
cols_remove <- setdiff(names(surveillance), names(marketscan))
surveillance[, (cols_remove) := NULL]
final_dt <- rbind(marketscan, surveillance)

# save a non-age-split crosswalk version
# save the age split data
date <- gsub("-", "_", Sys.Date())

out_dir <- paste0("/FILEPATH")

xwalk_model_name <- paste0(date, "_non_network_marketscan_age_spline_and_network_surveillance_subdef_linear_haq_no_trim_study_id_country")
file_out_path <- paste0(out_dir, xwalk_model_name, "_all_xwalked_data.csv")
dir.create(out_dir, recursive = TRUE)
fwrite(final_dt, file_out_path)
file_out_path <- paste0(out_dir, xwalk_model_name, "_all_xwalked_data.xlsx")
write.xlsx(final_dt, file_out_path, sheetName = "extraction")
file_out_path <- paste0(out_dir, xwalk_model_name, "_all_xwalked_data_drop_grp_rev_0.xlsx")
final_dt_no_grp_rev <- final_dt[group_review == 1 | is.na(group_review)]
write.xlsx(final_dt_no_grp_rev, file_out_path, sheetName = "extraction")

# save a crosswalk version
if (save_cv == T){
  desc <- paste("description", study_id)
  
  result <- save_crosswalk_version(bundle_version_id = bv_id, 
                                   data_filepath = file_out_path,
                                   description = desc
  )
  
  if (result$request_status == "Successful") {
    df_tmp <- data.table(bundle_id = bundle,
                         bundle_version_id = bv_id,
                         crosswalk_version = result$crosswalk_version_id, 
                         parent_crosswalk_version = NA,
                         is_bulk_outlier = 0,
                         date = date,
                         description = desc,
                         filepath = file_out_path,
                         current_best = 1)
    
    cv_tracker <- read.xlsx(paste0(bundle_version_dir, 'crosswalk_version_tracking.xlsx'))
    cv_tracker <- data.table(cv_tracker)
    cv_tracker[bundle_id == bundle,`:=` (current_best = 0)]
    cv_tracker <- rbind(cv_tracker, df_tmp, fill = T)
    write.xlsx(cv_tracker, paste0(bundle_version_dir, 'crosswalk_version_tracking.xlsx'))
  }
}

# AGE SPLIT THE DATA 
age_split_dt <- age_split_data(final_dt, cause, release_id = release)

# MISC DATA PROCESSING
final_age_split_dt <- age_split_dt[location_id != 95]
final_age_split_dt <- final_age_split_dt[group_review != 0 | is.na(group_review)]
final_age_split_dt$unit_value_as_published <- 1

# Outlier USA MS 2015
loc_meta <- get_location_metadata(location_set_id = 35, release_id = release)
usa_locs <- loc_meta[ihme_loc_id %like% "USA"]$location_id
final_age_split_dt[location_id %in% usa_locs & year_start == 2015 & clinical_data_type == "claims, inpatient only",
                   `:=` (is_outlier = 1, outlier_reason = "description")]

# save the age split data
file_out_path <- paste0(out_dir, xwalk_model_name, "_age_split_and_xwalked_data.csv")
fwrite(final_age_split_dt, file_out_path)
file_out_path <- paste0(out_dir, xwalk_model_name, "_age_split_and_xwalked_data.xlsx")
write.xlsx(final_age_split_dt, file_out_path, sheetName = "extraction")


bv_tracker <- fread(paste0(bundle_version_dir, 'gbd2021_bundle_version_tracking.csv'))
bv_row <- bv_tracker[bundle_id == bundle & current_best == 1]
bv_id <- bv_row$bundle_version

## Removing CFR rows

data <- read.xlsx(file_out_path)
data <- as.data.table(data)
data <- data[!(measure == "cfr")]
write.xlsx(data, file_out_path, sheetName = "extraction")

# save a crosswalk version
if (save_cv == T){
  desc <- paste("description")
  
  result <- save_crosswalk_version(bundle_version_id = bv_id, 
                                   data_filepath = file_out_path,
                                   description = desc
  )
  
  if (result$request_status == "Successful") {
    df_tmp <- data.table(bundle_id = bundle,
                         bundle_version_id = bv_id,
                         crosswalk_version = result$crosswalk_version_id, 
                         parent_crosswalk_version = NA,
                         is_bulk_outlier = 0,
                         date = date,
                         description = desc,
                         filepath = file_out_path,
                         current_best = 1)
    
    cv_tracker <- read.xlsx(paste0(bundle_version_dir, 'crosswalk_version_tracking.xlsx'))
    cv_tracker <- data.table(cv_tracker)
    cv_tracker[bundle_id == bundle,`:=` (current_best = 0)]
    cv_tracker <- rbind(cv_tracker, df_tmp, fill = T)
    write.xlsx(cv_tracker, paste0(bundle_version_dir, 'crosswalk_version_tracking.xlsx'))
  }
}
