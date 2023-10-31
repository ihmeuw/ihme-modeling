#' @author 
#' @date 2020/08/18
#' @description Main script for GBD 2020 meningitis crosswalking,
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

pacman::p_load(openxlsx, pbapply, ggplot2, data.table, boot, msm, metafor)

# SOURCE FUNCTIONS --------------------------------------------------------
# Source all GBD shared functions at once
shared.dir <- "/filepath/"
files.sources <- list.files(shared.dir)
files.sources <- paste0(shared.dir, files.sources) # writes the filepath to each function
invisible(sapply(files.sources, source)) # "invisible suppresses the sapply output

# Set objects
bundle <- 28
cause <- 332
name_short <- "meningitis"
acause <- "meningitis"
ds <- 'iterative'
gbd_round_id <- 7
save_cv <- T

helper_dir <- "filepath"
source(paste0(helper_dir, "bundle_age_split.R" ))
source(paste0(helper_dir, "get_cases_sample_size.R" ))
source(paste0(helper_dir, "get_closest_age.R" ))
source(paste0(helper_dir, "get_gbd_age_group_id.R" ))
source(paste0(helper_dir, "split_numerator.R" ))
source(paste0(helper_dir, "split_denominator.R" ))
source(paste0(helper_dir, "get_clinical_age_pattern.R" ))
source(paste0(helper_dir, "find_nondismod_locs.R" ))

# need study id
study_id <- "ihme_loc_abv" # this is only for surveillance
bundle_version_dir <- "filepath"
bv_tracker <- fread(paste0(bundle_version_dir, 'bundle_version_tracking.csv'))
bv_row <- bv_tracker[bundle_id == bundle & current_best == 1]

bv_id <- bv_row$bundle_version

# combine marketscan and surveillance crosswalks
source_date <- "2020_12_24"
surveillance <- fread(paste0("filepath", "_all_xwalked_data_NO_Marketscan.csv")) # does not include reference rows
marketscan <- fread(paste0("filepath", "_marketscan_xwalked_data_NO_reference.csv")) # includes reference rows

# remove the reference data which is included in both (despite the marketscan filename)
# data with no definition is also included in both (such as measure is not incidence)
marketscan <- marketscan[measure == "incidence" & definition != "cv_inpatient"] # this will remove duplicates
# remove columns that are surplus
cols_remove <- setdiff(names(surveillance), names(marketscan))
surveillance[, (cols_remove) := NULL]
final_dt <- rbind(marketscan, surveillance)

# save a non-age-split crosswalk version
# save the age split data
date <- gsub("-", "_", Sys.Date())
out_dir <- "filepath"
xwalk_model_name <- paste0(date, "_name")
file_out_path <- paste0(out_dir, xwalk_model_name, "_all_xwalked_data.csv")
fwrite(final_dt, file_out_path)
file_out_path <- paste0(out_dir, xwalk_model_name, "_all_xwalked_data.xlsx")
write.xlsx(final_dt, file_out_path, sheetName = "extraction")
file_out_path <- paste0(out_dir, xwalk_model_name, "_all_xwalked_data_drop_grp_rev_0.xlsx")
final_dt_no_grp_rev <- final_dt[group_review == 1 | is.na(group_review)]
write.xlsx(final_dt_no_grp_rev, file_out_path, sheetName = "extraction")

# save a crosswalk version
if (save_cv == T){
  desc <- paste("updated crosswalk, age spline for marketscan & ms 2000, linear haq and composite defs for surveillance, no trim, study_id on", study_id)
  
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
model_version_id <- 589658 #588950 # age pattern for parent meningitis 
age_split_dt <- age_split_data(final_dt, cause, gbd_round_id, ds)

# MISC DATA PROCESSING
# age_split_dt[is.na(cv_diag_blood_culture), cv_diag_blood_culture := cv_diag_blood]
# age_split_dt$cv_diag_blood <- NULL
final_age_split_dt <- age_split_dt[location_id != 95]
final_age_split_dt <- final_age_split_dt[group_review != 0 | is.na(group_review)]
final_age_split_dt$unit_value_as_published <- 1

# save the age split data
file_out_path <- paste0(out_dir, xwalk_model_name, "_age_split_and_xwalked_data.csv")
fwrite(final_age_split_dt, file_out_path)
file_out_path <- paste0(out_dir, xwalk_model_name, "_age_split_and_xwalked_data.xlsx")
write.xlsx(final_age_split_dt, file_out_path, sheetName = "extraction")

bundle_version_dir <- "filepath"
bv_tracker <- fread(paste0(bundle_version_dir, 'bundle_version_tracking.csv'))
bv_row <- bv_tracker[bundle_id == bundle & current_best == 1]
bv_id <- bv_row$bundle_version

# save a crosswalk version
if (save_cv == T){
  desc <- paste("updated age pattern for age split to CVID 34376 non_network, age spline for marketscan & ms 2000, age spline and linear haq surveillance")
  
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
