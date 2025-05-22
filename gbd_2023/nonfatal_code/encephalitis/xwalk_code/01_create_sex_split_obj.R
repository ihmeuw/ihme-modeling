#' @author
#' @description
#' CROSSWALKING GBD 2019 for Encephalitis

rm(list=ls())
pacman::p_load(openxlsx, pbapply)

# SOURCE FUNCTIONS --------------------------------------------------------
source() # source functions for data upload and download 
source(paste0("filepath/find_nondismod_locs.R"))
source(paste0("filepath/get_study_cov.R"))
source("filepath/bundle_sex_split_gbd_2019.R")

# SET OBJECTS -------------------------------------------------------------
version_id <- 33668 # "iterative"
ds <- "iterative"
# Specificy which xwalk versions you would like to upload
save_sex_split_version <- T
save_sex_split_xwalk_version <- T
save_sex_age_split_xwalk_version <- T
# Specify where to save xwalk versions and MR-BRT outputs
dem_dir <- "filepath"
mrbrt_dir <- "filepath"
model_objects_dir <- "filepath"
ref_name <- c("cv_inpatient")
alt_names <- c("cv_marketscan_data", "cv_marketscan_inp_2000", "cv_surveillance")
# Specify model names
date <- gsub("-", "_", Sys.Date())
sex_model_name <- paste0(date, "_encephalitis_sexsplit")
xwalk_model_name <- paste0(date, "_encephalitis_xwalk")
age_model_name <- paste0(date, "_encephalitis_agesplit")

# SEX SPLIT ---------------------------------------------------------------
dem_sex_dt <- get_bundle_version(version_id, fetch = "all", export = T, transform = T)
dem_sex_dt <- find_nondismod_locs(dem_sex_dt)
dem_sex_dt <- get_study_cov(dem_sex_dt)
# dem_sex_dt <- outlier_clinical_data(dem_sex_dt)
dem_sex_final_list <- run_sex_split(dem_sex_dt, sex_model_name)
ratio <- dem_sex_final_list$ratio
sex_model <- dem_sex_final_list$model
dem_sex_final_dt <- dem_sex_final_list$data
fwrite(dem_sex_final_dt, paste0(dem_dir, sex_model_name, ".csv"))

# SAVE SEX-SPLIT XWALK VERSION --------------------------------------------
if (save_sex_split_version) {
  sexsplit_xwalk_dt <- dem_sex_final_dt[group_review == 1 | is.na(group_review)]
  
  wb <- createWorkbook()
  addWorksheet(wb, "extraction")
  writeData(wb, "extraction", sexsplit_xwalk_dt)
  saveWorkbook(wb, paste0(dem_dir, sex_model_name, ".xlsx"), overwrite = T)
  
  result <- save_crosswalk_version(bundle_version_id = version_id, data_filepath = paste0(dem_dir, sex_model_name, ".xlsx"), 
                                   description = paste0(sex_model_name, "_female_male_ratio (", dem_sex_final_dt$ratio, ")"))
  
  print(sprintf('Request status: %s', result$request_status))
  print(sprintf('Request ID: %s', result$request_id))
  print(sprintf('Crosswalk version ID: %s', result$crosswalk_version_id))
}
