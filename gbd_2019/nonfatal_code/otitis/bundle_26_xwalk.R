#' @author
#' @date 2019/05/15
#' @description GBD 2019 Acute Otitis Media sex-split and xwalk

rm(list=ls())

library(openxlsx)

# SOURCE FUNCTIONS --------------------------------------------------------
source(paste0(k, "current/r/save_bundle_version.R"))
source(paste0(k, "current/r/get_bundle_version.R"))
source(paste0(k, "current/r/save_crosswalk_version.R"))
source(paste0(k, "current/r/get_location_metadata.R"))
# source MR-BRT wrapper functions
repo_dir <- # filepath
source(paste0(repo_dir, "run_mr_brt_function.R"))
source(paste0(repo_dir, "cov_info_function.R"))
source(paste0(repo_dir, "check_for_outputs_function.R"))
source(paste0(repo_dir, "plot_mr_brt_function.R"))
source(paste0(repo_dir, "load_mr_brt_outputs_function.R"))
source(paste0(repo_dir, "predict_mr_brt_function.R"))
source(paste0(repo_dir, "check_for_preds_function.R"))
source(paste0(repo_dir, "load_mr_brt_preds_function.R"))

# SET OBJECTS -------------------------------------------------------------
ds <- 'step2'
# version <- save_bundle_version(26, ds, include_clinical = T)
# version_id <- 1364 # decomp 2 refresh 1 bundle version
version_id <- 1484   # decomp 2 refresh 2 bundle version

dem_dir <- # filepath
mrbrt_dir <- # filepath

date <- gsub("-", "_", Sys.Date())
sex_model_name <- paste0(date, "_AOM_sexsplit")

# INPUTS ------------------------------------------------------------------
get_dem_data <- function() {
  dt <- get_bundle_version(version_id, transform = T)
  return(dt)
}

get_study_cov <- function(raw_dt) {
  dt <- copy(raw_dt)
  ms2000_nid <- c(244369)
  ms2010_nid <- c(244370)
  ms2012_nid <- c(244371)
  ms_nids    <- c(244369, 244370, 244371, 336848, 336850, 336849)
  taiwan_nid <- c(336203)
  
  cv_colnames <- c("cv_marketscan_all_2000", "cv_marketscan_all_2010",
                   "cv_marketscan_all_2012", "cv_marketscan",
                   "cv_taiwan_claims_data")
  dt[, cv_colnames := 0]
  dt[nid %in% ms2000_nid, cv_marketscan_all_2000:= 1]
  dt[nid %in% ms2010_nid, cv_marketscan_all_2010:= 1]
  dt[nid %in% ms2012_nid, cv_marketscan_all_2012:= 1]
  dt[nid %in% ms_nids,    cv_marketscan         := 1]
  dt[nid %in% taiwan_nid, cv_taiwan_claims_data := 1]
  
  # Remove inpatient data since it was not used
  dt <- dt[clinical_data_type != "inpatient"]
  return(dt)
}

# REMOVE GROUP_REVIEW == 0 before saving xwalk version
remove_group_review_0 <- function(sexplit_dt) {
  dt <- copy(sexplit_dt)
  dt <- dt[group_review != 0 | is.na(group_review)]
  return(dt)
}

# SEX SPLIT ---------------------------------------------------------------
source() # filepath
# source the function bundle_sex_split.R
dem_sex_dt <- get_dem_data()
dem_sex_dt <- get_study_cov(dem_sex_dt)
dem_sex_final_dt <- run_sex_split(dem_sex_dt, drop_large_se = F, sex_model_name)

# SAVE CROSSWALK VERSION --------------------------------------------------
xwalk_dt <- remove_group_review_0(dem_sex_final_dt$final)

wb <- createWorkbook()
addWorksheet(wb, "extraction")
writeData(wb, "extraction", xwalk_dt)
saveWorkbook(wb, paste0(dem_dir, sex_model_name, ".xlsx"), overwrite = T)

result <- save_crosswalk_version(bundle_version_id = version_id, 
                                 data_filepath = paste0(dem_dir, sex_model_name, ".xlsx"), 
                                 description = paste0(sex_model_name, "_female_male_ratio (", dem_sex_final_dt$ratio, ")"))

print(sprintf('Request status: %s', result$request_status))
print(sprintf('Request ID: %s', result$request_id))
print(sprintf('Crosswalk version ID: %s', result$crosswalk_version_id))
