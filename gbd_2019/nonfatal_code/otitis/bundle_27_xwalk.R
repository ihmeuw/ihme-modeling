#' @author
#' @date 2019/05/15
#' @description GBD 2019 Chronic Otitis Media sex-split and xwalk

rm(list=ls())

library(openxlsx)

# SOURCE FUNCTIONS --------------------------------------------------------

source(paste0(k, "current/r/save_bundle_version.R"))
source(paste0(k, "current/r/get_bundle_version.R"))
source(paste0(k, "current/r/save_crosswalk_version.R"))
source(paste0(k, "current/r/get_location_metadata.R"))

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
# version <- save_bundle_version(27, 'step2', include_clinical = F)
version_id <- 3860 # bundle version with no clinical data

dem_dir <- # filepath
mrbrt_dir <- # filepath

date <- gsub("-", "_", Sys.Date())
sex_model_name <- paste0(date, "_COM_sexsplit")

# INPUTS ------------------------------------------------------------------
get_dem_data <- function() {
  dt <- get_bundle_version(version_id, transform = T)
  return(dt)
}

# REMOVE GROUP_REVIEW == 0 before saving xwalk version
remove_group_review_0 <- function(sexplit_dt) {
  dt <- copy(sexplit_dt)
  dt <- dt[group_review != 0 | is.na(group_review)]
  return(dt)
}
# REMOVES LOCATIONS THAT ARE NOT IN THE DISMOD LOCATION HIERARCHY FOR XWALK VERSION
remove_nondismod_locs <- function(sexsplit_dt) {
  dt <- copy(sexsplit_dt)
  locs <- get_location_metadata(9)
  print("Dropping the following non-DisMod locations:")
  print(unique(dt[!location_id %in% locs$location_id, .(location_name, location_id)]))
  dt <- dt[location_id %in% locs$location_id]
  return(dt)
}

# COPIES BOTH SEX REMISSION DATA AS MALE AND FEMALE REMISSION FOR XWALK VERSION
get_sex_remission <- function(sexsplit_dt) {
  dt <- copy(sexsplit_dt)
  not_remission_dt <- dt[measure != "remission"]
  male_dt <- copy(dt[measure == "remission"])
  male_dt[, `:=`(sex = "Male", 
                 specificity = "sex", 
                 group_review = 1, 
                 group = 1)]
  male_dt[, crosswalk_parent_seq := seq]
  male_dt[, seq := NA]
  female_dt <- copy(dt[measure == "remission"])
  female_dt[, `:=`(sex = "Female", 
                   specificity = "sex", 
                   group_review = 1, 
                   group = 1)]
  female_dt[, crosswalk_parent_seq := seq]
  female_dt[, seq := NA]
  dt <- rbindlist(list(male_dt, female_dt, not_remission_dt))
}

# SEX SPLIT ---------------------------------------------------------------
source() # filepath
# source the function bundle_sex_split.R
dem_sex_dt <- get_dem_data()
dem_sex_final_dt <- run_sex_split(dem_sex_dt, sex_model_name)

xwalk_dt <- remove_group_review_0(dem_sex_final_dt$final)
xwalk_dt <- remove_nondismod_locs(xwalk_dt)
xwalk_dt <- get_sex_remission(xwalk_dt)


# SAVE CROSSWALK VERSION --------------------------------------------------
wb <- createWorkbook()
addWorksheet(wb, "extraction")
writeData(wb, "extraction", xwalk_dt)
saveWorkbook(wb, paste0(dem_dir, sex_model_name, ".xlsx"), overwrite = T)

xwalk_filepath <- paste0(dem_dir, sex_model_name, ".xlsx")
xwalk_desc <- paste0(sex_model_name, "_female_male_ratio (", dem_sex_final_dt$ratio, ") no clinical data, sex-specific remission")
result <- save_crosswalk_version(bundle_version_id = version_id, 
                                 data_filepath     = xwalk_filepath, 
                                 description       = xwalk_desc)

print(sprintf('Request status: %s', result$request_status))
print(sprintf('Request ID: %s', result$request_id))
print(sprintf('Crosswalk version ID: %s', result$crosswalk_version_id))
