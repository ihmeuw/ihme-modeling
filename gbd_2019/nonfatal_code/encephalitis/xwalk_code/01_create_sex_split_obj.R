#' @author
#' @description
#' CROSSWALKING GBD 2019 for Encephalitis

rm(list=ls())

pacman::p_load(openxlsx, pbapply)

# SOURCE FUNCTIONS --------------------------------------------------------
k <- # filepath
source(paste0(k, "save_bundle_version.R"))
source(paste0(k, "get_bundle_version.R"))
source(paste0(k, "save_crosswalk_version.R"))
source(paste0(k, "get_covariate_estimates.R"))
source(paste0(k, "get_location_metadata.R"))

repo_dir <- # filepath
source(paste0(repo_dir, "run_mr_brt_function.R"))
source(paste0(repo_dir, "cov_info_function.R"))
source(paste0(repo_dir, "check_for_outputs_function.R"))
source(paste0(repo_dir, "plot_mr_brt_function.R"))
source(paste0(repo_dir, "load_mr_brt_outputs_function.R"))
source(paste0(repo_dir, "predict_mr_brt_function.R"))
source(paste0(repo_dir, "check_for_preds_function.R"))
source(paste0(repo_dir, "load_mr_brt_preds_function.R"))

helper_dir <- # filepath
source(paste0(helper_dir, "helper_functions.R"))


# SET OBJECTS -------------------------------------------------------------
version_id <- 19067 # CF2 clinical, claims, and lit for step2
ds <- 'step2'
# Specify where to save xwalk versions and MR-BRT outputs
dem_dir <- # filepath
mrbrt_dir <- # filepath
model_objects_dir <- # filepath
ref_name <- c("cv_inpatient")
alt_names <- c("cv_marketscan_data", "cv_marketscan_inp_2000", "cv_surveillance")
# Specify model names
date <- gsub("-", "_", Sys.Date())
sex_model_name <- paste0(date, "_encephalitis_sexsplit_CF2")
xwalk_model_name <- paste0(date, "_encephalitis_xwalk_CF2")
age_model_name <- paste0(date, "_encephalitis_agesplit_CF2")

# SEX SPLIT ---------------------------------------------------------------
source() # filepath
# functions used here can be found in helper_functions.R
# sex split function can be found in bundle_sex_split.R

# pull bundle version
dem_sex_dt <- get_dem_data()

# assign study covariates
dem_sex_dt <- get_study_cov(dem_sex_dt)

# outlier clinical data where mean = 0
dem_sex_dt <- outlier_clinical_data(dem_sex_dt)

# sex split data and save
dem_sex_final_list <- run_sex_split(dem_sex_dt, sex_model_name)
ratio <- dem_sex_final_list$ratio
sex_model <- dem_sex_final_list$model
dem_sex_final_dt <- dem_sex_final_list$data
fwrite(dem_sex_final_dt, paste0(dem_dir, sex_model_name, ".csv"))