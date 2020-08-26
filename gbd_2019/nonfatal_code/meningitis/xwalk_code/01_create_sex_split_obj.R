#' @author 
#' @date 2019/03/01
#' @description Main script for GBD 2019 decomp 3 meningitis sex-splitting,
#'              crosswalking, and age splitting. 
#'              Creates sex-split and crosswalk model objects.
#'              Sex-split: Used standard approach using MR-BRT model informed by
#'                         Female:Male incidence rate to split numerator and 
#'                         population to split denominator
#'              Crosswalk:
#'              Reference: inpatient data with correction factor 3
#'              Alternatives: Marketscan 2000 claims, 
#'                            All other claims data (coded as cv_marketscan_data),
#'                            Population surveillance data
#'              MR-BRT model used logit difference (logit(alt) - logit(ref)) and
#'              used Delta method to calculate standard error in logit space
#'              Age-split: Used clinical data global age pattern to 
#'                         split numerator and population to split denominator 
#'                         for rows with age ranges greater than 25 years

rm(list=ls())

pacman::p_load(openxlsx, pbapply, ggplot2)

# SOURCE FUNCTIONS --------------------------------------------------------

# SET OBJECTS -------------------------------------------------------------
version_id <- 18908 # decomp step2 lit and clinical, NO bad NIDs
ds <- 'step2'

# Specify where to save xwalk versions and MR-BRT outputs
dem_dir   <- # filepath
mrbrt_dir <- # filepath
model_objects_dir <- # filepath
  
# Specify the column names for the reference and alternative study covariates
ref_name  <- c("cv_inpatient")
alt_names <- c("cv_population_surveillance", "cv_marketscan_data", 
               "cv_marketscan_inp_2000")

# Specify model names to uses a prefixes for the MR-BRT output directories
date <- gsub("-", "_", Sys.Date())
sex_model_name   <- paste0(date, "_meningitis_sexsplit")
xwalk_model_name <- paste0(date, "_meningitis_xwalk_")
age_model_name   <- paste0(date, "_meningitis_agesplit")

# specify filepaths for additional data used for crosswalks
data_dir <- # filepath
sinan_filepath <- # filepath
extraction_filepath <- # filepath
imd_pop_filepath <-  # filepath
imd_sentinel_filepath <- # filepath

# SEX SPLIT ---------------------------------------------------------------
source ()# filepath
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
dem_sex_final_dt <- dem_sex_final_list$data
fwrite(dem_sex_final_dt, paste0(dem_dir, sex_model_name, ".csv"))

# get additional surveillance data to sex split using the MR-BRT model informed by current bundle version data
additional_dem_sex_dt <- get_addiontional_sexsplit_data()
additional_dem_sex_dt <- get_se(additional_dem_sex_dt)
additional_dem_sex_final_list <- split_data(additional_dem_sex_dt, dem_sex_final_list$model)
additional_dem_sex_final_dt <- rbind(dem_sex_final_dt, additional_dem_sex_final_list$final, fill = T)
fwrite(additional_dem_sex_final_dt, paste0(dem_dir, sex_model_name, "_with_additional.csv"))