##########################################################################
### Author: [USER]
### Date: Spring 2024
### Project: GBD Nonfatal Estimation
### Purpose: Parkinson's Disease Data Processing Master
##########################################################################

rm(list=ls())

if (Sys.info()["sysname"] == "Linux") {
  j_root <- "FILEPATH" 
  h_root <- "FILEPATH"
  l_root <- "FILEPATH/"
  r_root <- "FILEPATH"
  functions_dir <- "FILEPATH"
} else { 
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
  l_root <- "FILEPATH"
  r_root <- "FILEPATH"
  functions_dir <- "FILEPATH"
}

pacman::p_load(data.table, openxlsx, ggplot2, dplyr, msm, dbplyr, Hmisc, reticulate)
library(mrbrt003, lib.loc = "FILEPATH")

date <- gsub("-", "_", Sys.Date())
#date <- "2019_06_13" ## PULL IN MODELS FROM THIS DATE IF DON'T NEED TO RERUN MODELS


# SET OBJECTS -------------------------------------------------------------
# File paths
repo_dir <- paste0(h_root, "repos/parkinson/data_processing/")
upload_dir <- paste0(j_root, "WORK/12_bundle/neuro_parkinsons/6968/03_review/02_upload/")
functions_dir <- paste0(functions_dir, "current/r/")
prep_dir <- paste0(r_root, "scratch/projects/birds/neuro/parkinson/data_prep_outputs/", date, "/")
if (!file.exists(prep_dir)){dir.create(prep_dir, recursive=T)}

# Specify these each time
cv_drop <- c("cv_Gelb_criteria", "cv_marketscan", "cv_cms", "cv_clinical_records", 
             "cv_self_report", "cv_nursing_homes", "cv_population_representative")

draws <- paste0("draw_", 0:999)
park_id <- ID
release_id <- ID
bv_id <- ID
remove_taiwan <- F
fatal_run <- F #False = not running fatal processing so should outlier relrisk, emr, smr data
have_agesplit_model <- T #False = need to run age split model, T = already have age split model


# SOURCE FUNCTIONS ------------------------------------------------

# Shared functions
functs <- c("get_bundle_data.R", "save_bundle_version.R", "get_bundle_version.R", "get_crosswalk_version.R",
            "save_crosswalk_version.R", "upload_bundle_data.R", "get_location_metadata.R", "get_age_metadata.R",
            "get_draws.R", "get_population.R", "get_ids.R")
invisible(lapply(functs, function(x) source(paste0(functions_dir, x))))

# MR-BRT functions
reticulate::use_python("FILEPATH")
mr <- import("mrtool")
cw <- import("crosswalk")

#Data processing functions
source(paste0(repo_dir, "gbd2023/data_processing_functions.R"))


# GET DATA AND PREP ----------------------------------------------------------------
#Bundle version includes Marketscan, CMS, Taiwan, Poland claims (clinical_data_type = "claims")
#Bundle version includes Mongolia and South Korea claims (clinical_data_type = "claims - flagged")

dt <- get_bundle_version(bundle_version_id = bv_id) 

#Remove Marketscan 2000 and all Marketscan over age 65, confirm no inpatient 
dt <- dt[(!clinical_data_type == "inpatient" & !(clinical_data_type == "claims" & field_citation_value %like% "Truven" & year_start == 2000)) | is.na(clinical_data_type)]
dt <- dt[!(clinical_data_type == "claims" & field_citation_value %like% "Truven" & age_start >= 65), ]

#Remove relative risk, EMR, standardized mortality ratio data unless running fatal processing
if(fatal_run == FALSE){
  dt <- dt[!(measure == "mtstandard" | measure == "mtexcess" | measure == "relrisk" | measure == "mtwith"), ]
}

#Remove group_review = 0 data
dt <- dt[(is.na(group_review) | group_review==1), ]

#Set up cross_walk_parent_seq variable and update specificity variable
dt[, crosswalk_parent_seq := NA]
dt[, crosswalk_parent_seq := as.numeric(crosswalk_parent_seq)]
dt[specificity == "", specificity := NA] ## NEED THIS TO BE NA NOT "" FOR UPLOADER VALIDATIONS

#Remove Taiwan claims data if want to
if(remove_taiwan == TRUE){
dt <- dt[nid != ID, ]
}

#Remove post-2020 claims data accidentally included in release
remove_nids <- c(IDS)
dt <- dt[!(nid %in% remove_nids), ]

#Fix sex in one row of Brazil study miscoded as Male in extraction
dt[nid==ID & origin_seq==870080610, sex :="Female"]

#Outlier inpatient study from Korea
dt[nid==535681, is_outlier := 1]
dt[nid==535681, note_modeler := "Outlier because inpatient database with extrapolated prevalence to Korea population"]

#Convert Stockholm and Stockholm except Sweden data to Sweden
dt[location_name == "Stockholm", location_name := "Sweden"]
dt[location_id == ID, location_id := ID]
dt[location_name == "Sweden except Stockholm", location_name := "Sweden"]
dt[location_id == ID, location_id := ID]

#Correctly specify England regions as needed, switch Sweden subnationals to 
dt[nid == 121486, location_name := "South East England"]
dt[nid == 121486, location_id := ID]
dt[nid == 220158, location_name := "North East England"]
dt[nid == 220158, location_id := ID]
dt[nid == 130110, location_name := "North East England"]
dt[nid == 130110, location_id := ID]
dt[nid == 130098, is_outlier := 1] #Outlier study from Canada because rates are age-adjusted

#Remove studies flagged to exclude from crosswalks because uninterpretable and then make remaining 0
dt <- dt[(is.na(exclude_xwalk) | exclude_xwalk==0), ]
dt[is.na(exclude_xwalk), exclude_xwalk := 0]

#Create covariate for CMS and Marketscan - test use at later point
dt[clinical_data_type == "claims" & field_citation_value %like% "CMS", cv_cms := 1]
dt[clinical_data_type == "claims" & field_citation_value %like% "Truven", cv_marketscan := 1]
dt[is.na(cv_cms), cv_cms := 0]
dt[is.na(cv_marketscan), cv_marketscan := 0]


#Set bundle ID 
dt[, bundle_id := ID]

#Drop unneeded covariates and other columns
dt[, c(cv_drop, "step2_location_year", "step3_seq", "step2_seq", "bundle_id_") := NULL]

#Write bundle version to csv
write.csv(dt, paste0(prep_dir, "prepped_bundle_version_", bv_id, ".csv"), row.names=FALSE)

# GET METADATA ------------------------------------------------------------

loc_dt <- get_location_metadata(location_set_id = ID, release_id = release_id)
age_dt <- as.data.table(get_age_metadata(age_group_set_id = ID, release_id = release_id))
age_dt <- age_dt[age_group_id>=ID & age_group_id!=ID & age_group_id<= ID, ]
age_dt[, age_group_years_end := age_group_years_end - 1]
age_dt[age_group_id == ID, age_group_years_end := 99]

# AGE SEX SPLIT -----------------------------------------------------------

dt_age_sex <- age_sex_split(raw_dt = dt)

# SEX SPLIT ---------------------------------------------------------------
# Save subsetted data to pull into function
dt_for_model <- find_sex_matches_for_sex_split(dt, measure_vars = c("prevalence", "incidence"))
dt_for_model <- dt_for_model[is_outlier!=1, ]
dt_for_model_incidence <- dt_for_model[measure=="incidence", ]
dt_for_model_prevalence <- dt_for_model[measure=="prevalence", ]
write.csv(dt_for_model_incidence, paste0(prep_dir, "data_raw_sex_specific_incidence.csv"), row.names=FALSE)
write.csv(dt_for_model_prevalence, paste0(prep_dir, "data_raw_sex_specific_prevalence.csv"), row.names=FALSE)

dt_for_split <- dt_age_sex[sex=="Both", ]
dt_for_split_prevalence <- dt_age_sex[sex=="Both" & measure=="prevalence", ]
dt_for_split_incidence <- dt_age_sex[sex=="Both" & measure=="incidence", ]
dt_hold <- dt_age_sex[sex!="Both", ]
write.csv(dt_for_split_prevalence, paste0(prep_dir, "data_to_sex_split_prevalence.csv"), row.names=FALSE)
write.csv(dt_for_split_incidence, paste0(prep_dir, "data_to_sex_split_incidence.csv"), row.names=FALSE)

# Source and run sex split
source(paste0(h_root, "FILEPATH"))
sex_split(topic_name = "parkinson", output_dir = prep_dir, bundle_version_id = NULL, data_all_csv = NULL, 
           data_to_split_csv = paste0(prep_dir, "data_to_sex_split_prevalence.csv"), 
           data_raw_sex_specific_csv = paste0(prep_dir, "data_raw_sex_specific_prevalence.csv"), 
           nids_to_drop = NULL, cv_drop = NULL, mrbrt_model = NULL, 
           mrbrt_model_age_cv = FALSE, release_id = 16, measure = "prevalence", vetting_plots = T)

sex_split(topic_name = "parkinson", output_dir = prep_dir, bundle_version_id = NULL, data_all_csv = NULL, 
           data_to_split_csv = paste0(prep_dir, "data_to_sex_split_incidence.csv"), 
           data_raw_sex_specific_csv = paste0(prep_dir, "data_raw_sex_specific_incidence.csv"), 
           nids_to_drop = NULL, cv_drop = NULL, mrbrt_model = NULL, 
           mrbrt_model_age_cv = FALSE, release_id = 16, measure = "incidence", vetting_plots = T)

total_dt_prev <- read.csv(paste0(prep_dir, "sex_split_", date, "_prevalence/sex_split_", date, "_parkinson_post_sex_split_data.csv")) 
unused_dt_prev <- read.csv(paste0(prep_dir, "sex_split_", date, "_prevalence/sex_split_", date, "_parkinson_unused_data.csv"))
total_dt_inc <- read.csv(paste0(prep_dir, "sex_split_", date, "_incidence/sex_split_", date, "_parkinson_post_sex_split_data.csv")) 
unused_dt_inc <- read.csv(paste0(prep_dir, "sex_split_", date, "_incidence/sex_split_", date, "_parkinson_unused_data.csv"))


#Recombine with unused data
final_sex_dt <- plyr::rbind.fill(total_dt_prev, unused_dt_prev, total_dt_inc, unused_dt_inc, dt_hold)
remove_cols <- c("step2_location_year", "step2_seq", "bundle_id_", "imputed_sample_size",
                 "population_both", "population_male", "population_female", "standard_dev", 
                 "sample_size_int", "male_mean", "female_mean", "male_standard_error",
                 "female_standard_error", "split", "drop", "log_mean", "midyear", "midage", "merge")
final_sex_dt[, c(remove_cols)] <- NULL
write.csv(final_sex_dt, paste0(prep_dir, "post_sex_split.csv"), row.names = FALSE)

check_sex <- final_sex_dt[final_sex_dt$sex=="Both" & final_sex_dt$is_outlier!=1, ]

#If do not need crosswalking
xwalked_dt <- copy(final_sex_dt) 


# LOCATION SPLITTING ------------------------------------------------------

## AFTER THIS THE OBJECT (xwalk_split_dt) WILL HAVE ALL DATA INCLUDING LOCATION SPLIT DATA
source(paste0(repo_dir, "gbd2023/location_splitting.R"))

# AGE SPLITTING -----------------------------------------------------------
if(have_agesplit_model==FALSE){
  age_split_dt_no_claims <- xwalk_split_dt[((age_end - age_start) <= 25)] 
  age_split_dt_no_claims <- age_split_dt_no_claims [clinical_data_type != "claims" & clinical_data_type != "claims - flagged", ]
  age_split_dt_no_claims <- age_split_dt_no_claims[!(sex=="Both" & is_outlier==1), ]
  age_split_dt_no_claims[, crosswalk_parent_seq := seq]
  age_split_dt_no_claims[, seq := ""]
  age_split_dt_no_claims[is.na(lower) & is.na(upper), uncertainty_type_value := NA]
  write.xlsx(age_split_dt_no_claims, paste0(prep_dir, "xwalk_for_age_split_model_no_claims.xlsx"), sheetName="extraction")
  xwalk_description <- "Data for Age Splitting Model GBD 2023 without claims - updated"
  save_crosswalk_version(bundle_version_id = bv_id, description = xwalk_description,
                         data_filepath = paste0(prep_dir, "xwalk_for_age_split_model_no_claims.xlsx"))
}

if(have_agesplit_model==FALSE){
  age_split_dt <- xwalk_split_dt[(age_end - age_start) <= 25, ]
  age_split_dt <- age_split_dt[!(sex=="Both" & is_outlier==1), ]
  age_split_dt[, crosswalk_parent_seq := seq]
  age_split_dt[, seq := ""]
  age_split_dt[is.na(lower) & is.na(upper), uncertainty_type_value := NA]
  write.xlsx(age_split_dt, paste0(prep_dir, "xwalk_for_age_split_model_with_claims.xlsx"), sheetName="extraction")
  xwalk_description <- "Data for Age Splitting Model GBD 2023 including claims"
  save_crosswalk_version(bundle_version_id = bv_id, description = xwalk_description,
                         data_filepath = paste0(prep_dir, "xwalk_for_age_split_model_with_claims.xlsx"))
}

if(have_agesplit_model==TRUE){
  preagesplit_dt <- copy(xwalk_split_dt)
  preagesplit_dt <- calculate_cases_fromse(preagesplit_dt)
  preagesplit_dt <- get_cases_sample_size(preagesplit_dt)
  preagesplit_dt <- preagesplit_dt[age_end >= 20, ] #don't retain rows if age_end is less than 20
  preagesplit_dt[age_start < 20, age_start := 20] #SET earliest age for PD as 20 years as per Dismod
  
  source(paste0(repo_dir, "gbd2023/age_split.R"))
  final_split <- age_split(gbd_id = 23890, df = preagesplit_dt, age = age_dt[age_group_years_start>=25, age_group_id], 
                           region_pattern = F, location_pattern_id = 1)
  
  age_split_dt <- final_split[!(sex=="Both" & is_outlier==1), ]
  age_split_dt[, crosswalk_parent_seq := origin_seq]
  age_split_dt[, seq := ""]
  age_split_dt[is.na(group) | is.na(specificity), group_review := NA]
  age_split_dt[standard_error > 1, standard_error := 1]
  age_split_dt[is.na(lower) & is.na(upper), uncertainty_type_value := NA]
  age_split_dt[age_end==124, age_end := 99]
  write.xlsx(age_split_dt, paste0(prep_dir, "postagesplit_add_brazil_study.xlsx"), sheetName = "extraction")
  xwalk_description <- "Post Age Splitting GBD 2023 - added additional Brazil study - include Taiwan claims, exclude post-2020 claims"
  save_crosswalk_version(bundle_version_id = bv_id, description = xwalk_description,
                         data_filepath = paste0(prep_dir, "postagesplit_add_brazil_study.xlsx"))
}



