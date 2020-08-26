## Preparation of new GBD 2019 data for all peptic ulcer disease bundles

rm(list=ls())

## Set up working environment 
if (Sys.info()["sysname"] == "Linux") {
  j <- "FILEPATH_J"
  h <- "FILEPATH_H"
  l <- "FILEPATH_L"
} else {
  j <- "FILEPATH_J"
  h <- "FILEPATH_H"
  l <- "FILEPATH_L"
}

my.lib <- paste0(h, "R/")
central_fxn <- paste0(j, "FILEPATH_CENTRAL_FXN")

date <- gsub("-", "_", Sys.Date())

pacman::p_load(data.table, ggplot2, openxlsx, readxl, readr, RMySQL, stringr, tidyr, plyr, dplyr, mvtnorm)
install.packages("msm", lib = my.lib)
library("msm", lib.loc = my.lib)

## Source central functions
source(paste0(central_fxn, "get_age_metadata.R"))
# source(paste0(central_fxn, "get_location_metadata.R"))
source(paste0(central_fxn, "save_bundle_version.R"))
source(paste0(central_fxn, "get_bundle_version.R"))
source(paste0(central_fxn, "save_crosswalk_version.R"))
source(paste0(central_fxn, "get_crosswalk_version.R"))
source(paste0(central_fxn, "get_bundle_data.R"))
source(paste0(central_fxn, "upload_bundle_data.R"))
source(paste0(central_fxn, "save_bulk_outlier.R"))

## Source other functions
source(paste0(h, "code/getrawdata.R"))
# source(paste0(h, "code/sexratio.R"))
source(paste0(h, "code/datascatters.R"))
source(paste0(h, "code/samplematching_wageaggregation.R"))
# source(paste0(h, "code/prepmatchesforMRBRT.R"))
source(paste0(j, "FILEPATH/mr_brt_functions.R"))
source(paste0(h, "code/applycrosswalks.R"))
source(paste0(h, "code/outlierbyMAD.R"))
source(paste0(h, "code/update_seq.R"))

## Get metadata
all_fine_ages <- as.data.table(get_age_metadata(age_group_set_id=12))
not_babies <- all_fine_ages[!age_group_id %in% c(2:4)]
not_babies[, age_end := age_group_years_end-1]
all_fine_babies <- as.data.table(get_age_metadata(age_group_set_id=18))
group_babies <- all_fine_babies[age_group_id %in% c(28)]

age_dt <- rbind(not_babies, group_babies, fill=TRUE)
age_dt[, age_start := age_group_years_start]

age_dt[age_group_id==28, age_end := 0.999]

## Save a Step 4 bundle version for original bundles (just once) to get the new clinical informatics data
save_bundle_version(543, "step4", include_clinical = TRUE)
get_bundle_version(13442, export = TRUE)
save_bundle_version(3196, "step4", include_clinical = TRUE)
get_bundle_version(13445, export = TRUE)
save_bundle_version(3197, "step4", include_clinical = TRUE)
get_bundle_version(13448, export = TRUE)
# Confirm output in bundle folders 

## TOTAL PEPTIC ULCER DISEASE (NEW ME AND BUNDLE IN STEP3)

## Download bundle data prior to processing
get_bundle_data(6998, "step4", export = TRUE) # returns an empty data-frame
get_bundle_data(6998, "step3", export = TRUE) # returns 61,274 rows

## Move data from bundle 543 to 6998
step4_bundle_version_543 <- get_bundle_version(13442, export = F)
step4_bundle_version_543[ , seq := NA]
step4_bundle_version_543 <- step4_bundle_version_543[standard_error <= 1, ]
write.xlsx(step4_bundle_version_543, paste0(j, "FILEPATH/step4_clinicalinfo_from543_bv13442_stnderr1orless.xlsx"), col.names=TRUE, sheetName = "extraction")
upload_bundle_data(6998, "step4", paste0(j, "FILEPATH/step4_clinicalinfo_from543_bv13442_stnderr1orless.xlsx"))
save_bundle_version(6998, "step4", include_clinical = F)
get_bundle_version(14102, export = TRUE)

cv_alts <- c("cv_marketscan_other")
old_model_summary <- paste0(j, "FILEPATH/tpud_xwmodel_2019_07_01/model_summaries.csv")
out_path <- paste0(j, "FILEPATH_OUT")

version <- 14102
bundle_id <- 6998

## Get raw data and create covariates for marking different groups of clinical informatics data
bundle_version_dt <- get_raw_data(bundle_id, 'step4', bundle_version = version)
print(table(bundle_version_dt$cv_admin, bundle_version_dt$cv_marketscan_data, useNA = "always"))
print(table(bundle_version_dt$cv_marketscan_2000, bundle_version_dt$cv_marketscan_other, useNA = "always"))

## Store bundle columns for later
bundle_columns <- names(bundle_version_dt)

## Make definitions for plotting and subsetting
cv_drop <- bundle_columns[grepl("^cv_", bundle_columns) & !bundle_columns %in% cv_alts]
bundle_version_dt <- get_definitions(bundle_version_dt)

## Plot all new data without adjustments
scatter_bydef(bundle_version_dt)

## Subset to data to crosswalk and not crosswalk
to_crosswalk_dt <- bundle_version_dt[definition!="reference", ]
reference_dt <- bundle_version_dt[definition=="reference", ]

## Fill out cases, sample size, standard error using Epi Uploader formulae, Update parent seq and seq
get_cases_sample_size(to_crosswalk_dt)
get_se(to_crosswalk_dt)
update_seq(to_crosswalk_dt)

## Get predicted coefficients with all sources of uncertainty, the predictions for training data are fine since there are no continuous covariates or multi-dimensional case-definitions
new_predicted_xws <- unique(predict_xw(choice_fit = NULL, "logit_dif", bundle_version_dt, old_model_summary), by = cv_alts)

## Transform data 
crosswalked_dt <- transform_altdt(to_crosswalk_dt, new_predicted_xws, "logit_dif")
crosswalked_dt <- crosswalked_dt[standard_error<=1, ]

## Bind reference data and crosswalked data; make scatter-plot 
modeling_dt <- rbind(crosswalked_dt, reference_dt, fill=TRUE)
scatter_bydef(modeling_dt, raw = FALSE)

## Prep and upload transformed data as a crosswalk version for this bundle
columns_keep <- unique(c(bundle_columns, "crosswalk_parent_seq"))
columns_drop <- c("cv_admin", "cv_marketscan_2000", "cv_marketscan_other")
columns_keep <- setdiff(columns_keep, columns_drop)
xwv_no_outliers <- modeling_dt[, ..columns_keep]

## Write the data to an Excel file in order to upload it  
upload_path <- paste0(j, "FILEPATH/6998_newclininfo_nooutliersmarked_", date, ".xlsx")
write.xlsx(xwv_no_outliers, upload_path, col.names=TRUE, sheetName = "extraction")

## Add a description and upload
description <- "Step4 clinical data have been crosswalked but not had outliers marked, new surveys not yet added"
xwv_no_out_upload <- save_crosswalk_version(bundle_version_id=version, data_filepath=upload_path, description = description)
#xwv 9356

# This produces a xwv that contains ONLY the new step4 clinical data, crosswalked without outlier changes

## Ran EMR_append_as_step4.r script 
# Used with the following objects:
# bundle_id = 6998 #EMR bundle
# path_upload <- 'FILEPATH/2019_step3_to_step4_bundle_data_upload.xlsx' #filepath for uploading to bundle of your cause
# path_save <-  'FILEPATH/2019_step4_xw_step3_best_and_step4_new.xlsx' #location to save flat file for crosswalk version from step3 now appended to step4 crosswalk version
# crosswalk_version_id_step3 <- 8885 #this is your step3 best crosswalk version (with appended EMR)
# crosswalk_version_id_step4 <- 9356 #this is your step4 best crosswalk version (new data)

# Produced
# bundle_version_id 14396
# crosswalk_version_id 9623

## Export the xwv above, and it will export with both the new Step 4 data just crosswalked and the old Step 2/3 crosswalked data it got appended to
full_xwv <- get_crosswalk_version(9623, export = TRUE) # returns 117,202 rows
table(full_xwv$measure)
"crosswalk_origin_id" %in% names(full_xwv)

## Confirm Step 4 bundle data and bundle version have everything from Steps 2, 3 and 4
get_bundle_data(6998, "step4", export = TRUE) # returns 86,468 rows
get_bundle_version(14396, export = TRUE) # returns 86,468

## Identify outliers using full distribution of data in Step 4
full_xwv <- full_xwv[ , is_outlier_old := is_outlier]
full_xwv_reoutliered <- auto_outlier(full_xwv)
table(full_xwv_reoutliered$is_outlier, full_xwv_reoutliered$measure)
scatter_markout(full_xwv_reoutliered, upper = 2)

## Write full Excel file that reflects new outlier statuses
upload_path <- paste0(j, "FILEPATH/6998_xwv9623_2MADreapplied_", date, ".xlsx")
write.xlsx(full_xwv_reoutliered, upload_path, col.names=TRUE, sheetName = "extraction")

## Write Excel file with only seq and new outlier statuses
reoutliered_xwv_2columns <- full_xwv_reoutliered[ , c("seq", "is_outlier")]
upload_path <- paste0(j, "FILEPATH/6998_xwv9623_2MADreapplied_2columns", date, ".xlsx")
write.xlsx(reoutliered_xwv_2columns, upload_path, col.names=TRUE, sheetName = "extraction")

## Apply the new outlier statuses using the central function
description <- "Step4 clin data have been xwd, saved as xwv 9356, appended to xwd data from Step3 best (xwv 8885), saved as xwv 9623, gotten new outlier status using 2MAD rule"
save_bulk_outlier(9623, "step4", upload_path, description)
# xwv 9626
get_crosswalk_version(9626, export = TRUE) # returns 117,202

## Check on presence of surveys from Ubcov project
ubcov_nids <- c(163120, 9999, 125591, 3100, 238008, 218067, 110301, 91569, 42609, 42525, 42517, 42427, 42357, 41635, 41577, 41522, 41257, 41213, 41204, 41195, 41173, 247309, 198145, 124396, 80177, 80263, 80202, 80224, 13373, 80262, 111848, 111854, 126191, 126332, 81162, 95306)
ubcov_present <- ubcov_nids[ubcov_nids %in% full_xwv$nid]
ubcov_absent <- ubcov_nids[!(ubcov_nids %in% full_xwv$nid)]

## Upload newly extracted survey data
upload_bundle_data(6998, "step4", paste0(j, "FILEPATH/decomp4_newsurveys.xlsx"))

## Get new Step 4 bundle version, see if it has only the newly extracted surveys uploaded in line 200, or also all the data uploaded in 87 and in example outside script (154)
save_bundle_version(6998, "step4", include_clinical = FALSE) #all the clinical data was written to 543 and manually uploaded to 6998 in 87 or 154
step4_bv_with_surveys <- get_bundle_version(16010, export = TRUE) #returns 86,914 rows
table(step4_bv_with_surveys$cv_self_report, step4_bv_with_surveys$clinical_data_type, useNA = "always")
print(step4_bv_with_surveys[is.na(cv_self_report) & clinical_data_type=="",]) #modeled EMR row
step4_nids_all <- unique(step4_bv_with_surveys$nid)
 
## Subset to just the new survey data
step4_bv_no_new_surveys <- get_bundle_version(14396, export = FALSE)
step4_nids_no_new_surveys <- unique(step4_bv_no_new_surveys$nid)
nids_new_surveys <- setdiff(step4_nids_all, step4_nids_no_new_surveys)
step4_new_surveys <- step4_bv_with_surveys[nid %in% nids_new_surveys, ]
 
## Create covariates for marking different groups of clinical informatics data
market_scan_cv_labels(step4_new_surveys)
print(table(step4_new_surveys$cv_admin, step4_new_surveys$cv_marketscan_data, useNA = "always"))
print(table(step4_new_surveys$cv_marketscan_2000, step4_new_surveys$cv_marketscan_other, useNA = "always"))

## Store bundle columns for later
bundle_columns <- names(step4_new_surveys)

## Make definitions for plotting and subsetting
cv_alts <- c("cv_self_report")
cv_drop <- bundle_columns[grepl("^cv_", bundle_columns) & !bundle_columns %in% cv_alts]
step4_new_surveys <- get_definitions(step4_new_surveys)

## Plot all new data without adjustments
scatter_bydef(step4_new_surveys, upper = 0.25)

## Subset to data to crosswalk and not crosswalk
to_crosswalk_dt <- step4_new_surveys[definition!="reference", ]
reference_dt <- step4_new_surveys[definition=="reference", ]

## Fill out cases, sample size, standard error using Epi Uploader formulae, Update parent seq and seq
get_cases_sample_size(to_crosswalk_dt)
get_se(to_crosswalk_dt)
update_seq(to_crosswalk_dt)

## Get predicted coefficients with all sources of uncertainty, the predictions for training data are fine since there are no continuous covariates or multi-dimensional case-definitions
new_predicted_xws <- unique(predict_xw(choice_fit = NULL, "logit_dif", bundle_version_dt, old_model_summary), by = cv_alts)

## Transform data
crosswalked_dt <- transform_altdt(to_crosswalk_dt, new_predicted_xws, "logit_dif")
crosswalked_dt <- crosswalked_dt[standard_error<=1, ]

## Bind reference data and crosswalked data; make scatter-plot
modeling_dt <- rbind(crosswalked_dt, reference_dt, fill=TRUE)
scatter_bydef(modeling_dt, raw = FALSE)

## Prep and upload transformed data as a crosswalk version for this bundle
columns_keep <- unique(c(bundle_columns, "crosswalk_parent_seq"))
columns_drop <- c("cv_admin", "cv_marketscan_2000", "cv_marketscan_other")
columns_keep <- setdiff(columns_keep, columns_drop)
xw_new_surveys_no_outliers <- modeling_dt[, ..columns_keep]

# Write the data to an Excel file in order to upload it
upload_path <- paste0(j, "FILEPATH/6998_new_surveys_xwd_no_outliers_", date, ".xlsx")
write.xlsx(xw_new_surveys_no_outliers, upload_path, col.names=TRUE, sheetName = "extraction")

##Then add a description and upload
description <- "Step4 new surveys, crosswalked, no outliers marked, not manually appended to prior xwv"
xw_new_surveys_noout_noprevious <- save_crosswalk_version(bundle_version_id=16010, data_filepath=upload_path, description = description)
# xwv 11807, new surveys only

## Append new crosswalked surveys to last crosswalk version for this step and bundle, clear all outlier statuses, mark outliers, and upload
step4_xwv_no_new_surveys <- get_crosswalk_version(9626)
step4_xwv_just_new_surveys <- get_crosswalk_version(11807)
step4_xwv_all_data <- rbind(step4_xwv_no_new_surveys, step4_xwv_just_new_surveys, fill=TRUE)
step4_xwv_all_data <- step4_xwv_all_data[!is.na(crosswalk_parent_seq), seq:=NA]

step4_xwv_all_prevalence <- step4_xwv_all_data[measure=="prevalence", ]
scatter_markout(step4_xwv_all_prevalence, upper = 0.25)

step4_xwv_all_prevalence <- step4_xwv_all_prevalence[ , is_outlier:=0]

# Made several crosswalks with various outlier-selection schema, only subset of schema tried are shown, so there are gaps in numbering versions
step4_xwv_prev_2mad_std <- auto_outlier(step4_xwv_all_prevalence, numb_mad = 2, standardized = TRUE)
scatter_markout(step4_xwv_prev_2mad_std, upper = 0.25)

step4_xwv_NOT_prevalence <- step4_xwv_all_data[measure!="prevalence", ]

xwv0 <- rbind(step4_xwv_NOT_prevalence, step4_xwv_all_prevalence)
xwv1 <- rbind(step4_xwv_NOT_prevalence, step4_xwv_prev_2mad_std, fill = TRUE)

upload_path0 <- paste0(j, "FILEPATH/6998_all_step4_no_outliers_", date, ".xlsx")
upload_path1 <- paste0(j, "FILEPATH/6998_all_step4_2mad_std_", date, ".xlsx")

write.xlsx(xwv0, upload_path0, col.names=TRUE, sheetName = "extraction")
write.xlsx(xwv1, upload_path1, col.names=TRUE, sheetName = "extraction")

description0 <- "Step 2/3 best data, plus Step 4 clinical info and surveys, crosswalked but no outliers marked"
description1 <- "Step 2/3 best data, plus Step 4 clinical info and surveys, crosswalked, outliers based on age-standardized mean 2MAD"

upload_xw0 <- save_crosswalk_version(bundle_version_id=16010, data_filepath=upload_path0, description = description0) #11858
upload_xw1 <- save_crosswalk_version(bundle_version_id=16010, data_filepath=upload_path1, description = description1) #11897

# Drop Russia claims data (will investigate features of this data set further next round and decide whether or not to include)
xwv0 <- get_crosswalk_version(11858) #xwv with no outliers
xwv1 <- get_crosswalk_version(11897) #xwv with 2MAD outliering on all data

xwv0_prev <- xwv0[measure=="prevalence", ]
xwv0_NOT_prev <- xwv0[measure!="prevalence", ]
xwv1_prev <- xwv1[measure=="prevalence", ]
xwv1_NOT_prev <- xwv1[measure!="prevalence", ]

xwv_prev_no_russian_claims <- xwv1_prev[grepl("Russia Statistical Yearbook", xwv1_prev$field_citation_value), is_outlier:=1]
table(xwv_prev_no_russian_claims[is_outlier==1, ]$location_name)

xwv6 <- rbind(xwv1_NOT_prev, xwv_prev_no_russian_claims, fill=TRUE)
xwv6 <- xwv6[!is.na(crosswalk_parent_seq), seq:=NA]

upload_path6 <- paste0(j, "FILEPATH/6998_all_step4_2mad_noruss_", date, ".xlsx")

write.xlsx(xwv6, upload_path6, col.names=TRUE, sheetName = "extraction")

description6 <- "Step 2/3 best data, plus Step 4 clinical info and surveys, crosswalked, outliers agestd 2MAD and Russian claims"
 
upload_xw6 <- save_crosswalk_version(bundle_version_id=16010, data_filepath=upload_path6, description = description6) #xwv 12221