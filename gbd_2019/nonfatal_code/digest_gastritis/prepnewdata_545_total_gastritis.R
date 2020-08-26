## Gastritis and duodenitis; processing data new in GBD 2019

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
central_fxn <- paste0(j, "FILEPATH_CENTRAL_FXNS")

date <- gsub("-", "_", Sys.Date())

pacman::p_load(data.table, ggplot2, openxlsx, readxl, readr, RMySQL, stringr, tidyr, plyr, dplyr, mvtnorm, msm)
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

## Clear Step 4 bundle data
step4_bundle <- get_bundle_data(7001, "step4")
step4_bundle <- step4_bundle[ , "seq"]
write.xlsx(step4_bundle, paste0(j, "FILEPATH/clear_step4_bundle.xlsx"), col.names=TRUE, sheetName = "extraction")
clear <- upload_bundle_data(7001, "step4", paste0(j, "FILEPATH/clear_step4_bundle.xlsx"))
step4_bundle <- get_bundle_data(7001, "step4", export = TRUE)

## Get Step 3 bundle data
step3_bundle <- get_bundle_data(7001, "step3")
# Rename column seq to step3_seq
step3_bundle[ , "step3_seq" := seq]
# Make empty seq column
step3_bundle[ , seq := NA]
# Upload as raw data to Step 4 bundle
write.xlsx(step3_bundle, paste0(j, "FILEPATH/manually_upload_step3_bundle_to_step4.xlsx"), col.names=TRUE, sheetName = "extraction")
upload <- upload_bundle_data(7001, "step4", paste0(j, "FILEPATH/manually_upload_step3_bundle_to_step4.xlsx"))
step4_bundle <- get_bundle_data(7001, "step4", export = TRUE)

## Save a Step 4 bundle version without new clinical data
save_bundle_version(7001, "step4")
step4_bv_only_old_data <- get_bundle_version(14828, export = TRUE)

# Drop all columns except step3_seq and seq, rename step3_parent_seq and step4_parent_seq
old_data_paired_seqs_steps3and4 <- step4_bv_only_old_data[ , c("step3_seq", "seq")]
#setnames(bundle_dt, "seq", "step3_seq")
setnames(old_data_paired_seqs_steps3and4, "step3_seq", "step3_parent_seq")
setnames(old_data_paired_seqs_steps3and4, "seq", "step4_parent_seq")

## Get crosswalk version 9563, subset to data carried over from Step3, rename column crosswalk_parent_seq to step3_parent_seq
step4_crosswalk <- get_crosswalk_version(9563)
step4_crosswalk_olddt <- step4_crosswalk[crosswalk_origin_id==2, ]
range(step4_crosswalk_olddt$crosswalk_parent_seq)
setnames(step4_crosswalk_olddt, "crosswalk_parent_seq", "step3_parent_seq")
step4_crosswalk_olddt <- merge(step4_crosswalk_olddt, old_data_paired_seqs_steps3and4, by = "step3_parent_seq")
# Rename column step4_parent_seq as crosswalk_parent_seq
setnames(step4_crosswalk_olddt, "step4_parent_seq", "crosswalk_parent_seq")
step4_crosswalk_olddt[!is.na(crosswalk_parent_seq), seq := NA]

## Get new Step 4 clinical data from old bundle and upload to Step 4 new bundle, get in a bundle version, subset for crosswalking
add_545_step4_new_clininfo <- upload_bundle_data(7001, "step4", paste0(j, "FILEPATH/step4_GetBundleVersion_bundle_545_request_335345.xlsx"))
save_bundle_version(7001, "step4")
step4_bundleversion_complete <- get_bundle_version(14846, export = TRUE)
head(step4_bundleversion_complete[is.na(step3_seq), ])
tail(step4_bundleversion_complete[is.na(step3_seq), ])
str(step4_bundleversion_complete[is.na(step3_seq), ]) # data.table...7774 obs of 67 variables
# Subset to just the new clinical data
step4_bundleversion_new_only <- step4_bundleversion_complete[is.na(step3_seq), ]

# Label with cv_* for clinical informatics subsets
step4_bundleversion_new_only <- market_scan_cv_labels(step4_bundleversion_new_only)
# Store bundle columns for later
bundle_columns <- names(step4_bundleversion_new_only)

## Apply crosswalk coefficients and update seq/crosswalk_parent_seq
cv_alts <- c("cv_marketscan_other")
old_model_summary <- paste0(j, "FILEPATH/totalgd_xwmodel_2019_07_01/model_summaries.csv")
out_path <- paste0(j, "FILEPATH")
 
## Make definitions for plotting and subsetting
cv_drop <- bundle_columns[grepl("^cv_", bundle_columns) & !bundle_columns %in% cv_alts]
step4_bundleversion_new_only <- get_definitions(step4_bundleversion_new_only)

## Plot all new data without adjustments
scatter_bydef(step4_bundleversion_new_only)
 
## Subset to data to crosswalk and not crosswalk
to_crosswalk_dt <- step4_bundleversion_new_only[definition!="reference", ] 
reference_dt <- step4_bundleversion_new_only[definition=="reference", ] 

## Fill out cases, sample size, standard error using Epi Uploader formulae, Update parent seq and seq
get_cases_sample_size(to_crosswalk_dt)
get_se(to_crosswalk_dt)
update_seq(to_crosswalk_dt) 

## Get predicted coefficients with all sources of uncertainty, the predictions for training data are fine since there are no continuous covariates or multi-dimensional case-definitions
new_predicted_xws <- unique(predict_xw(choice_fit = NULL, "logit_dif", to_crosswalk_dt, old_model_summary), by = cv_alts)

## Transform data 
crosswalked_dt <- transform_altdt(to_crosswalk_dt, new_predicted_xws, "logit_dif")
crosswalked_dt <- crosswalked_dt[standard_error<=1, ]
 
## Bind reference data and crosswalked data; make scatter-plot 
step4_new_for_xwv <- rbind(crosswalked_dt, reference_dt, fill=TRUE)
scatter_bydef(step4_new_for_xwv, raw = FALSE)
 
## Clean up columns on transformed data 
columns_keep <- unique(c(bundle_columns, "crosswalk_parent_seq")) 
columns_drop <- c("cv_admin", "cv_marketscan_2000", "cv_marketscan_other")
columns_keep <- setdiff(columns_keep, columns_drop)
step4_new_for_xwv <- step4_new_for_xwv[, ..columns_keep]

## Bind table from last step to the preceding one
step4_crosswalk <- rbind(step4_crosswalk_olddt, step4_new_for_xwv, fill = TRUE) 
table(step4_crosswalk$measure)

## Apply outlier criteria (make sure only prevalence rows are taken into account)
step4_crosswalk <- step4_crosswalk[ , is_outlier_old := is_outlier]
step4_crosswalk_prev <- step4_crosswalk[measure=="prevalence", ]
step4_crosswalk_prev_out <- auto_outlier(step4_crosswalk_prev)
scatter_markout(step4_crosswalk_prev_out, upper = 0.8) 

step4_crosswalk_outliered <- rbind(step4_crosswalk[measure != "prevalence", ], step4_crosswalk_prev_out, fill = TRUE)

## Save crosswalk version, associated with new Step 4 bundle version with clinical data created above
upload_path <- paste0(j, "FILEPATH/7001_step3and4seqsfixed_2MADonprev_", date, ".xlsx")
write.xlsx(step4_crosswalk_outliered, upload_path, col.names=TRUE, sheetName = "extraction")

##Then add a description and upload
description <- "Step3 best crosswalk with Step4 clinical info xw'd and appended, 2MAD applied to all prev points, seqs and steps fixed from previous"
xwv_upload <- save_crosswalk_version(bundle_version_id=14846, data_filepath=upload_path, description = description)
# xwv 9890

