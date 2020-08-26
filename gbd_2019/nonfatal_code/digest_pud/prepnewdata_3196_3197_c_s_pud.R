## Complicated PUD; Severe, acute, uncomplicated PUD; Complicated gastritis/duodenitis; Severe, acute, uncomplicated gastritis/duodenitis
## Preparation of new data for GBD 2019

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

pacman::p_load(data.table, ggplot2, openxlsx, readxl, readr, RMySQL, stringr, tidyr, plyr, dplyr, mvtnorm)
# install.packages("msm", lib = my.lib)
library("msm", lib.loc = my.lib)

## Source central functions
source(paste0(central_fxn, "get_age_metadata.R"))
source(paste0(central_fxn, "get_location_metadata.R"))
source(paste0(central_fxn, "save_bundle_version.R"))
source(paste0(central_fxn, "get_bundle_version.R"))
source(paste0(central_fxn, "save_crosswalk_version.R"))
source(paste0(central_fxn, "get_crosswalk_version.R"))
source(paste0(central_fxn, "get_bundle_data.R"))
source(paste0(central_fxn, "upload_bundle_data.R"))
source(paste0(central_fxn, "save_bulk_outlier.R"))

## Source other functions
source(paste0(h, "code/getrawdata.R"))
source(paste0(h, "code/sexratio.R"))
source(paste0(h, "code/datascatters.R"))
source(paste0(h, "code/samplematching_wageaggregation.R"))
# source(paste0(h, "code/prepmatchesforMRBRT.R"))
source(paste0(j, "FILEPATH/mr_brt_functions.R"))
source(paste0(h, "code/applycrosswalks.R"))
source(paste0(h, "code/outlierbyMAD.R"))
source(paste0(h, "code/update_seq.R"))

loc_dt <- get_location_metadata(location_set_id = 22)

all_fine_ages <- as.data.table(get_age_metadata(age_group_set_id=12))
not_babies <- all_fine_ages[!age_group_id %in% c(2:4)]
not_babies[, age_end := age_group_years_end-1]
all_fine_babies <- as.data.table(get_age_metadata(age_group_set_id=18))
group_babies <- all_fine_babies[age_group_id %in% c(28)]

age_dt <- rbind(not_babies, group_babies, fill=TRUE)
age_dt[, age_start := age_group_years_start]

age_dt[age_group_id==28, age_end := 0.999]

## Get Step 4 data (all clinical informatics)
save_bundle_version(3196, "step4", include_clinical = TRUE)
step4_3196_bv <- get_bundle_version(16427, export = TRUE)
bundle_columns_3196 <- names(step4_3196_bv)

save_bundle_version(3197, "step4", include_clinical = TRUE)
step4_3197_bv <- get_bundle_version(16430, export = TRUE)
bundle_columns_3197 <- names(step4_3197_bv)

save_bundle_version(3200, "step4", include_clinical = TRUE)
step4_3200_bv <- get_bundle_version(16433, export = TRUE)
bundle_columns_3200 <- names(step4_3200_bv)

save_bundle_version(3201, "step4", include_clinical = TRUE)
step4_3201_bv <- get_bundle_version(16436, export = TRUE)
bundle_columns_3201 <- names(step4_3201_bv)

## Pathways for recreating crosswalk model objects
xw_model_3196 <- paste0(j, "FILEPATH/comppud_xwmodel_2019_07_26/")
xw_model_3197 <- paste0(j, "FILEPATH/apud_xwmodel_2019_07_25/")
xw_model_3200 <- paste0(j, "FILEPATH/compgd_xwmodel_2019_07_01/")
xw_model_3201 <- paste0(j, "FILEPATH/agd_xwmodel_2019_07_01/")

# ## Process bundle 3196 (Complicated PUD)
out_path <- paste0(j, "FILEPATH_OUT")

# Make cvs for clinical informatics data
step4_3196_dt <- market_scan_cv_labels(step4_3196_bv)

# Make definitions for visualizations and vetting
# List of cvs that are useful tags for manipulating data, but not bias variables to crosswalk
cv_manip <- c("cv_marketscan_data", "cv_literature")
# List of cvs that positively identify reference data
cv_ref <- c("cv_admin")
# Combined list of cvs to drop in making definitions
cv_drop <- c(cv_manip, cv_ref)
# List of cvs that positively identify components of alternative case defs
cv_alts <- c("cv_marketscan_2000", "cv_marketscan_other", "cv_diag_exam")

step4_3196_dt <- get_definitions(step4_3196_dt)
table(step4_3196_dt$definition, useNA = "always")

# Fill out cases, sample size, standard error using Epi Uploader formulae; standardize the naming of case definitions; add IDs for higher-order geography
get_cases_sample_size(step4_3196_dt)
get_se(step4_3196_dt)
step4_3196_dt <- merge(step4_3196_dt, loc_dt[ , c("location_id", "super_region_id", "super_region_name", "region_id")], by = "location_id")

ggplot(step4_3196_dt, aes(age_start, mean, color = sex)) + geom_point()
ggsave(paste0(out_path, "/step4_3196_dt_", date, ".pdf"), width=10, height=7, limitsize=FALSE)

## Apply crosswalk coefficient
# Drop CV for which we couldn't get enough matches to crosswalk
cv_nomatch <- c("cv_diag_exam")
cv_alts <- setdiff(cv_alts, cv_nomatch)

# Get modeling data requiring crosswalk for non-reference case definition
step4_3196_nonref <- step4_3196_dt[definition!="reference", ]

## Save reference modeling data to bind back later
step4_3196_ref <- step4_3196_dt[definition=="reference", ]

## Predict crosswalks for non-reference modeling data
prediction_3196 <- unique(predict_xw(choice_fit = NULL, lhs = "logit_dif", raw_data = step4_3196_nonref, old_model_directory = xw_model_3196), by = cv_alts)

## Transform nonreference modeling data
step4_3196_nonref_x <- transform_altdt(step4_3196_nonref, prediction_3196, "logit_dif")

## Visualize effect
pre_post_dt <- merge(step4_3196_nonref, step4_3196_nonref_x, by = c("seq", "age_start", "sex"))
mean_v_mean(pre_post_dt, "xw_mod_dt", xlim=0.05)

## Update seqs in transformed observations, bind back to reference modeling data
step4_3196_nonref_x_seqsu <- update_seq(step4_3196_nonref_x)
step4_3196_dt_2 <- rbind(step4_3196_ref, step4_3196_nonref_x_seqsu, use.names = TRUE, fill = TRUE)

step4_3196_dt_2 <- step4_3196_nonref_x_seqsu[ , c(bundle_columns_3196, "crosswalk_parent_seq"), with = FALSE]

write.xlsx(step4_3196_dt_2, paste0(out_path, "/3196_step4_xw_applied_", date, ".xlsx"), col.names=TRUE)

################## TIDY COLUMNS, MARK OUTLIERS AND SAVE CROSSWALK VERSIONS
step4_3196_dt_3 <- step4_3196_dt_2[ , cv_admin_data:=cv_admin]
step4_3196_dt_3 <- step4_3196_dt_3[ , c("cv_admin", "cv_marketscan_2000", "cv_marketscan_other"):=NULL]

#Write the data to an Excel file in order to upload it
upload_path <- paste0(j, "FILEPATH/step4_xw_noout_", date, ".xlsx")
write.xlsx(step4_3196_dt_3, upload_path, col.names=TRUE, sheetName = "extraction")

#Then add a description and upload
description <- "Crosswalked new clinical informatics data, but not changed any outlier statuses"
step4_3196_dt_3 <- save_crosswalk_version(bundle_version_id=16427, data_filepath=upload_path, description = description)
#Crosswalk version ID 12716

#Confirm this has all the step 2 and step 4, then re-do outliers
step4_3196_xw0 <- get_crosswalk_version(12716, export = TRUE)

#Get the full step 2 and step 4 crosswalks and append for MAD outliering
step4_3196_xw1 <- auto_outlier(step4_3196_xw0)
scatter_markout(step4_3196_xw1, upper = 0.1)
table(step4_3196_xw1$is_outlier, step4_3196_xw1$cv_literature, useNA = "always")

#Lit data with non-std age-groups don't get included in MAD processing (but don't get dropped), so will see if they need to be outliered manually
  dt <- merge(step4_3196_xw1, age_dt, by= "age_start")
  dt <- dt[ , age_group:=as.factor(age_group_id)]
  dt <- dt[age_group_id==28, age_group:="<1"]

  ggplot(dt, aes(age_group, mean, color = factor(cv_literature))) +
    geom_point() +
    ylim(0, 0.1)
  ggsave(paste0(out_path, "/scatter_literature_all_", date, ".pdf"), width=10, height=7)

  dt <- merge(step4_3196_xw1[cv_literature==1 | is_outlier==1, ], age_dt, by= "age_start")
  dt <- dt[ , age_group:=as.factor(age_group_id)]
  dt <- dt[age_group_id==28, age_group:="<1"]

  ggplot(dt, aes(age_group, mean, color = factor(cv_literature))) +
    geom_point() +
    ylim(0, 0.1)
  ggsave(paste0(out_path, "/scatter_lit_v_outliers", date, ".pdf"), width=10, height=7)

  dt <- merge(step4_3196_xw1[cv_literature==1 | is_outlier==0, ], age_dt, by= "age_start")
  dt <- dt[ , age_group:=as.factor(age_group_id)]
  dt <- dt[age_group_id==28, age_group:="<1"]

  ggplot(dt, aes(age_group, mean, color = factor(cv_literature))) +
    geom_point() +
    ylim(0, 0.1)
  ggsave(paste0(out_path, "/scatter_lit_v_nonout", date, ".pdf"), width=10, height=7)
#Retain all
 
step4_3196_xw1 <- step4_3196_xw1[ , c("seq", "is_outlier"), with = FALSE]
upload_path <- paste0(j, "FILEPATH/step4_xw_2MAD_2col_", date, ".xlsx")
write.xlsx(step4_3196_xw1, upload_path, col.names=TRUE, sheetName = "extraction")

save_bulk_outlier(
    crosswalk_version_id=12716,
    decomp_step="step4",
    filepath=upload_path,
    description=description)
#Crosswalk version ID 12806

step4_3196_xw2 <- auto_outlier(step4_3196_xw0, numb_mad = 1.8)
scatter_markout(step4_3196_xw2, upper = 0.1)
table(step4_3196_xw2$super_region_name, step4_3196_xw2$is_outlier)

## Process bundle 3197 (Severe, acute, uncomplicated PUD)
out_path <- paste0(j, "FILEPATH_OUT")

# Make cvs for clinical informatics data
step4_3197_dt <- market_scan_cv_labels(step4_3197_bv)

# Make definitions for visualizations and vetting
# List of cvs that are useful tags for manipulating data, but not bias variables to crosswalk
cv_manip <- c("cv_marketscan_data")
# List of cvs that positively identify reference data
cv_ref <- c("cv_admin")
# Combined list of cvs to drop in making definitions
cv_drop <- c(cv_manip, cv_ref)
# List of cvs that positively identify components of alternative case defs
cv_alts <- c("cv_marketscan_2000", "cv_marketscan_other")

step4_3197_dt <- get_definitions(step4_3197_dt)
table(step4_3197_dt$definition, useNA = "always")

# Fill out cases, sample size, standard error using Epi Uploader formulae; standardize the naming of case definitions; add IDs for higher-order geography
get_cases_sample_size(step4_3197_dt)
get_se(step4_3197_dt)
step4_3197_dt <- merge(step4_3197_dt, loc_dt[ , c("location_id", "super_region_id", "super_region_name", "region_id")], by = "location_id")

ggplot(step4_3197_dt, aes(age_start, mean, color = sex)) + geom_point()
ggsave(paste0(out_path, "/step4_3197_dt_", date, ".pdf"), width=10, height=7, limitsize=FALSE)

## Apply crosswalk coefficient
 # Get modeling data requiring crosswalk for non-reference case definition
step4_3197_nonref <- step4_3197_dt[definition!="reference", ]

## Save reference modeling data to bind back later
step4_3197_ref <- step4_3197_dt[definition=="reference", ]

## Predict crosswalks for non-reference modeling data
prediction_3197 <- unique(predict_xw(choice_fit = NULL, lhs = "logit_dif", raw_data = step4_3197_nonref, old_model_directory = xw_model_3197), by = cv_alts)

## Transform nonreference modeling data
step4_3197_nonref_x <- transform_altdt(step4_3197_nonref, prediction_3197, "logit_dif")

## Visualize effect
pre_post_dt <- merge(step4_3197_nonref, step4_3197_nonref_x, by = c("seq", "age_start", "sex"))
mean_v_mean(pre_post_dt, "xw_mod_dt", xlim=0.0035)

## Update seqs in transformed observations, bind back to reference modeling data
step4_3197_nonref_x_seqsu <- update_seq(step4_3197_nonref_x)
step4_3197_dt_2 <- rbind(step4_3197_ref, step4_3197_nonref_x_seqsu, use.names = TRUE, fill = TRUE)

step4_3197_dt_2 <- step4_3197_nonref_x_seqsu[ , c(bundle_columns_3197, "crosswalk_parent_seq"), with = FALSE]

write.xlsx(step4_3197_dt_2, paste0(out_path, "/3197_step4_xw_applied_", date, ".xlsx"), col.names=TRUE)

################## TIDY COLUMNS, MARK OUTLIERS AND SAVE CROSSWALK VERSIONS
step4_3197_dt_3 <- step4_3197_dt_2[ , cv_admin_data:=cv_admin]
step4_3197_dt_3 <- step4_3197_dt_3[ , c("cv_admin", "cv_marketscan_2000", "cv_marketscan_other"):=NULL]

#Write the data to an Excel file in order to upload it
upload_path <- paste0(j, "FILEPATH/step4_xw_noout_", date, ".xlsx")
write.xlsx(step4_3197_dt_3, upload_path, col.names=TRUE, sheetName = "extraction")

#Then add a description and upload
description <- "Crosswalked new clinical informatics data, but not changed any outlier statuses"
step4_3197_dt_3 <- save_crosswalk_version(bundle_version_id=16430, data_filepath=upload_path, description = description)
#Crosswalk version ID 12854

#Confirm this has all the step 2 and step 4 for me to then re-do outliers
step4_3197_xw0 <- get_crosswalk_version(12854, export = TRUE)

#Get the full step 2 and step 4 crosswalks and append for MAD outliering
step4_3197_xw1 <- auto_outlier(step4_3197_xw0)
scatter_markout(step4_3197_xw1, upper = 0.02)

upload_path <- paste0(j, "FILEPATH/step4_xw_2MAD_", date, ".xlsx")
write.xlsx(step4_3197_xw1, upload_path, col.names=TRUE, sheetName = "extraction")

step4_3197_xw1 <- step4_3197_xw1[ , c("seq", "is_outlier"), with = FALSE]

upload_path <- paste0(j, "FILEPATH/step4_xw_2MAD_2col_", date, ".xlsx")
write.xlsx(step4_3197_xw1, upload_path, col.names=TRUE, sheetName = "extraction")

description <- "Crosswalked new clinical informatics data, applied 2MAD rule"

save_bulk_outlier(
  crosswalk_version_id=12854,
  decomp_step="step4",
  filepath=upload_path,
  description=description)
#Crosswalk version ID 12857

## Process bundle 3200 (Complicated gastritis/duodenitis)
out_path <- paste0(j, "FILEPATH_OUT")

# Make cvs for clinical informatics data
step4_3200_dt <- market_scan_cv_labels(step4_3200_bv)

# Make definitions for visualizations and vetting
# List of cvs that are useful tags for manipulating data, but not bias variables to crosswalk
cv_manip <- c("cv_marketscan_data")
# List of cvs that positively identify reference data
cv_ref <- c("cv_admin")
# Combined list of cvs to drop in making definitions
cv_drop <- c(cv_manip, cv_ref)
# List of cvs that positively identify components of alternative case defs
cv_alts <- c("cv_marketscan_2000", "cv_marketscan_other")

step4_3200_dt <- get_definitions(step4_3200_dt)
table(step4_3200_dt$definition, useNA = "always")

# Fill out cases, sample size, standard error using Epi Uploader formulae; standardize the naming of case definitions; add IDs for higher-order geography
get_cases_sample_size(step4_3200_dt)
get_se(step4_3200_dt)
step4_3200_dt <- merge(step4_3200_dt, loc_dt[ , c("location_id", "super_region_id", "super_region_name", "region_id")], by = "location_id")

ggplot(step4_3200_dt, aes(age_start, mean, color = sex)) + geom_point()
ggsave(paste0(out_path, "/step4_3200_dt_", date, ".pdf"), width=10, height=7, limitsize=FALSE)

## Apply crosswalk coefficient
# Get modeling data requiring crosswalk for non-reference case definition
step4_3200_nonref <- step4_3200_dt[definition!="reference", ]

## Save reference modeling data to bind back later
step4_3200_ref <- step4_3200_dt[definition=="reference", ]

## Predict crosswalks for non-reference modeling data
prediction_3200 <- unique(predict_xw(choice_fit = NULL, lhs = "logit_dif", raw_data = step4_3200_nonref, old_model_directory = xw_model_3200), by = cv_alts)

## Transform nonreference modeling data
step4_3200_nonref_x <- transform_altdt(step4_3200_nonref, prediction_3200, "logit_dif")

## Visualize effect
pre_post_dt <- merge(step4_3200_nonref, step4_3200_nonref_x, by = c("seq", "age_start", "sex"))
mean_v_mean(pre_post_dt, "xw_mod_dt", xlim=0.0035)

## Update seqs in transformed observations, bind back to reference modeling data
step4_3200_nonref_x_seqsu <- update_seq(step4_3200_nonref_x)
step4_3200_dt_2 <- rbind(step4_3200_ref, step4_3200_nonref_x_seqsu, use.names = TRUE, fill = TRUE)

step4_3200_dt_2 <- step4_3200_nonref_x_seqsu[ , c(bundle_columns_3200, "crosswalk_parent_seq"), with = FALSE]

write.xlsx(step4_3200_dt_2, paste0(out_path, "/3200_step4_xw_applied_", date, ".xlsx"), col.names=TRUE)

################## TIDY COLUMNS, MARK OUTLIERS AND SAVE CROSSWALK VERSIONS
step4_3200_dt_3 <- step4_3200_dt_2[ , cv_admin_data:=cv_admin]
step4_3200_dt_3 <- step4_3200_dt_3[ , c("cv_admin", "cv_marketscan_2000", "cv_marketscan_other"):=NULL]

#Write the data to an Excel file in order to upload it
upload_path <- paste0(j, "FILEPATH/step4_xw_noout_", date, ".xlsx")
write.xlsx(step4_3200_dt_3, upload_path, col.names=TRUE, sheetName = "extraction")

#Then add a description and upload
description <- "Crosswalked new clinical informatics data, but not changed any outlier statuses"
step4_3200_dt_3 <- save_crosswalk_version(bundle_version_id=16433, data_filepath=upload_path, description = description)
#Crosswalk version ID 12869

#See if this has all the step 2 and step 4 for me to then re-do outliers
step4_3200_xw0 <- get_crosswalk_version(12869, export = TRUE)

#Get the full step 2 and step 4 crosswalks and append for MAD outliering
step4_3200_xw1 <- auto_outlier(step4_3200_xw0)
scatter_markout(step4_3200_xw1, upper = 0.02)

upload_path <- paste0(j, "FILEPATH/step4_xw_2MAD_", date, ".xlsx")
write.xlsx(step4_3200_xw1, upload_path, col.names=TRUE, sheetName = "extraction")

step4_3200_xw1 <- step4_3200_xw1[ , c("seq", "is_outlier"), with = FALSE]

upload_path <- paste0(j, "FILEPATH/step4_xw_2MAD_2col_", date, ".xlsx")
write.xlsx(step4_3200_xw1, upload_path, col.names=TRUE, sheetName = "extraction")

description <- "Crosswalked new clinical informatics data, applied 2MAD rule"

save_bulk_outlier(
  crosswalk_version_id=12869,
  decomp_step="step4",
  filepath=upload_path,
  description=description)
#Crosswalk version ID 12875

## Process bundle 3201 (Severe, acute, uncomplicated gastritis/duodenitis)
out_path <- paste0(j, "FILEPATH_OUT")

# Make cvs for clinical informatics data
step4_3201_dt <- market_scan_cv_labels(step4_3201_bv)

# Make definitions for visualizations and vetting
# List of cvs that are useful tags for manipulating data, but not bias variables to crosswalk
cv_manip <- c("cv_marketscan_data")
# List of cvs that positively identify reference data
cv_ref <- c("cv_admin")
# Combined list of cvs to drop in making definitions
cv_drop <- c(cv_manip, cv_ref)
# List of cvs that positively identify components of alternative case defs
cv_alts <- c("cv_marketscan_2000", "cv_marketscan_other")

step4_3201_dt <- get_definitions(step4_3201_dt)
table(step4_3201_dt$definition, useNA = "always")

# Fill out cases, sample size, standard error using Epi Uploader formulae; standardize the naming of case definitions; add IDs for higher-order geography
get_cases_sample_size(step4_3201_dt)
get_se(step4_3201_dt)
step4_3201_dt <- merge(step4_3201_dt, loc_dt[ , c("location_id", "super_region_id", "super_region_name", "region_id")], by = "location_id")

ggplot(step4_3201_dt, aes(age_start, mean, color = sex)) + geom_point()
ggsave(paste0(out_path, "/step4_3201_dt_", date, ".pdf"), width=10, height=7, limitsize=FALSE)

## Apply crosswalk coefficient
# Get modeling data requiring crosswalk for non-reference case definition
step4_3201_nonref <- step4_3201_dt[definition!="reference", ]

## Save reference modeling data to bind back later
step4_3201_ref <- step4_3201_dt[definition=="reference", ]

## Predict crosswalks for non-reference modeling data
prediction_3201 <- unique(predict_xw(choice_fit = NULL, lhs = "logit_dif", raw_data = step4_3201_nonref, old_model_directory = xw_model_3201), by = cv_alts)

## Transform nonreference modeling data
step4_3201_nonref_x <- transform_altdt(step4_3201_nonref, prediction_3201, "logit_dif")

## Visualize effect
pre_post_dt <- merge(step4_3201_nonref, step4_3201_nonref_x, by = c("seq", "age_start", "sex"))
mean_v_mean(pre_post_dt, "xw_mod_dt", xlim=0.005)

## Update seqs in transformed observations, bind back to reference modeling data
step4_3201_nonref_x_seqsu <- update_seq(step4_3201_nonref_x)
step4_3201_dt_2 <- rbind(step4_3201_ref, step4_3201_nonref_x_seqsu, use.names = TRUE, fill = TRUE) 

step4_3201_dt_2 <- step4_3201_nonref_x_seqsu[ , c(bundle_columns_3201, "crosswalk_parent_seq"), with = FALSE] 

write.xlsx(step4_3201_dt_2, paste0(out_path, "/3201_step4_xw_applied_", date, ".xlsx"), col.names=TRUE)

################## TIDY COLUMNS, MARK OUTLIERS AND SAVE CROSSWALK VERSIONS
step4_3201_dt_3 <- step4_3201_dt_2[ , cv_admin_data:=cv_admin]
step4_3201_dt_3 <- step4_3201_dt_3[ , c("cv_admin", "cv_marketscan_2000", "cv_marketscan_other"):=NULL]

#Write the data to an Excel file in order to upload it
upload_path <- paste0(j, "FILEPATH/step4_xw_noout_", date, ".xlsx")
write.xlsx(step4_3201_dt_3, upload_path, col.names=TRUE, sheetName = "extraction")

#Then add a description and upload
description <- "Crosswalked new clinical informatics data, but not changed any outlier statuses"
step4_3201_dt_3 <- save_crosswalk_version(bundle_version_id=16436, data_filepath=upload_path, description = description)
#Crosswalk version ID 12878

#Confirm this has all the step 2 and step 4 for me to then re-do outliers
step4_3201_xw0 <- get_crosswalk_version(12878, export = TRUE)

#Get the full step 2 and step 4 crosswalks and append for MAD outliering
step4_3201_xw1 <- auto_outlier(step4_3201_xw0)
scatter_markout(step4_3201_xw1, upper = 0.02)

upload_path <- paste0(j, "FILEPATH/step4_xw_2MAD_", date, ".xlsx")
write.xlsx(step4_3201_xw1, upload_path, col.names=TRUE, sheetName = "extraction")

step4_3201_xw1 <- step4_3201_xw1[ , c("seq", "is_outlier"), with = FALSE]

upload_path <- paste0(j, "FILEPATH/step4_xw_2MAD_2col_", date, ".xlsx")
write.xlsx(step4_3201_xw1, upload_path, col.names=TRUE, sheetName = "extraction")

description <- "Crosswalked new clinical informatics data, applied 2MAD rule"

save_bulk_outlier(
  crosswalk_version_id=12878,
  decomp_step="step4",
  filepath=upload_path,
  description=description)
#Crosswalk version ID 12881

