## Applying GBD 2019 crosswalk coefficients to GBD 2020 data for the Upper digestive causes
## Sections of code that need to be tailored to a specific bundle are marked with *** and require a decision whether to un-comment prior to running, code is otherwise suited to all Upper digestive bundles that use clinical informatics (NOT GERD bundles)

rm(list=ls())

## Set up working environment 
if (Sys.info()["sysname"] == "Linux") {
  j <- "FILEPATH"
  h <- "FILEPATH"
  l <- "/FILEPATH"
} else {
  j <- "FILEPATH"
  h <- "FILEPATH"
  l <- "FILEPATH"
}

my.lib <- paste0(h, "R/")
central_fxn <- paste0(j, "FILEPATH")

date <- gsub("-", "_", Sys.Date())

pacman::p_load(data.table, ggplot2, openxlsx, readxl, readr, RMySQL, stringr, tidyr, plyr, dplyr, mvtnorm, msm)

## Source central functions
source(paste0(central_fxn, "get_ids.R"))
source(paste0(central_fxn, "get_age_metadata.R"))
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
source(paste0(j, "FILEPATH"))
source(paste0(h, "code/applycrosswalks.R"))
source(paste0(h, "code/outlierbyMAD.R"))
source(paste0(h, "code/update_seq.R"))

## Get metadata
all_fine_ages <- as.data.table(get_age_metadata(age_group_set_id=19))
not_babies <- all_fine_ages[!age_group_id %in% c(2, 3, 388, 389)] # drop age-groups under 12mos
not_babies[, age_end := age_group_years_end-1]
not_babies[, age_start := age_group_years_start]

group_babies <- all_fine_ages[age_group_id %in% c(2,3,388,389)]
group_babies$age_group_id <- 28
group_babies$age_end <- 0.999
group_babies$age_start <- 0
group_babies[, age_group_weight_value := sum(age_group_weight_value)]
group_babies <- group_babies[, c("age_start", "age_end", "age_group_id", "age_group_weight_value")]
group_babies <- unique(group_babies)

age_dt <- rbind(not_babies, group_babies, fill=TRUE)

# ## *** Objects to process Total G/D - RUN DATE
# bundle_id <- id
# bversion_id <- bvid
# cv_alts <- c("cv_marketscan_2000", "cv_marketscan_other")
# cv_ref <- c("cv_admin")
# cv_nomatch <- c("cv_serology", "cv_self_report", "cv_endoscopy")
# old_model_summary <- paste0(j, "FILEPATH") #GBD2019 final crosswalk MR-BRT model outputs
# out_path <- paste0(j, "FILEPATH")
# path_xwv_no_outlier <- paste0(out_path, "FILEPATH")
# description_noout <- "GBD2020 1st CI refresh, bv22916, crosswalked, no outliers marked"
# path_xwv_2mad <- paste0(out_path, "FILEPATH")
# description_2mad <- "GBD2020 1st CI refresh, bv22916, crosswalked, 2MAD outliers"
# gbd2019_best_xwv <- xw  # To get modeled EMR
# ## *** Close section

# ## *** Objects to process Complicated G/D - RUN DATE
# bundle_id <- id
# bversion_id <- bvid
# cv_alts <- c("cv_marketscan_2000", "cv_marketscan_other")
# cv_ref <- c("cv_admin")
# cv_nomatch <- NULL # MIGHT NOT BE RELEVANT TO ALL SEQUELAE, WILL HAVE TO EDIT MAIN SCRIPT ACCORDINGLY
# old_model_summary <- paste0(j, "FILEPATH") #GBD2019 final crosswalk MR-BRT model outputs
# out_path <- paste0(j, "FILEPATH")
# path_xwv_no_outlier <- paste0(out_path, "FILEPATH")
# description_noout <- "GBD2020 1st CI refresh, bv22937, crosswalked, no outliers marked"
# path_xwv_2mad <- paste0(out_path, "FILEPATH")
# description_2mad <- "GBD2020 1st CI refresh, bv22937, crosswalked, 2MAD outliers"
# # NEXT OBJECT NOT RELEVANT TO SEQUELAE BUNDLES
# # gbd2019_best_xwv <- # To get modeled EMR
# ## *** Close section

# ## *** Objects to process Acute, uncomplicated G/D - RUN DATE
# bundle_id <- id
# bversion_id <- bvid
# cv_alts <- c("cv_marketscan_2000", "cv_marketscan_other")
# cv_ref <- c("cv_admin")
# cv_nomatch <- NULL
# old_model_summary <- paste0(j, "FILEPATH") #GBD 2019 final crosswalk MR-BRT model outputs
# out_path <- paste0(j, "FILEPATH")
# path_xwv_no_outlier <- paste0(out_path, "FILEPATH")
# description_noout <- "GBD2020 1st CI refresh, bv22940, crosswalked, no outliers marked"
# path_xwv_2mad <- paste0(out_path, "FILEPATH")
# description_2mad <- "GBD2020 1st CI refresh, bv22940, crosswalked, 2MAD outliers"
# # NEXT OBJECT ONLY RELEVANT TO BUNDLES THAT USE EMR
# # gbd2019_best_xwv <- # To get modeled EMR
# ## *** Close section

############# Main script#####################

## Get raw data and create covariates 
bundle_version_dt <- get_bundle_version(bundle_version_id = bversion_id, fetch = "all") 
## Label different groups of clinical informatics data
bundle_version_dt <- label_clinical_info(bundle_version_dt)

## Tag bundle data that don't come from the clinical informatics team with consistently-named study-level covariates
# *** NOT RELEVANT FOR BUNDLES THAT ONLY HAVE CLINICAL INFORMATICS DATA
bundle_version_dt[cv_admin_data==1, cv_admin:=1]
bundle_version_dt[ , cv_admin_data:= NULL]
# bundle_version_dt[is.na(cv_self_report), cv_self_report:=0]
# *** Close section

print(table(bundle_version_dt$cv_marketscan_2000, bundle_version_dt$cv_marketscan_other, useNA = "always"))
print(table(bundle_version_dt$cv_admin, bundle_version_dt$cv_marketscan_data, useNA = "always"))
print(bundle_version_dt[measure == "mtexcess", ]$seq)
# Total gastritis/duodenitis EMR parent row seq = 67954
# Complicated gastritis/duodenitis has no EMR
# Acute, uncomplicated g/d has no EMR

## Store bundle columns for later
bundle_columns <- names(bundle_version_dt)

## *** Keep only reference data and non-reference data that can be crosswalked
# Total gastritis and duodenitis
# usable_bundle_dt <- bundle_version_dt[(cv_literature==0 | is.na(cv_literature)) & (group_review==1 | is.na(group_review)), ]
# Complicated gastritis and duodenitis
# usable_bundle_dt <- copy(bundle_version_dt)
# Acute uncomplicated gastritis and duodenitis
# usable_bundle_dt <- copy(bundle_version_dt)
## *** Close section

## Make definitions for plotting and subsetting
cv_drop <- bundle_columns[grepl("^cv_", bundle_columns) & !bundle_columns %in% cv_alts]
usable_bundle_dt <- get_definitions(usable_bundle_dt)
print(table(usable_bundle_dt$definition, usable_bundle_dt$measure, useNA = "always"))

## Plot all new data without adjustments
scatter_bydef(usable_bundle_dt, upper = 0.02, raw = TRUE) 

## Subset to data to crosswalk and not crosswalk (don't crosswalk the EMR parent row, if present, save its seq for later)
to_crosswalk_dt <- usable_bundle_dt[definition!="reference", ] 
reference_dt <- usable_bundle_dt[definition=="reference" & measure!="mtexcess", ] 
emr_parent_seq <- usable_bundle_dt[measure=="mtexcess", ]$seq

print(table(to_crosswalk_dt$measure, to_crosswalk_dt$definition)) 
print(table(reference_dt$measure, reference_dt$definition))

## Fill out cases, sample size, standard error using Epi Uploader formulae, Update parent seq and seq
get_cases_sample_size(to_crosswalk_dt)
get_se(to_crosswalk_dt)
update_seq(to_crosswalk_dt) 

## Get predicted coefficients with all sources of uncertainty, the predictions for training data are fine since there are no continuous covariates or multi-dimensional case-definitions
new_predicted_xws <- unique(predict_xw(choice_fit = NULL, "logit_dif", to_crosswalk_dt, old_model_summary), by = cv_alts)

## Transform data 
crosswalked_dt <- transform_altdt(to_crosswalk_dt, new_predicted_xws, "logit_dif")
crosswalked_dt[ , `:=` (crosswalk_parent_seq = seq, seq = NA)]
print(table(crosswalked_dt$definition, useNA = "always")) 

## Bind reference data and crosswalked data; make scatter-plot 
modeling_dt <- rbind(crosswalked_dt, reference_dt, fill=TRUE)
scatter_bydef(modeling_dt, upper = 0.02, raw = FALSE) 

## Bind modeled EMR
# *** This section only relevant to bundles that used modeled EMR in GBD 2019
# modeled_emr_dt <- get_crosswalk_version(gbd2019_best_xwv)
# modeled_emr_dt <- modeled_emr_dt[measure=="mtexcess", ]
# modeled_emr_dt <- modeled_emr_dt[!(location_id %in% c(4911, 4912, 4913, 4914, 4915, 4916, 4917, 4918, 4919, 4921, 4922, 4927, 4928)), ]
# modeled_emr_dt <- modeled_emr_dt[ , `:=` (crosswalk_parent_seq = emr_parent_seq, seq = NA)]
# modeling_dt <- rbind(modeling_dt, modeled_emr_dt, fill=TRUE)
# *** Close section

# *** This only applies to a subset of clinical-only bundles
modeling_dt[ , unit_value_as_published:=1]
modeling_dt[ , recall_type:= "Not Set"]
modeling_dt[ , unit_type:= "Person"]
modeling_dt[ , urbanicity_type:="Unknown"]
# *** Close section

# # *** Prevent validation error in 3201
# modeling_dt[ , cases:=as.integer(cases)]
# # *** Close section

modeling_dt[standard_error>1, standard_error := NA]

## Prep and upload transformed data as a crosswalk version for this bundle
columns_keep <- unique(c(bundle_columns, "crosswalk_parent_seq")) 
columns_drop <- c("cv_admin", "cv_marketscan_2000", "cv_marketscan_other")
columns_keep <- setdiff(columns_keep, columns_drop)
xwv_no_outliers <- modeling_dt[, ..columns_keep]

print(table(xwv_no_outliers$is_outlier, useNA = "always"))

## Write full Excel file with new crosswalks and no outliers
write.xlsx(xwv_no_outliers, path_xwv_no_outlier, col.names=TRUE, sheetName = "extraction")
##Then upload
save_xwv_no_outliers <- save_crosswalk_version(bundle_version_id=bversion_id, data_filepath=path_xwv_no_outlier, description = description_noout)
# Total g/d xwv: 20174
# Complicated g/d xwv: xwid
# Acute uncomp g/d xwv: xwid

## Bind reference data and crosswalked data; make scatter-plot 
modeling_dt2 <- rbind(crosswalked_dt, reference_dt, fill=TRUE)
scatter_bydef(modeling_dt2, raw = FALSE, upper = 0.03) 

## Mark outliers by 2MAD algorithm (prevalence only)
xwv_2mad <- auto_outlier(modeling_dt2, numb_mad = 2, standardized = TRUE)
scatter_markout(xwv_2mad, upper = 0.03)

## Bind prevalence data (now with outliers marked) to the modeled EMR data
# *** Only needed for bundles that use modeled EMR
# xwv_2mad <- rbind(xwv_2mad, modeled_emr_dt, fill=TRUE)
# *** Close section

# *** This only applies to a subset of clinical-only bundles
xwv_2mad[ , unit_value_as_published:=1]
xwv_2mad[ , recall_type:= "Not Set"]
xwv_2mad[ , unit_type:= "Person"]
xwv_2mad[ , urbanicity_type:="Unknown"]
# xwv_2mad[ , cases:=as.integer(cases)]
# *** Close section

xwv_2mad[standard_error>1, standard_error := NA]
xwv_2mad <- xwv_2mad[, ..columns_keep]

print(table(xwv_2mad$is_outlier, useNA = "always"))

## Write full Excel file with new crosswalks and 2MAD outliers
write.xlsx(xwv_2mad, path_xwv_2mad, col.names=TRUE, sheetName = "extraction")
##Then upload
save_xwv_2mad <- save_crosswalk_version(bundle_version_id=bversion_id, data_filepath=path_xwv_2mad, description = description_2mad)
# Total g/d xwv: xwid
# Comp g/d xwv: xwid
# Acute uncomp g/d xwv: xwid
################################



