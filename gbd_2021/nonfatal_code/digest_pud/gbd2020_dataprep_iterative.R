## Applying GBD 2019 crosswalk coefficients to GBD 2020 data for the Upper digestive causes
## Sections of code that need to be tailored to a specific bundle are marked with *** and a decision should be made to un-comment/comment all sections so marked prior to running, code is otherwise suited to all Upper digestive bundles that use clinical informatics (NOT GERD bundles)

rm(list=ls())

## Set up working environment 
if (Sys.info()["sysname"] == "Linux") {
  j <- "FILEPATH"
  h <- "FILEPATH"
  l <- "FILEPATH"
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
source(paste0(j, "FILEPATH/mr_brt_functions.R"))
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

# # *** Objects to process Total PUD - RUN 13-14 May 2020
# bundle_id <- 6998
# bversion_id <- 22913
# cv_alts <- c("cv_marketscan_2000", "cv_marketscan_other", "cv_self_report")
# cv_ref <- c("cv_admin")
# cv_nomatch <- c("cv_endoscopy", "cv_indication")
# old_model_summary <- paste0(j, "FILEPATH/tpud_xwmodel_2019_07_01/")
# out_path <- paste0(j, "FILEPATH0")
# path_xwv_no_outlier <- paste0(out_path, "/gbd2020_tpud_6998_21812_xwgbd2019_nooutliers.xlsx")
# description_noout <- "GBD2020 1st CI refresh, bv21812, crosswalked, no outliers marked"
# path_xwv_2mad <- paste0(out_path, "/gbd2020_tpud_6998_21812_xwgbd2019_2mad.xlsx")
# description_2mad <- "GBD2020 1st CI refresh, bv21812, crosswalked, 2MAD outliers"
# gbd2019_best_xwv <- 12221
# # *** Close section
 
## *** Objects to process Complicated PUD 
bundle_id <- 3196
bversion_id <- 22931
cv_alts <- c("cv_marketscan_2000", "cv_marketscan_other")
cv_ref <- c("cv_admin")
cv_nomatch <- c("cv_diag_exam") # NOT RELEVANT
old_model_summary <- paste0(j, "FILEPATH/comppud_xwmodel_2019_07_26/")
sex_model_summary <- paste0(j, "FILEPATH/sexratio_comppud_2019_07_26/")
out_path <- paste0(j, "FILEPATH")
path_xwv_no_outlier <- paste0(out_path, "/gbd2020_comppud_3196_bv22931_xwgbd2019_nooutliers.xlsx")
description_noout <- "GBD2020 1st CI refresh, bv22931, crosswalked, no outliers marked"
path_xwv_2mad <- paste0(out_path, "/gbd2020_comppud_3196_bv22931_xwgbd2019_2mad.xlsx")
description_2mad <- "GBD2020 1st CI refresh, bv22931, crosswalked, 2MAD outliers"
# gbd2019_best_xwv <- # To get modeled EMR
## *** Close section

# ## *** Objects to process Acute, uncomplicated PUD - RUN 16 MAY 2020
# bundle_id <- 3197
# bversion_id <- 22934
# cv_alts <- c("cv_marketscan_2000", "cv_marketscan_other")
# cv_ref <- c("cv_admin")
# cv_nomatch <- NULL 
# old_model_summary <- paste0(j, "FILEPATH/apud_xwmodel_2019_07_25/")
# out_path <- paste0(j, "FILEPATH")
# path_xwv_no_outlier <- paste0(out_path, "/gbd2020_acutepud_3197_bv22934_xwgbd2019_nooutliers.xlsx")
# description_noout <- "GBD2020 1st CI refresh, bv22934, crosswalked, no outliers marked"
# path_xwv_2mad <- paste0(out_path, "/gbd2020_comppud_3197_bv22934_xwgbd2019_2mad.xlsx")
# description_2mad <- "GBD2020 1st CI refresh, bv22934, crosswalked, 2MAD outliers"
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
# Total PUD EMR parent row seq = 86468
# Complicated PUD has no EMR
# Acute, uncomplicated PUD has no EMR

## Store bundle columns for later
bundle_columns <- names(bundle_version_dt)

# Complicated PUD
# *** Getting to the data set to be processed is a bit more complicated for this bundle, need to combine rows of data reported sub-types of ulcers and complications
usable_bundle_dt <- bundle_version_dt[cv_diag_exam==0 | is.na(cv_diag_exam), ]
print(table(usable_bundle_dt$cv_admin, usable_bundle_dt$cv_marketscan_data, useNA = "always"))

ggplot(usable_bundle_dt, aes(age_start, mean, color = sex)) + geom_point()
ggsave(paste0(out_path, "/comppud_usabledt_", date, ".pdf"), width=10, height=7, limitsize=FALSE)
count(usable_bundle_dt[is.na(mean), ])
head(usable_bundle_dt[is.na(mean), ])

usable_nogrp <- usable_bundle_dt[!(nid %in% c(120530, 214642)), ]
comppud_120530 <- usable_bundle_dt[nid==120530, ]
comppud_214642 <- usable_bundle_dt[nid==214642, ]

## Process groups 
comppud_120530 <- comppud_120530[group_review==1, ]
comppud_120530[ , c("mean", "lower", "upper", "standard_error", "uncertainty_type_value"):=NA]
by_vars <- c("sex", "year_start", "year_end", "age_start", "age_end", "group")
comppud_120530[ , new_cases := sum(cases), by=by_vars]
head(comppud_120530)
tail(comppud_120530)
comppud_120530 <- comppud_120530[ , .SD[c(1)], by=by_vars]
comppud_120530[ , cases := new_cases]
comppud_120530[ , `:=` (new_cases = NULL, case_definition = NA, specificity = "sum", case_name = "sum of hemorrhage, perf, duodenal, gastric", input_type = "split")]
comppud_120530 <- update_seq(comppud_120530)
head(comppud_120530)
tail(comppud_120530)

head(comppud_214642)

bleedspec_214642 <- comppud_214642[specificity == "sex_age_complicationtype", ]
perfspec_214642 <- copy(bleedspec_214642)
allperf_214642 <- comppud_214642[case_name=="Perforated ulcer", c("nid", "location_id", "year_start", "year_end", "measure", "cases", "sample_size", "case_name", "case_definition", "group")]

perfspec_214642[ , cases_total := sum(cases), by = "group"]
perfspec_214642[ , prop_cases := round(cases / cases_total, digits = 3)]

perfspec_214642[ , samp_total := sum(sample_size), by = "group"]
perfspec_214642[ , prop_samp := round(sample_size / samp_total, digits = 3)]

perfspec_214642[ , c("mean", "lower", "upper", "standard_error", "uncertainty_type_value", "table_num", "page_num", "effective_sample_size"):=NA]
perfspec_214642[ , `:=` (cases_total = NULL, samp_total = NULL, cases = NULL, sample_size = NULL, case_name = NULL, case_definition = NULL, specificity = "sex_age_complicationtype", sex_issue = 1, age_issue=1)]

merge_by <- c("nid", "location_id", "year_start", "year_end", "group", "measure")
perfspec_214642 <- merge(perfspec_214642, allperf_214642, by = merge_by, all.x = TRUE)

perfspec_214642[ , `:=` (cases = cases*prop_cases, sample_size = sample_size*prop_samp)]
perfspec_214642 <- perfspec_214642[ , c("nid", "measure", "year_start", "year_end", "age_start", "age_end", "sex", "location_id", "cases", "sample_size")]

comppud_214642 <- merge(bleedspec_214642, perfspec_214642, by = c("nid", "measure", "year_start", "year_end", "age_start", "age_end", "sex", "location_id"))
comppud_214642 <- comppud_214642[ , `:=` (cases = cases.x + cases.y, sample_size = sample_size.x)]
comppud_214642[ , c("cases.x", "sample_size.x", "cases.y", "sample_size.y"):=NULL]
comppud_214642[ , c("page_num", "table_num", "mean", "lower", "upper", "standard_error", "uncertainty_type_value", "case_definition"):=NA]
comppud_214642[ , c("sex_issue", "age_issue"):=1]
comppud_214642[ , case_name:= "sum bleeding and perforated"]
comppud_214642[ , input_type:= "split"]
comppud_214642 <- update_seq(comppud_214642)

head(comppud_214642)

## Bind processed groups back to main body
usable_bundle_dt <- rbind(usable_nogrp, comppud_120530, use.names=TRUE, fill = TRUE)
usable_bundle_dt <- rbind(usable_bundle_dt, comppud_214642, use.names=TRUE)

dt_viz <- usable_bundle_dt[is.na(mean), mean:=cases/sample_size]
ggplot(dt_viz, aes(age_start, mean, color = sex)) + geom_point()
ggsave(paste0(out_path, "/comppud_usabledt2_", date, ".pdf"), width=10, height=7, limitsize=FALSE)
# *** Close section

## Make definitions for plotting and subsetting
cv_drop <- bundle_columns[grepl("^cv_", bundle_columns) & !bundle_columns %in% cv_alts]
usable_bundle_dt <- get_definitions(usable_bundle_dt)
print(table(usable_bundle_dt$definition, usable_bundle_dt$measure, useNA = "always"))

## Plot all new data without adjustments
scatter_bydef(usable_bundle_dt, upper = 0.02, raw = TRUE) 

## Apply sex-ratio
# Get remaining both-sex modeling data
comppud_bothsxmod <- usable_bundle_dt[sex=="Both", ]

## Save data that don't require this processing to bind back later
comppud_mod_bothsx_dropped <- usable_bundle_dt[sex!="Both", ]

## Make mid-age variable for making visualization and then visualize observations to be processed
comppud_bothsxmod <- comppud_bothsxmod[ , age_mid := (age_end-age_start)/2 + age_start]
print(comppud_bothsxmod[ , c("age_start", "age_end", "age_mid")])

ggplot(comppud_bothsxmod, aes(age_start, mean)) + geom_point()
ggsave(paste0(out_path, "/bothsx_PRE_ratio", date, ".pdf"), width=10, height=7, limitsize=FALSE)

## Estimate sample sex-ratios in modeling data as ppln sex-ratios
comppud_bothsxmod <- get_sex_ppln(comppud_bothsxmod, "iterative", age_dt = age_dt)

## Get estimated sex-ratio from MR-BRT, and transform both-sex prevalence to separate male and female prevalence
## Predict ratios from model for training data
sexratio_new_predictions <- predict_new(sex_fit = NULL, old_model_summary = sex_model_summary, comppud_bothsxmod, by = "none")

comppud_bothsxmod <- transform_bothsexdt(comppud_bothsxmod, sexratio_new_predictions, by = "none") 
table(comppud_bothsxmod$specificity, comppud_bothsxmod$group)

## Visualize transformed observations
ggplot(comppud_bothsxmod, aes(age_start, mean, color = sex)) + geom_point()
ggsave(paste0(out_path, "/bothsx_POST_ratio", date, ".pdf"), width=10, height=7, limitsize=FALSE)

## Update seqs in processed observations, bind back to the modeling data that didn't require this processing step
comppud_bothsxmod_seqsu <- update_seq(comppud_bothsxmod)
usable_bundle_dt <- rbind(comppud_mod_bothsx_dropped, comppud_bothsxmod_seqsu, use.names = TRUE)
## *** Close section 

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
# Total PUD xwv: 20354
# Complicated PUD xwv: 20714
# Acute uncomp PUD xwv: 20618

## Bind reference data and crosswalked data; make scatter-plot 
modeling_dt2 <- rbind(crosswalked_dt, reference_dt, fill=TRUE)
scatter_bydef(modeling_dt2, raw = FALSE, upper = 0.03) 

## Mark outliers by 2MAD algorithm (prevalence only)
xwv_2mad <- auto_outlier(modeling_dt2, numb_mad = 2, standardized = TRUE)
scatter_markout(xwv_2mad, upper = 0.03)
## *** Total PUD only
# xwv_2mad <- xwv_2mad[grepl("Russia Statistical Yearbook", xwv_2mad$field_citation_value), is_outlier:=1]
## *** Close section

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
# Total PUD: 20357
# Complicated PUD xwv: 20717
# Acute uncomp PUD xwv: 20621
################################



