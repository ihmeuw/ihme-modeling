## Total peptic ulcer disease, GBD 2017 data re-prepared with GBD 2019 data preparation methods

###### SETUP

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

out_path <- paste0(j, "FILEPATH_OUTPUT")

date <- gsub("-", "_", Sys.Date())

pacman::p_load(data.table, ggplot2, openxlsx, readxl, readr, RMySQL, stringr, tidyr, plyr, dplyr, mvtnorm)
#install.packages("metafor", lib = my.lib)
library("metafor", lib.loc = my.lib)
#install.packages("msm", lib = my.lib)
library("msm", lib.loc = my.lib)

## Source central functions
source(paste0(central_fxn, "get_age_metadata.R"))
source(paste0(central_fxn, "get_location_metadata.R"))
source(paste0(central_fxn, "save_bundle_version.R"))
source(paste0(central_fxn, "get_bundle_version.R"))
source(paste0(central_fxn, "save_crosswalk_version.R"))
source(paste0(central_fxn, "get_bundle_data.R"))
source(paste0(central_fxn, "upload_bundle_data.R"))

## Source other functions
source(paste0(h, "code/getrawdata.R"))
#source(paste0(h, "code/sexratio.R"))  # not used for this particular dataset
source(paste0(h, "code/datascatters.R"))
source(paste0(h, "code/samplematching_wageaggregation.R"))
source(paste0(h, "code/prepmatchesforMRBRT.R"))
source(paste0(j, "FILEPATH/mr_brt_functions.R"))
source(paste0(h, "code/applycrosswalks.R"))
source(paste0(h, "code/outlierbyMAD.R"))
source(paste0(h, "code/aggregate_marketscan.r"))

## Empty bundle and upload reviewed/re-extracted data
# upload_bundle_data(543, "step2", paste0(j, "FILEPATH/clearbundle.xlsx"))
# 266186, successful
# upload_bundle_data(543, "step2", paste0(j, "FILEPATH/gbd2019_step2_oldlitandsurvtabsreviewed.xlsx"))
# 266195, successful
# upload_bundle_data(543, "step2", paste0(j, "FILEPATH/gbd2019_step2_reextractfrommicro.xlsx"))
# request_id request_status
# 1:     275015     Successful
# get_bundle_data(543, "step2", export=TRUE)
# request 275285

## Get metadata
loc_dt <- get_location_metadata(location_set_id = 22)

all_fine_ages <- as.data.table(get_age_metadata(age_group_set_id=12))
not_babies <- all_fine_ages[!age_group_id %in% c(2:4)]
not_babies[, age_end := age_group_years_end-1]
all_fine_babies <- as.data.table(get_age_metadata(age_group_set_id=18))
group_babies <- all_fine_babies[age_group_id %in% c(28)]

age_dt <- rbind(not_babies, group_babies, fill=TRUE)  
age_dt[, age_start := age_group_years_start]

age_dt[age_group_id==28, age_end := 0.999]

###### GET AND LABEL INPUT DATA

## Get raw data and create covariates for marking different groups of clinical informatics data
#Use argument bundle_version = 0 if you want to create a new bundle version
#tpud_dt <- get_raw_data(543, 'step2', bundle_version = 0)
#Bundle version ID: 7481
tpud_dt <- get_raw_data(543, 'step2', bundle_version = 7481)

## FYI, there are incidence and prevalence data in the bundle, but very little incidence, ran this whole pipeline without distinguishing, and ultimately, all incidence data used a case definition that had no matches regardless of the measure used

## Store bundle columns for later
bundle_columns <- names(tpud_dt)

## Get data to use in processing that won't join the model until Step4, give same columns as modeling data, and mark it for retrieval
tpud_procdt <- as.data.table(read.xlsx(paste0(j, "FILEPATH/543_decomp2_gbd2019.xlsx"), sheet = "ubcov_new_for_xw", colNames = TRUE))
add_columns <- setdiff(bundle_columns, names(tpud_procdt))
tpud_procdt <- tpud_procdt[ , (add_columns):=NA]
tpud_procdt <- tpud_procdt[ , ..bundle_columns]
tpud_procdt <- tpud_procdt[ , proc_only:=1]

## Bind modeling and processing data into single data table, will separate later
tpud_dt2 <- rbind(tpud_procdt, tpud_dt, use.names=TRUE, fill=TRUE)
## Remove all lifetime recall data; previous analysis saw this was only present in processing bundle, not modeling data (ie every study with lifetime recall data also reported prevalence measured using a more-preferred recall period)
tpud_dt2 <- tpud_dt2[cv_recall_lifetime==0 | is.na(cv_recall_lifetime), ]

## List of cvs that are useful tags for manipulating data, but not actually things I want a separate crosswalk for
cv_manip <- c("cv_marketscan_data", "cv_literature", "cv_survey")
## List of cvs that are marked in the raw data and I looked at possibly crosswalking, but systemic bias not observed and later decided to collapse into an aggregate category
cv_collapse <- c("cv_indication", "cv_mail", "cv_point_prev", "cv_12_month_recall")
## List of cvs that positively identify reference data
cv_ref <- c("cv_admin")
## Combined list of cvs to drop in match-finding (but keep in raw data)
cv_drop <- c(cv_manip, cv_collapse, cv_ref)

## List of cvs that positively identify components of alternative case defs
cv_alts <- c("cv_marketscan_2000", "cv_marketscan_other", "cv_endoscopy",  "cv_self_report")

## Make sure columns for alt case definitions are non-missing for clinical informatics data, otherwise, predict MR-BRT will estimate coefficients correctly but won't know which coefficients to apply to to make adjustments later (these are all complete for data that don't come from clinical informatics)
tpud_dt2[cv_marketscan_data==1 | cv_admin==1, `:=` (cv_endoscopy = 0, cv_self_report = 0)]

## Tag bundle data that *don't* come from the clinical informatics team with consistently-named study-level covariates (name used in analysis and assigned to clin info data upon import is shorter than the equivalent in the lit/survey bundle data)
tpud_dt2[cv_admin_data==1, cv_admin:=1]
tpud_dt2[ , cv_admin_data:= NULL]

## Fill out cases, sample size, standard error using Epi Uploader formulae; standardize the naming of case definitions; add IDs for higher-order geography
get_cases_sample_size(tpud_dt2)
get_se(tpud_dt2)

## Add variables for use in data-prep that will later need to be dropped to pass Uploader validations
tpud_dt2 <- get_definitions(tpud_dt2)
tpud_dt2 <- merge(tpud_dt2, loc_dt[ , c("location_id", "super_region_id", "super_region_name", "region_id")], by = "location_id")

## Get MarketScan subset and aggregate for processing purposes
tpud_mscan <- tpud_dt2[cv_marketscan_data==1, ]
tpud_mscan <- aggregate_marketscan(tpud_mscan)

## Make modeling and processing bundles
tpud_moddt <- tpud_dt2[(group_review==1 | is.na(group_review)) & is.na(proc_only), ]

tpud_procdt2 <- rbind(tpud_dt2, tpud_mscan, use.names=TRUE)

## Where age-specific and sex-specific data reported for the same study, but not age-sex-specific data, split age-specific, both-sex data points using the sex ratio from sex-specific, all-age data points
## Model sex-ratios from true sex-specific measurements in MR-BRT, apply to input data points reported for only for both sexes combined
## Ultimately found all input data for both sexes combined used non-reference case definition for which there were not adequate matched data to develop a crosswalk, so dropped both-sex data and steps for splitting them

tpud_moddt3 <- tpud_moddt[sex!="Both" & (age_end-age_start <= 20 | age_start >= 95), ]

###### ADJUST NON-STANDARD CASE DEFINITIONS (IE CROSSWALK)

## Prep raw data for match-finding, crosswalking and transformation
tpud_findmatch <- copy(tpud_procdt2)
tpud_findmatch <- subnat_to_nat(tpud_findmatch)
tpud_findmatch <- calc_year(tpud_findmatch)

scatter_bydef(tpud_findmatch)
scatter_bydef(tpud_findmatch, upper = 0.25)

## Find matches

age_dts <- get_age_combos(tpud_findmatch)
tpud_findmatch <- age_dts[[1]]
age_match_dt <- age_dts[[2]] 

pairs <- combn(tpud_findmatch[, unique(definition)], 2)
#combn function generates all combinations of the elements of vector x (in this case, a vector of the unique values found in the definition column of the data.table tpud_xw) taken m (in this case, 2) at a time

matches <- rbindlist(lapply(1:dim(pairs)[2], function(x) get_matches(n = x, pair_dt = tpud_findmatch)))

matches[ , nid_comb:=paste0(nid, nid2)]
matches[nid==nid2, cv_intRA:=1]

write.xlsx(matches, paste0(out_path, "/543_matches_", date, ".xlsx"), col.names=TRUE)

#***comment out reading in the data table if you are running full pipeline uninterrupted, bring back in if running MR-BRT on previously prepped data
#ratios <- as.data.table(read_excel(paste0(out_path, "/543_matches_", date, ".xlsx")))

## Calculate ratios and their standard errors, drop unneeded variables, calculate dummies, visualize ratios, take their logs and calc standard error of their logs
ratios <- calc_ratio(matches)
#Drop matches with infinite or zero values for the ratios (from visualization only)
ratios <- drop_stuff(ratios)
ratios <- add_compdumminter(ratios)
histo_ratios(ratios, bins = 20)
lratios <- logtrans_ratios(ratios)

## Calculate logits, logit differences, and standard errors to go into meta-regression
logit_differences <- calc_logitdf(lratios)

## Take list of cvs for components of alternative case definitions, drop the alts that don't have enough direct or indirect matches to reference, and make into a list MR-BRT will understand
cv_nomatch <- c("cv_endoscopy")
cv_alts <- setdiff(cv_alts, cv_nomatch)

for(c in cv_alts){
  cov <- cov_info(c, "X", "network")
  if(c == cv_alts[1]){
    cov_list <- list(cov)
  } else {
    cov_list <- c(cov_list, list(cov))
  }
}

## Ran various MR-BRT models to decide on best fit
#  Final choice of model to carry forward

  tpud_fit <- run_mr_brt(
    output_dir = out_path,
    model_label = paste0("tpud_xwmodel_", date),
    data = logit_differences,
    mean_var = "diff_logit",
    se_var = "se_diff_logit",
    remove_x_intercept = TRUE,
    covs = cov_list,
    method = "trim_maxl",
    overwrite_previous = TRUE,
    trim_pct = 0.1,
    study_id = "nid_comb"
  )
 
## Get predicted coefficients with all sources of uncertainty, predict for training data and then for all data needing crosswalks (these sets of predictions will be the same if there are no continuous covariates or multi-dimensional case-definitions) 

# Keep only the modeling data with reference defs or defs that can be crosswalked, drop the uncrosswalkable
tpud_moddt4 <- tpud_moddt3[definition!="_cv_endoscopy", ]

# Select data that need and can be crosswalked and mark parent seq and seq
tpud_transform <- tpud_moddt4[definition!="reference", ]
if ("crosswalk_parent_seq" %in% names(tpud_transform)) {
  tpud_transform[is.na(crosswalk_parent_seq), `:=` (crosswalk_parent_seq = seq, seq = NA)]
  tpud_transform[!is.na(crosswalk_parent_seq), seq := NA] 
} else {
  tpud_transform[, `:=` (crosswalk_parent_seq = seq, seq = NA)]
}

# Predict crosswalk for training data
tpud_trainingprediction <- unique(predict_xw(tpud_fit, "logit_dif"), by = cv_alts)
# Predict crosswalk for modeling data
tpud_predictnew <- unique(predict_xw(tpud_fit, "logit_dif", tpud_transform), by = cv_alts)

# Transform data that need and can be crosswalked
tpud_transform <- transform_altdt(tpud_transform, tpud_predictnew, "logit_dif")

###### REGROUP AFTER CROSSWALKING

## Drop untransformed data from processing set and bind transformed; make scatter-plot 
tpud_procdt3 <- tpud_procdt2[!(definition %in% c("_cv_marketscan_2000", "_cv_marketscan_other", "_cv_self_report")), ]
tpud_procdt3 <- rbind(tpud_procdt3, tpud_transform, use.names=TRUE, fill=TRUE)
scatter_bydef(tpud_procdt3, raw = FALSE, upper = 0.25)

## Drop untransformed data from modeling set and bind transformed; make scatter-plot
tpud_moddt5 <- tpud_moddt4[!(definition %in% c("_cv_marketscan_2000", "_cv_marketscan_other", "_cv_self_report")),  ]
tpud_moddt5 <- rbind(tpud_moddt5, tpud_transform, use.names=TRUE, fill=TRUE)
scatter_bydef(tpud_moddt5, raw = FALSE, upper=0.25)

###### OUTLIER, FORMAT, UPLOAD

## Prep and upload transformed data as a crosswalk version for this bundle

#Choose how many MAD above and below median to outlier, defaults to 2 if no numb_mad argument supplied, if numb_mad=0 will remove series with age-standardized mean of 0, but nothing else
tpud_outliered <- auto_outlier(tpud_moddt5)
scatter_markout(tpud_outliered)

#Keep the original bundle columns, plus the new crosswalk_parent_seq column, rename or drop some of the columns that were created with first function to get data
setnames(tpud_outliered, old="cv_admin", new="cv_admin_data")

columns_keep <- unique(c(bundle_columns, "crosswalk_parent_seq")) 
dont_keep <- c("cv_admin", "cv_marketscan_2000", "cv_marketscan_other")
columns_keep <- setdiff(columns_keep, dont_keep)

tpud_final <- tpud_outliered[, ..columns_keep]

#You have to write the data to an Excel file in order to upload it  :(
upload_path <- paste0(j, "FILEPATH/543_", date, ".xlsx")
write.xlsx(tpud_final, upload_path, col.names=TRUE, sheetName = "extraction")

#Then add a description and upload
description <- "Same approach as XWV 2861, but fixed MAD outliering code "
tpud_upload_xw <- save_crosswalk_version(bundle_version_id=7481, data_filepath=upload_path, description = description)
#Crosswalk version ID 3116
