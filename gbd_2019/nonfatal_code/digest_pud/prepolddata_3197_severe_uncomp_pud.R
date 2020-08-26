## Severe, acute, uncomplicated PUD; Re-preparing GBD 2017 data with GBD 2019 methods

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

out_path <- paste0(j, "FILEPATH_OUT")

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
source(paste0(h, "code/sexratio.R"))
source(paste0(h, "code/datascatters.R"))
source(paste0(h, "code/samplematching_wageaggregation.R"))
source(paste0(h, "code/prepmatchesforMRBRT.R"))
source(paste0(j, "FILEPATH/mr_brt_functions.R"))
source(paste0(h, "code/applycrosswalks.R"))
source(paste0(h, "code/outlierbyMAD.R"))

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

## Get raw data and create covariates for marking different groups of clinical informatics data, subset to the measure being crosswalked
#Use argument bundle_version = 0 if you want to create a new bundle version
apud_dt <- get_raw_data(3197, 'step2', bundle_version = 0)
#Bundle version ID: 7967
apud_bundle <- get_raw_data(3197, 'step2', bundle_version = 7967)

## Add some of the columns that later functions expect but this bundle version does not have for lack of manually extracted data
apud_dt <- apud_bundle[ , c("case_name", "note_modeler"):=NA]

## List of cvs that are useful tags for manipulating data, but not actually things I want a separate crosswalk for
cv_manip <- c("cv_marketscan_data")
## List of cvs that positively identify reference data
cv_ref <- c("cv_admin")
## Combined list of cvs to drop in match-finding (but keep in raw data)
cv_drop <- c(cv_manip, cv_ref)

## List of cvs that positively identify components of alternative case defs
cv_alts <- c("cv_marketscan_2000", "cv_marketscan_other")

## Fill out cases, sample size, standard error using Epi Uploader formulae; standardize the naming of case definitions; add IDs for higher-order geography
get_cases_sample_size(apud_dt)
get_se(apud_dt)

## Add variables for use in data-prep that will later need to be dropped to pass Uploader validations
apud_dt <- get_definitions(apud_dt)
apud_dt <- merge(apud_dt, loc_dt[ , c("location_id", "super_region_id", "region_id")], by = "location_id")

## Prep raw data for match-finding, crosswalking and transformation
apud_findmatch <- subnat_to_nat(apud_dt)
apud_findmatch <- calc_year(apud_findmatch)

scatter_bydef(apud_findmatch, upper = 0.0225)

## Find matches
age_dts <- get_age_combos(apud_findmatch)
apud_findmatch <- age_dts[[1]]
age_match_dt <- age_dts[[2]] 

pairs <- combn(apud_findmatch[, unique(definition)], 2)
#combn function generates all combinations of the elements of vector x (in this case, a vector of the unique values found in the definition column of the data.table apud_findmatch) taken m (in this case, 2) at a time

matches <- rbindlist(lapply(1:dim(pairs)[2], function(x) get_matches(n = x, pair_dt = apud_findmatch)))

matches[ , nid_comb:=paste0(nid, nid2)]

## Write output of match-finding and ratio calculation for human review
write.xlsx(matches, paste0(out_path, "/3197_matches_", date, ".xlsx"), col.names=TRUE)

## Prep and perform meta-regression

#***comment out reading in the data table if you are running full pipeline uninterrupted, bring back in if running MR-BRT on previously prepped data
#ratios <- as.data.table(read_excel(paste0(out_path, "/3197_matches_2019_05_24_decomp2_2cv.xlsx")))

## Calculate ratios and their standard errors, drop unneeded variables, calculate dummies, visualize ratios, take their logs and calc standard error of their logs
#Log-ratios are no longer the preferred input variable for MR-BRT modeling, but still useful for visualizing
ratios <- calc_ratio(matches)
#This line is for dropping matches with infinite or zero values for the ratios (from visualization, not data-set)
ratios <- drop_stuff(ratios)
ratios <- add_compdumminter(ratios)
histo_ratios(ratios, bins = 0.2)
lratios <- logtrans_ratios(ratios)

## Calculate logits, logit differences, and standard errors to go into meta-regression
logit_differences <- calc_logitdf(lratios)

## Take list of cvs for components of alternative case definitions, drop the alts that don't have enough direct or indirect matches to reference, and make into a list MR-BRT will understand
# cv_nomatch <- c(***)
# cv_alts <- setdiff(cv_alts, cv_nomatch)

for(c in cv_alts){
  cov <- cov_info(c, "X", "network")
  if(c == cv_alts[1]){
    cov_list <- list(cov)
  } else {
    cov_list <- c(cov_list, list(cov))
  }
}

## Run various MR-BRT models to decide on best fit
# Final model
apud_fit <- run_mr_brt(
  output_dir = paste0(j, "WORK/12_bundle/digest_pud/3197/01_input_data/00_lit/GBD2019/decomp2_dataprep"),
  model_label = paste0("apud_xwmodel_", date),
  data = logit_differences,
  mean_var = "diff_logit",
  se_var = "se_diff_logit",
  covs = cov_list,
  remove_x_intercept = TRUE,
  method = "remL",
  overwrite_previous = TRUE,
  study_id = "nid_comb"
)

## Get predicted coefficients with all sources of uncertainty, predict for training data and then for all data needing crosswalks (these sets of predictions will be the same if there are no continuous covariates or multi-dimensional case-definitions) 
  
# Select data that need and can be crosswalked and mark parent seq and seq
apud_transform <- apud_dt[definition!="reference", ]
if ("crosswalk_parent_seq" %in% names(apud_transform)) {
  apud_transform[is.na(crosswalk_parent_seq), `:=` (crosswalk_parent_seq = seq, seq = NA)]
  apud_transform[!is.na(crosswalk_parent_seq), seq = NA] 
} else {
  apud_transform[, `:=` (crosswalk_parent_seq = seq, seq = NA)]
}

# Predict crosswalk for training data
apud_trainingprediction <- unique(predict_xw(apud_fit, "logit_dif"), by = cv_alts)
# Predict crosswalk for raw data
apud_predictnew <- unique(predict_xw(apud_fit, "logit_dif", apud_transform), by = cv_alts)

# Transform data that need and can be crosswalked
apud_transform <- transform_altdt(apud_transform, apud_predictnew, "logit_dif")

# Bind reference data and crosswalked data; make scatter-plot; would include un-crosswalkable in this scatter but there are none for this bundle
apud_dt2 <- rbind(apud_transform, apud_dt[definition=="reference", ], fill=TRUE)
scatter_bydef(apud_dt2, raw = FALSE, upper = 0.0225)
# Now drop data that could not be crosswalked, or that were group-reviewed out (none for this bundle so just make a copy for consistent naming)
apud_modeling <- copy(apud_dt2)

## Prep and upload transformed data as a crosswalk version for this bundle

#Choose how many MAD above and below median to outlier, defaults to 2 if no numb_mad argument supplied, if numb_mad=0 will remove series with age-standardized mean of 0, but nothing else
apud_outliered <- auto_outlier(apud_modeling)
scatter_markout(apud_outliered)
table(apud_outliered$is_outlier)

apud_final <- apud_outliered[, c("cv_admin", "cv_marketscan_2000", "cv_marketscan_other") := NULL]

#Write the data to an Excel file in order to upload it  
upload_path <- paste0(j, "FILEPATH/3197_", date, ".xlsx")
write.xlsx(apud_final, upload_path, col.names=TRUE, sheetName = "extraction")

#Then add a description and upload
description <- "Same bundle version and approach as XWV 3128, but checking all transformations were applied properly."

apud_upload_xw <- save_crosswalk_version(bundle_version_id=7967, data_filepath=upload_path, description = description)
#Crosswalk version ID: 6926