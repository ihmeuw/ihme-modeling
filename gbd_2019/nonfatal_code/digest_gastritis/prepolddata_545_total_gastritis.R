## Total gastritis and duodenitis, re-prepare GBD 2017 data with GBD 2019 data-preparation methods

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
install.packages("metafor", lib = my.lib)
library("metafor", lib.loc = my.lib)
install.packages("msm", lib = my.lib)
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
totalgd_dt <- get_raw_data(545, 'step2', bundle_version = 0)
#Bundle version ID: 2498
totalgd_dt <- get_raw_data(545, 'step2', bundle_version = 2498)

## Store bundle columns for later
bundle_columns <- names(totalgd_dt)

## Tag bundle data that *don't* come from the clinical informatics team with consistently-named study-level covariates
totalgd_dt[cv_admin_data==1, cv_admin:=1]
totalgd_dt[ , cv_admin_data:= NULL]

## List of cvs that are useful tags for manipulating data, but not actually bias variables to crosswalk
cv_manip <- c("cv_marketscan_data", "cv_literature")
## List of cvs that positively identify reference data
cv_ref <- c("cv_admin")
## Combined list of cvs to drop in match-finding (but keep in raw data)
cv_drop <- c(cv_manip, cv_ref)

## List of cvs that positively identify components of alternative case defs
cv_alts <- c("cv_marketscan_2000", "cv_marketscan_other", "cv_endoscopy", "cv_serology", "cv_indication")

## Fill out cases, sample size, standard error using Epi Uploader formulae; standardize the naming of case definitions; add IDs for higher-order geography
get_cases_sample_size(totalgd_dt)
get_se(totalgd_dt)

## Add variables for use in data-prep that will later need to be dropped to pass Uploader validations
totalgd_dt <- get_definitions(totalgd_dt)
totalgd_dt <- merge(totalgd_dt, loc_dt[ , c("location_id", "super_region_id", "region_id")], by = "location_id")

####################################################
## Split data reported for both sexes combined  
# Split sex-specific rows of data with > 20-year age-span (or drop if finer available) using age distribution for both sexes from the same study, incorporate split rows back into the total data set and drop their source rows
# ie INTRA-STUDY SEX-SPLITTING
# Use sex-specific data to calculate sex-ratios, make covariates to meta-regress with MR-BRT, run model, and apply sex-ratio to data from studies with only both-sex data available, incorporate split rows back into the total data set and drop their source rows
# ie INTER-STUDY SEX-SPLITTING
# These steps were dropped once it was noted all data with reference or crosswalk-able case definitions/design variables are sex-specific
####################################################
  
## Prep raw data for match-finding, crosswalking and transformation
totalgd_findmatch <- copy(totalgd_dt)
totalgd_findmatch <- subnat_to_nat(totalgd_findmatch)
totalgd_findmatch <- calc_year(totalgd_findmatch)

scatter_bydef(totalgd_findmatch, upper = 0.80)

## Find matches
age_dts <- get_age_combos(totalgd_findmatch)
totalgd_findmatch <- age_dts[[1]]
age_match_dt <- age_dts[[2]]

pairs <- combn(totalgd_findmatch[, unique(definition)], 2)
#combn function generates all combinations of the elements of vector x (in this case, a vector of the unique values found in the definition column of the data.table totalgd_xw) taken m (in this case, 2) at a time

matches <- rbindlist(lapply(1:dim(pairs)[2], function(x) get_matches(n = x, pair_dt = totalgd_findmatch)))
matches[ , nid_comb:=paste0(nid, nid2)]

write.xlsx(matches, paste0(out_path, "/545_matches_", date, ".xlsx"), col.names=TRUE)
########################################
#***comment out reading in the data table if you are running full pipeline uninterrupted, bring back in if running MR-BRT on previously prepped data
matches <- as.data.table(read_excel(paste0(out_path, "/545_matches_2019_06_11.xlsx")))

## Calculate ratios and their standard errors, drop unneeded variables, calculate dummies, visualize ratios, take their logs and calc standard error of their logs
#Log-ratios are no longer the preferred input variable for MR-BRT modeling, but useful for visualizing
ratios <- calc_ratio(matches)
#This line is for dropping matches with infinite or zero values for the ratios (from visualization only)
ratios <- drop_stuff(ratios)
ratios <- add_compdumminter(ratios)
histo_ratios(ratios, bins = 0.2)

lratios <- logtrans_ratios(ratios)

## Calculate logits, logit differences, and standard errors to go into meta-regression
logit_differences <- calc_logitdf(lratios)

## Take list of cvs for components of alternative case definitions, drop the alts that don't have enough direct or indirect matches to reference, and make into a list MR-BRT will understand
cv_nomatch <- c("cv_endoscopy", "cv_serology", "cv_indication")
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

#Final choice of model to carry forward
totalgd_fit <- run_mr_brt(
      output_dir = out_path,
      model_label = paste0("totalgd_xwmodel_", date),
      data = logit_differences,
      mean_var = "diff_logit",
      se_var = "se_diff_logit",
      remove_x_intercept = TRUE,
      covs = cov_list,
      method = "remL",
      overwrite_previous = TRUE,
      study_id = "nid_comb"
)

## Get predicted coefficients with all sources of uncertainty, predict for training data and then for all data needing crosswalks (these sets of predictions will be the same if there are no continuous covariates or multi-dimensional case-definitions) 

# Select data that need and can be crosswalked and mark parent seq and seq
totalgd_transform <- totalgd_dt[definition!="reference" & is.na(cv_literature), ]
if ("crosswalk_parent_seq" %in% names(totalgd_transform)) {
  totalgd_transform[is.na(crosswalk_parent_seq), `:=` (crosswalk_parent_seq = seq, seq = NA)]
  totalgd_transform[!is.na(crosswalk_parent_seq), seq = NA] 
} else {
  totalgd_transform[, `:=` (crosswalk_parent_seq = seq, seq = NA)]
}

# Predict crosswalk for training data
totalgd_trainingprediction <- unique(predict_xw(totalgd_fit, "logit_dif"), by = cv_alts)
# Predict crosswalk for raw data
totalgd_predictnew <- unique(predict_xw(totalgd_fit, "logit_dif", totalgd_transform), by = cv_alts)

# Transform data that need and can be crosswalked
totalgd_transform <- transform_altdt(totalgd_transform, totalgd_predictnew, "logit_dif")

# Bind reference data, uncrosswalkable data, crosswalked data; make scatter-plot 
totalgd_dt2 <- rbind(totalgd_transform, totalgd_dt[definition=="reference" & is.na(cv_literature), ], totalgd_dt[cv_literature==1, ], fill=TRUE)
scatter_bydef(totalgd_dt2, raw = FALSE)
# Now drop data that could not be crosswalked, as well as any rows that were group-reviewed out
totalgd_modeling <- totalgd_dt2[(cv_literature==0 | is.na(cv_literature)) & (group_review==1 | is.na(group_review)), ]

## Prep and upload transformed data as a crosswalk version for this bundle

#Choose how many MAD above and below median to outlier, defaults to 2 if no numb_mad argument supplied, if numb_mad=0 will remove series with age-standardized mean of 0, but nothing else
totalgd_outliered <- auto_outlier(totalgd_modeling)
scatter_markout(totalgd_outliered)

columns_keep <- unique(c(bundle_columns, "crosswalk_parent_seq")) 
columns_drop <- c("cv_admin", "cv_marketscan_2000", "cv_marketscan_other")
columns_keep <- setdiff(columns_keep, columns_drop)
totalgd_final <- totalgd_outliered[, ..columns_keep]
scatter_markout(totalgd_final)

#Write the data to an Excel file in order to upload it  
upload_path <- paste0(j, "FILEPATH/545_", date, ".xlsx")
write.xlsx(totalgd_final, upload_path, col.names=TRUE, sheetName = "extraction")

#Then add a description and upload
description <- "Same as XWV 869 but fixed outlier code"
totalgd_upload_xw <- save_crosswalk_version(bundle_version_id=2498, data_filepath=upload_path, description = description)
