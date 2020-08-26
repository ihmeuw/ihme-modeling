## USER
## Data prep for Step 4

rm(list=ls())

## Set up working environment 
if (Sys.info()["sysname"] == "Linux") {
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
  l_root <- "FILEPATH"
} else {
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
  l_root <- "FILEPATH"
}

my.lib <- paste0(h, "R/")
central_fxn <- paste0(j, "FILEPATH")

date <- gsub("-", "_", Sys.Date())

pacman::p_load(data.table, ggplot2, openxlsx, readxl, readr, RMySQL, stringr, tidyr, plyr, dplyr, mvtnorm)
# install.packages("metafor", lib = my.lib)
# library("metafor", lib.loc = my.lib)
install.packages("msm", lib = my.lib)
library("msm", lib.loc = my.lib)
# installr, update = TRUE, , install = TRUE
# installr::install.rtools()

## Source central functions
source(paste0(central_fxn, "get_age_metadata.R"))
source(paste0(central_fxn, "get_location_metadata.R"))
source(paste0(central_fxn, "save_bundle_version.R"))
source(paste0(central_fxn, "get_bundle_version.R"))
source(paste0(central_fxn, "save_crosswalk_version.R"))
source(paste0(central_fxn, "get_bundle_data.R"))
source(paste0(central_fxn, "upload_bundle_data.R"))
source(paste0(central_fxn, "get_draws.R"))

## Source other functions
source(paste0(h, "code/getrawdata.R"))
source(paste0(h, "code/sexratio.R"))
source(paste0(h, "code/datascatters.R"))
source(paste0(h, "code/aggregate_marketscan.r"))
source(paste0(h, "code/samplematching_wageaggregation.R"))
source(paste0(h, "code/prepmatchesforMRBRT.R"))
source(paste0(j, "temp/reed/prog/projects/run_mr_brt/mr_brt_functions.R"))
source(paste0(h, "code/applycrosswalks.R"))
# source(paste0(h, "code/outlierbyMAD.R"))
source(paste0(h, "code/update_seq.R"))
source(paste0(h, "code/age_splitting_fxns.R"))

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

# This is the set of age-group metadata with correct weights for age-split code, with under 5, 5-9,...90-94 and 95+
under_fives <- age_dt[age_group_id %in% c(28,5), ]
under_fives <- under_fives[ , u5 := sum(age_group_weight_value)]
under_fives <- unique(under_fives[ , `:=` (age_group_id = 1, age_group_years_start = 0, age_group_years_end = 4, age_start = 0, age_end = 4, age_group_weight_value = u5)])
under_fives[ , u5 := NULL]
age_dt2 <- age_dt[!(age_group_id %in% c(28,5)), ]
age_dt2 <- rbind(age_dt2, under_fives)

############################ MAKE CV LISTS
# List of cvs that are in the bundle as useful tags for manipulating data, but not actually considered in crosswalking 
cv_manip <- c("cv_hospital", "cv_marketscan_inp_2000", "cv_marketscan_all_2000", "cv_marketscan_all_2010", "cv_marketscan_inp_2010", "cv_marketscan_all_2012", "cv_marketscan_inp_2012", "cv_taiwan_claims_data", "cv_marketscan", "cv_marketscan_data")

# List of cvs that are marked 1 in one or more reference data source; we tried them as separate crosswalks and abandoned
cv_ref <- c("cv_modified", "cv_mcdonalds", "cv_admin", "cv_marketscan_other", "cv_diag_phys_neuro", "cv_other", "cv_posner", "cv_schumacher")

# List of cvs to not use in making definitions
cv_drop <- c(cv_manip, cv_ref)

# List of cvs that positively identify components of alternative case defs
cv_alts <- c("cv_marketscan_2000")

############################ 1. GET AND FORMAT DATA; make separate tables for modeling sex-ratio, modeling crosswalks, and modeling in DisMod
## Get bundle version prepped by USER,
step4_bundle_version <- get_bundle_version(16277)
table(step4_bundle_version$clinical_data_type, useNA = "always")

bundle_columns <- names(step4_bundle_version)

step4_data <- step4_bundle_version[clinical_data_type!= "inpatient", ]
step4_data <- step4_data[measure=="prevalence" | measure=="incidence", ]
table(step4_data$clinical_data_type, step4_data$measure, useNA = "always")

setnames(step4_data, old = c("mcdonalds", "schumacher", "modified"), new = c("cv_mcdonalds", "cv_schumacher", "cv_modified"))
market_scan_cv_labels(step4_data)
step4_data <- merge(step4_data, loc_dt[ , c("location_id", "super_region_id", "super_region_name", "region_id")], by = "location_id")

step4_data <- get_definitions(step4_data)
table(step4_data$definition)

mrbrt_xw_dt <- copy(step4_data)
mrbrt_sex_dt <- copy(step4_data)

# Drop all both-sex data from sex-ratio modeling data, make separate datasets for modeling sex-ratio in prev and inc
mrbrt_sex_dt <- mrbrt_sex_dt[sex!="Both", ]
mrbrt_sex_prev <- mrbrt_sex_dt[measure=="prevalence", ]
mrbrt_sex_inc <- mrbrt_sex_dt[measure=="incidence", ]

############################## PATHS TO MODELS PROVIDED
sex_model_path <- "FILEPATH"  #no summaries, draws or predictions
claims_model_path <- "FILEPATH"  #no summaries, draws or predictions
network_model_path <- "FILEPATH"  #has coefficient for reference?

out_path <- paste0(j, "FILEPATH")

# Check specificity designations
# Age-sex split based on intra-study designations
#################################################

############## Prep sex-ratio and run MR-BRT sex-ratio model for prev
mrbrt_sex_prev <- sex_yearage(mrbrt_sex_prev)
sex_ratios <- get_sex_ratios(mrbrt_sex_prev)
sex_ratios <- drop_stuff(sex_ratios)

ratio_byage(sex_ratios)

sex_lratios <- logtrans_ratios(sex_ratios)
sex_histo(sex_lratios)

sexratio_fit <- run_mr_brt(
  output_dir = out_path,
  model_label = paste0("sexratio_prev_[USER]_", date),   
  data = sex_lratios,
  mean_var = "lratio",
  se_var = "log_ratio_se",
  #covs = sex_covs,
  method = "trim_maxL",
  overwrite_previous = TRUE,
  trim_pct = 0.1,
  study_id = "nid"
  #lasso = T
)

## Predict ratios from model for training data
sexratio_training_predictions <- predict_fromtrain(sexratio_fit)
write.xlsx(sexratio_training_predictions, paste0(out_path, "/148_sexrat_trainpred_prev", date, ".xlsx"), col.names=TRUE)

############# 2. Apply MR-BRT sex-ratio on prev
## Get both-sex prevalence data for modeling
step4_bothsx_prev <- step4_data[sex=="Both" & measure=="prevalence", ]

## Save data that don't require this processing to bind back later
step4_sxspec_or_inc <- step4_data[!(sex=="Both" & measure=="prevalence"), ]

## Make mid-age variable for making visualization and then visualize observations to be processed
step4_bothsx_prev <- step4_bothsx_prev[ , age_mid := (age_end-age_start)/2 + age_start]
print(step4_bothsx_prev[ , c("age_start", "age_end", "age_mid")])

ggplot(step4_bothsx_prev, aes(age_start, mean)) + geom_point()
ggsave(paste0(out_path, "/bothsx_prev_PRE_ratio", date, ".pdf"), width=10, height=7, limitsize=FALSE)

## Estimate sample sex-ratios in modeling data as ppln sex-ratios
step4_bothsx_prev <- get_sex_ppln(step4_bothsx_prev, "step4", age_dt = age_dt2)

## Predict ratios from model for modeling data
sexratio_new_predictions <- predict_new(sexratio_fit, bothsx_dt=step4_bothsx_prev, by = "none")
step4_bothsx_prev <- transform_bothsexdt(step4_bothsx_prev, sexratio_new_predictions, by = "none") 
table(step4_bothsx_prev$specificity, step4_bothsx_prev$group)

## Visualize transformed observations
ggplot(step4_bothsx_prev, aes(age_start, mean, color = sex)) + geom_point()
ggsave(paste0(out_path, "/bothsx_prev_POST_ratio", date, ".pdf"), width=10, height=7, limitsize=FALSE)

## Update seqs in processed observations, bind back to the modeling data that didn't require this processing step
step4_bothsx_prev_seqsu <- update_seq(step4_bothsx_prev)
step4_data_2 <- rbind(step4_sxspec_or_inc, step4_bothsx_prev, use.names = TRUE, fill = TRUE)

write.xlsx(step4_data_2, paste0(out_path, "/148_step4data_prevsexrat_applied", date, ".xlsx"), col.names=TRUE)

setdiff(step4_data$nid, step4_data_2$nid) #0 
step4_seq <- unique(step4_data$seq)
step4_2_seq <- unique(c(step4_data_2$seq, step4_data_2$crosswalk_parent_seq))
setdiff(step4_seq, step4_2_seq) #0

table(step4_data_2[measure=="prevalence", ]$sex)

############## Prep sex-ratio and run MR-BRT sex-ratio model for inc
mrbrt_sex_inc<- sex_yearage(mrbrt_sex_inc)
sex_ratios <- get_sex_ratios(mrbrt_sex_inc)
sex_ratios <- drop_stuff(sex_ratios)

ratio_byage(sex_ratios)

sex_lratios <- logtrans_ratios(sex_ratios)
sex_histo(sex_lratios)

sexratio_fit <- run_mr_brt(
  output_dir = out_path,
  model_label = paste0("sexratio_inc_[USER]_", date),   
  data = sex_lratios,
  mean_var = "lratio",
  se_var = "log_ratio_se",
  #covs = sex_covs,
  method = "trim_maxL",
  overwrite_previous = TRUE,
  trim_pct = 0.1,
  study_id = "nid"
  #lasso = T
)

## Predict ratios from model for training data
sexratio_training_predictions <- predict_fromtrain(sexratio_fit)
write.xlsx(sexratio_training_predictions, paste0(out_path, "/148_sexrat_trainpred_inc", date, ".xlsx"), col.names=TRUE)

############# 3. Apply MR-BRT sex-ratio on inc
## Get both-sex incidence data for modeling
step4_bothsx_inc <- step4_data_2[sex=="Both" & measure=="incidence", ]

## Save data that don't require this processing to bind back later
step4_sxspec_or_prev <- step4_data_2[!(sex=="Both" & measure=="incidence"), ]

## Make mid-age variable for making visualization and then visualize observations to be processed
step4_bothsx_inc <- step4_bothsx_inc[ , age_mid := (age_end-age_start)/2 + age_start]
print(step4_bothsx_inc[ , c("age_start", "age_end", "age_mid")])

ggplot(step4_bothsx_inc, aes(age_start, mean)) + geom_point()
ggsave(paste0(out_path, "/bothsx_inc_PRE_ratio", date, ".pdf"), width=10, height=7, limitsize=FALSE)

## Estimate sample sex-ratios in modeling data as ppln sex-ratios
step4_bothsx_inc <- get_sex_ppln(step4_bothsx_inc, "step4", age_dt = age_dt2)

## Predict ratios from model for modeling data
sexratio_new_predictions <- predict_new(sexratio_fit, bothsx_dt=step4_bothsx_inc, by = "none")
step4_bothsx_inc <- transform_bothsexdt(step4_bothsx_inc, sexratio_new_predictions, by = "none") 
table(step4_bothsx_inc$specificity, step4_bothsx_inc$group)

## Visualize transformed observations
ggplot(step4_bothsx_inc, aes(age_start, mean, color = sex)) + geom_point()
ggsave(paste0(out_path, "/bothsx_inc_POST_ratio", date, ".pdf"), width=10, height=7, limitsize=FALSE)

## Update seqs in processed observations, bind back to the modeling data that didn't require this processing step
step4_bothsx_inc_seqsu <- update_seq(step4_bothsx_inc)
step4_data_3 <- rbind(step4_sxspec_or_prev, step4_bothsx_inc_seqsu, use.names = TRUE, fill = TRUE)

write.xlsx(step4_data_3, paste0(out_path, "/148_step4data_allsexrat_applied", date, ".xlsx"), col.names=TRUE)

setdiff(step4_data$nid, step4_data_3$nid) #0
step4_3_seq <- unique(c(step4_data_3$seq, step4_data_3$crosswalk_parent_seq))
setdiff(step4_seq, step4_3_seq) #0

table(step4_data_3$measure, step4_data_3$sex)

################## Re-run MR-BRT MS 2000 crosswalk model 
## Dropping data outside of HINA since we are only looking for matches in USA and match-finding will be faster with fewer rows to check
mrbrt_xw_dt <- mrbrt_xw_dt[region_id==100, ]

## Find matches
findmatch <- copy(mrbrt_xw_dt)
findmatch <- subnat_to_nat(findmatch)
findmatch <- calc_year(findmatch)

scatter_bydef(findmatch, upper = 0.01, width = 10, height = 7) 

#I don't really understand what the get_age_combos function does, or the subsequent two lines of line of code after it gets used.  The function makes sense to me up until it defines small_dt.  Why doesn't the function just return the datatable from the 3rd line?
age_dts <- get_age_combos(findmatch)
findmatch <- age_dts[[1]]
age_match_dt <- age_dts[[2]] ## DON'T ACTUALLY NEED THIS BUT FOUND IT HELPFUL FOR VETTING
#? Email Emma about the above three lines

pairs <- combn(findmatch[, unique(definition)], 2)
#combn function generates all combinations of the elements of vector x (in this case, a vector of the unique values found in the definition column of the data.table tpud_xw) taken m (in this case, 2) at a time

matches <- rbindlist(lapply(1:dim(pairs)[2], function(x) get_matches(n = x, pair_dt = findmatch)))
#I ran through the get_matches function for an example that has matches without having to use age-aggregation, but haven't worked through the case with needing to use the age-aggregation, nor have I thought through what is going on with lapply and rbindlist on a detailed level, probably should at some point.

table(matches$def, matches$def2)

matches[ , nid_comb:=paste0(nid, nid2)]
write.xlsx(matches, paste0(out_path, "/148_ms2000_v_lit_other_claims_matches_", date, ".xlsx"), col.names=TRUE)

## Format matches
ratios <- copy(matches)

## Calculate ratios and their standard errors, drop unneeded variables, calculate dummies, visualize ratios, take their logs and calc standard error of their logs
#Log-ratios are no longer the preferred input variable for MR-BRT modeling, but still nice to have in data-set for visualizing
ratios <- calc_ratio(ratios)
#This line is for dropping matches with infinite or zero values for the ratios, some of which I think the difference of logits method will handle
ratios <- drop_stuff(ratios)
ratios <- add_compdumminter(ratios)

lratios <- logtrans_ratios(ratios)

## Calculate logits, logit differences, and standard errors to go into meta-regression
logit_differences <- calc_logitdf(lratios)

## Pick list of cvs for alternative case definitions and make into a list MR-BRT will understand
for(c in cv_alts){
  cov <- cov_info(c, "X")
  if(c == cv_alts[1]){
    cov_list <- list(cov)
  } else {
    cov_list <- c(cov_list, list(cov))
  }
}

## Run MR-BRT model
marketscan2000_fit <- run_mr_brt(
  output_dir = out_path,
  model_label = paste0("marketscan2000_v_all_", date),
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

## Make predictions and model summaries
ms2000_prediction <- unique(predict_xw(marketscan2000_fit, "logit_dif"), by = cv_alts)

################## 4. Apply MS 2000 crosswalk
## Make sure modeling data have values for all alt defs
step4_data_3[is.na(cv_marketscan_2000), cv_marketscan_2000:=0]

## Get modeling data requiring crosswalk for non-reference case definition
step4_nonref <- step4_data_3[definition!="reference", ]

## Save reference modeling data to bind back later
step4_ref <- step4_data_3[definition=="reference", ]

## Predictions for new data
network_prediction_new <- unique(predict_xw(marketscan2000_fit, "logit_dif", step4_nonref), by = cv_alts)
write.xlsx(network_prediction_new, paste0(out_path, "/marketscan2000_v_all_2019_10_06/predictions_on_step4dt", date, ".xlsx" ), col.names=TRUE)

## Transform nonreference modeling data
step4_nonref_x <- transform_altdt(step4_nonref, network_prediction_new, "logit_dif")

## Visualize effect
pre_post_dt <- merge(step4_nonref, step4_nonref_x, by = c("seq", "crosswalk_parent_seq", "age_start", "sex"))
mean_v_mean(pre_post_dt, "xw_step4_dt", width=10, height=7, xlim=0.01)

## Update seqs in transformed observations, bind back to reference modeling data
step4_nonref_x_seqsu <- update_seq(step4_nonref_x)
step4_data_4 <- rbind(step4_ref, step4_nonref_x_seqsu, use.names = TRUE)

write.xlsx(step4_data_4, paste0(out_path, "/148_step4dt_sexrat_xw_applied", date, ".xlsx"), col.names=TRUE)

setdiff(step4_data$nid, step4_data_4$nid) #0
step4_4_seq <- unique(c(step4_data_4$seq, step4_data_4$crosswalk_parent_seq)) 
setdiff(step4_seq, step4_4_seq) #0
setdiff(step4_2_seq, step4_4_seq) #0

################## 5. Age-split from DisMod age-pattern
ms_age_prev_split <- step4_data_4[measure=="prevalence", ]
ms_age_prev_split <- dis_age_split(ms_age_prev_split, age_dt2, gbd_round_id = 6, age_pattern_loc = 1, measure = 5, age_pattern_mod_ent = 1955, step = "step1")

ms_age_inc_split <- step4_data_4[measure=="incidence", ]
ms_age_inc_split <- dis_age_split(ms_age_inc_split, age_dt2, gbd_round_id = 6, age_pattern_loc = 1, measure = 6, age_pattern_mod_ent = 1955, step = "step1")

step4_data_5 <- rbind(ms_age_prev_split, ms_age_inc_split, use.names = TRUE)

dropped_nids <- setdiff(step4_data$nid, step4_data_5$nid) 
table(step4_data_4[nid %in% dropped_nids, ]$year_start)

step4_data_6 <- rbind(step4_data_5, step4_data_4[nid %in% dropped_nids, ], use.names = TRUE)
step4_data_6 <- update_seq(step4_data_6)

setdiff(step4_data$nid, step4_data_6$nid) #0

step4_6_seq <- unique(c(step4_data_6$seq, step4_data_6$crosswalk_parent_seq)) 
dropped_seqs <- setdiff(step4_4_seq, step4_6_seq)
print(dropped_seqs)
table(step4_data_4[(seq %in% dropped_seqs | crosswalk_parent_seq %in% dropped_seqs), ]$year_start)

step4_data_7 <- rbind(step4_data_6, step4_data_4[(seq %in% dropped_seqs | crosswalk_parent_seq %in% dropped_seqs), ], use.names = TRUE)
step4_data_7 <- update_seq(step4_data_7)

step4_7_seq <- unique(c(step4_data_7$seq, step4_data_7$crosswalk_parent_seq))
setdiff(step4_seq, step4_7_seq) #0

write.xlsx(step4_data_7, paste0(out_path, "/148_step4_sexrat_xw_extage_applied_", date, ".xlsx"), col.names=TRUE)

################## Clean up and upload
# Revert names
setnames(step4_data_7, old = c("cv_mcdonalds", "cv_schumacher", "cv_modified"), new = c("mcdonalds", "schumacher", "modified"))
step4_data_8 <- step4_data_7[ , c(bundle_columns, "crosswalk_parent_seq"), with=FALSE]

# Fix standard errors > 1 from clinical informatics
step4_data_8 <- step4_data_8[standard_error > 1, standard_error:=1]

print(unique(step4_data_8[location_id %in% c(95, 4621, 4623, 4624, 4618, 4619, 4625, 4626, 4622, 4620), ]$nid)) #111320 and 313985, splitting GBR into countries and South East of England into UTLAs was missed in some cases, not going back now
step4_data_8 <- step4_data_8[!(location_id %in% c(95, 4621, 4623, 4624, 4618, 4619, 4625, 4626, 4622, 4620)), ]

print(unique(step4_data_8[is_outlier==1, ]$note_sr))
print(step4_data_8[grepl("Outliered due to insufficient uncertain", note_sr), c("nid", "note_sr", "mean", "cases", "lower", "upper", "standard_error", "sample_size")])
step4_data_8 <- step4_data_8[grepl("Outliered due to insufficient uncertain", note_sr), `:=` (upper = 1, standard_error = NA, sample_size = NA)]

ggplot(step4_data_8, aes(age_start, mean)) + geom_point()
ggsave(paste0(out_path, "/148_final_dt", date, ".pdf"), width=10, height=7, limitsize=FALSE)

#Write data to Excel file and upload
upload_path <- paste0(j, "FILEPATH", date, ".xlsx")
write.xlsx(step4_data_8, upload_path, col.names=TRUE, sheetName = "extraction")
description <- "Lit, step 2 and 4 claims, xw MS2000, SR on both inc and prev, outliered hx and 0 mean 0 cases"
step4_upload <- save_crosswalk_version(bundle_version_id=16277, data_filepath=upload_path, description = description)
#Crosswalk version ID #####

step4_data_9 <- step4_data_8[grepl("Russia Statistical", field_citation_value), is_outlier:=1]

#Write data to Excel file and upload
upload_path <- paste0(j, "FILEPATH", date, ".xlsx")
write.xlsx(step4_data_9, upload_path, col.names=TRUE, sheetName = "extraction")
description <- "Lit, step 2 and 4 claims, xw MS2000, SR on both inc and prev, outliered hx, inadeq uncert and Russia claims"
step4_upload <- save_crosswalk_version(bundle_version_id=16277, data_filepath=upload_path, description = description)
#Crosswalk version ID #####

step4_data_10 <- step4_data_9[grepl("Poland National Health Fund", field_citation_value), is_outlier:=1]

#Write data to Excel file and upload
upload_path <- paste0(j, "FILEPATH", date, ".xlsx")
write.xlsx(step4_data_10, upload_path, col.names=TRUE, sheetName = "extraction")
description <- "Lit, step 2 and 4 claims, xw MS2000, SR on both inc and prev, outliered hx, inadeq uncer, and Russia and Poland claims"
step4_upload <- save_crosswalk_version(bundle_version_id=16277, data_filepath=upload_path, description = description)
#Crosswalk version ID #####

