## PUD with complication, re-preparation of data from GBD 2017 using GBD 2019 data-prep methods

#################### ENVIRONMENT
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
source(paste0(central_fxn, "get_draws.R"))
source(paste0(central_fxn, "get_population.R"))

## Source other functions
source(paste0(h, "code/getrawdata.R"))
source(paste0(h, "code/sexratio.R"))
source(paste0(h, "code/datascatters.R"))
source(paste0(h, "code/samplematching_wageaggregation.R"))
source(paste0(h, "code/prepmatchesforMRBRT.R"))
source(paste0(j, "FILEPATH/mr_brt_functions.R"))
source(paste0(h, "code/applycrosswalks.R"))
source(paste0(h, "code/outlierbyMAD.R"))
source(paste0(h, "code/update_seq.R"))
source(paste0(h, "code/age_splitting_fxns.R"))

## Get metadata
loc_dt <- get_location_metadata(location_set_id = 22)

all_fine_ages <- as.data.table(get_age_metadata(age_group_set_id=12))
not_babies <- all_fine_ages[!age_group_id %in% c(2:4)]
not_babies[, age_end := age_group_years_end-1]
all_fine_babies <- as.data.table(get_age_metadata(age_group_set_id=18))
group_babies <- all_fine_babies[age_group_id %in% c(28)]

# This is the set of age-group metadata with correct weights for aggregating clinical informatics data or Ubcov-extracted data with under 1, 1-4, 5-9...90-94 and 95+
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

################## INPUT DATA
## Get raw data and create covariates for marking different groups of clinical informatics data, subset to the measure being crosswalked
#Use argument bundle_version = 0 if you want to create a new bundle version
comppud_dt <- get_raw_data(3196, 'step2', bundle_version = 0)
#Bundle version ID: 7964
comppud_bundle <- get_raw_data(3196, 'step2', bundle_version = 7964)

## Tag bundle data that *don't* come from the clinical informatics team with consistently-named study-level covariates
comppud_dt <- copy(comppud_bundle)
comppud_dt[cv_admin_data==1, cv_admin:=1]
comppud_dt[ , cv_admin_data:=NULL]

## List of cvs that are useful tags for manipulating data, but not actually things I want a separate crosswalk for
cv_manip <- c("cv_marketscan_data", "cv_literature")
## List of cvs that positively identify reference data
cv_ref <- c("cv_admin")
## Combined list of cvs to drop in match-finding (but keep in raw data)
cv_drop <- c(cv_manip, cv_ref)

## List of cvs that positively identify components of alternative case defs
cv_alts <- c("cv_marketscan_2000", "cv_marketscan_other", "cv_diag_exam")

## Fill out cases, sample size, standard error using Epi Uploader formulae; standardize the naming of case definitions; add IDs for higher-order geography
get_cases_sample_size(comppud_dt)
get_se(comppud_dt)

## Add variables for use in data-prep that will later need to be dropped to pass Uploader validations
comppud_dt <- get_definitions(comppud_dt)
comppud_dt <- merge(comppud_dt, loc_dt[ , c("location_id", "super_region_id", "super_region_name", "region_id")], by = "location_id")

## See data that would need sex-split or age-split
comppud_needssplit <- comppud_dt[(sex=="Both" | (age_end-age_start >= 25 & age_start!=95)) & (group_review == 1| is.na(group_review)), ]

print(comppud_needssplit[ , c("nid", "age_start", "age_end", "sex", "definition", "measure", "year_start", "year_end", "case_name", "mean", "cases", "sample_size")])
# 223507, 120528, and 118193 aren't worth age-splitting b/c they use a case-definition that can't be crosswalked 
# 214642 and 217557 can be split and included

####################### PROCESSING DATA

comppud_procdt <- copy(comppud_dt)

###################### MODEL CROSSWALK FOR CASE DEFINITIONS/DESIGN VARIABLES
## Prep proc data for match-finding and crosswalk modeling
comppud_findmatch <- copy(comppud_procdt)
comppud_findmatch <- subnat_to_nat(comppud_findmatch)
comppud_findmatch <- calc_year(comppud_findmatch)

scatter_bydef(comppud_findmatch, upper = 0.01)
scatter_bydef(comppud_findmatch)
scatter_bydef(comppud_findmatch[cv_literature==1, ], upper = 0.01)

## Find matches

age_dts <- get_age_combos(comppud_findmatch)
comppud_findmatch <- age_dts[[1]]
age_match_dt <- age_dts[[2]] 

pairs <- combn(comppud_findmatch[, unique(definition)], 2)
#combn function generates all combinations of the elements of vector x (in this case, a vector of the unique values found in the definition column of the data.table comppud_findmatch) taken m (in this case, 2) at a time

matches <- rbindlist(lapply(1:dim(pairs)[2], function(x) get_matches(n = x, pair_dt = comppud_findmatch)))

matches[ , nid_comb:=paste0(nid, nid2)]
matches[nid==nid2, cv_intRA:=1]

write.xlsx(matches, paste0(out_path, "/3196_matches_", date, ".xlsx"), col.names=TRUE)

## There are no matches to cv_diag_exam from *any* other case-definition (Denmark in the 2000s, Spain and Germany in the 1980s and 1990s)

## Prep and perform meta-regression

#***comment out reading in the data table if you are running full pipeline uninterrupted, bring back in if running MR-BRT on previously prepped data
#ratios <- as.data.table(read_excel(paste0(out_path, "/3196_matches_", date, ".xlsx")))

## Calculate ratios and their standard errors, drop unneeded variables, calculate dummies, visualize ratios, take their logs and calc standard error of their logs
#Log-ratios are no longer the preferred input variable for MR-BRT modeling, but useful visualization
ratios <- calc_ratio(matches)
#This line is for dropping matches with infinite or zero values for the ratios (from visualization only)
ratios <- drop_stuff(ratios)
ratios <- add_compdumminter(ratios)
histo_ratios(ratios, bins = 0.2)

lratios <- logtrans_ratios(ratios)

## Calculate logits, logit differences, and standard errors to go into meta-regression
logit_differences <- calc_logitdf(lratios)

## Take list of cvs for components of alternative case definitions, drop the alts that don't have enough direct or indirect matches to reference, and make into a list MR-BRT will understand
cv_nomatch <- c("cv_diag_exam")
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
## Final model shown

comppud_fit <- run_mr_brt(
  output_dir = out_path,
  model_label = paste0("comppud_xwmodel_", date),
  data = logit_differences,
  mean_var = "diff_logit",
  se_var = "se_diff_logit",
  covs = cov_list,
  remove_x_intercept = TRUE,
  method = "remL",
  overwrite_previous = TRUE,
  study_id = "nid_comb"
)

# Only 7 studies, so no trim allowed

## Get predicted coefficients with all sources of uncertainty, predict for training data (these sets of predictions will be the same for training and new data if there are no continuous covariates or multi-dimensional case-definitions) 

# Predict crosswalk for training data
comppud_trainingprediction <- unique(predict_xw(comppud_fit, "logit_dif"), by = cv_alts)

###################### MODEL SEX-RATIO
# Use sex-specific rows in processing set to estimate sex-ratio in MR-BRT
comppud_sxproc <- comppud_procdt[sex != "Both", ]

comppud_sxproc <- sex_yearage(comppud_sxproc)
sex_ratios <- get_sex_ratios(comppud_sxproc)
sex_ratios <- drop_stuff(sex_ratios)

ratio_byage(sex_ratios)

sex_lratios <- logtrans_ratios(sex_ratios) 
sex_histo(sex_lratios)

## No apparent differences in sex-ratio by age or super-region.

## Model sex ratios from sex-specific data

sexratio3196_fit <- run_mr_brt(
  output_dir = out_path,
  model_label = paste0("sexratio_comppud_", date),   
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
sexratio_training_predictions <- predict_fromtrain(sexratio3196_fit)
write.xlsx(sexratio_training_predictions, paste0(out_path, "/3196_sexrat_trainpred", date, ".xlsx"), col.names=TRUE)

###################### MAKE MODELING SET AND APPLY TRANSFORMATIONS
## Make modeling set
comppud_moddt <- comppud_procdt[!(grepl(cv_nomatch, comppud_procdt$definition)), ]  #Definition can't be on the list of uncrosswalkables
table(comppud_procdt$definition)
table(comppud_moddt$definition)

table(comppud_moddt[!(is.na(group)), ]$nid)
table(comppud_moddt[!(is.na(group)), ]$nid, comppud_moddt[!(is.na(group)), ]$specificity)
print(comppud_moddt[nid==120530, c("nid", "sex", "age_start", "age_end", "case_name", "group", "specificity", "group_review")])

ggplot(comppud_moddt, aes(age_start, mean, color = sex)) + geom_point()
ggsave(paste0(out_path, "/comppud_moddt_", date, ".pdf"), width=10, height=7, limitsize=FALSE)

write.xlsx(comppud_moddt, paste0(out_path, "/3196_unadj_moddt_", date, ".xlsx"), col.names=TRUE)

## For two sources that reported different types of complications, combine numerators and denominator appropriately to create data for incidence of any complication
# (This section is not generalizable to other datasets, but better to extract numbers as reported and document aggregations in code than perform manually)
comppud_mod_nogrp <- comppud_moddt[!(nid %in% c(120530, 214642)), ]
comppud_120530 <- comppud_moddt[nid==120530, ]
comppud_214642 <- comppud_moddt[nid==214642, ]

comppud_120530 <- comppud_120530[group_review==1, ]
comppud_120530[ , c("mean", "lower", "upper", "standard_error", "uncertainty_type_value"):=NA]
by_vars <- c("sex", "year_start", "year_end", "age_start", "age_end", "group")
comppud_120530[ , new_cases := sum(cases), by=by_vars]
head(comppud_120530)
comppud_120530 <- comppud_120530[ , .SD[c(1)], by=by_vars]
comppud_120530[ , cases := new_cases]
comppud_120530[ , `:=` (new_cases = NULL, case_definition = NA, specificity = "sum", case_name = "sum of hemorrhage, perf, duodenal, gastric", input_type = "split")]
comppud_120530 <- update_seq(comppud_120530)

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

comppud_moddt2 <- rbind(comppud_mod_nogrp, comppud_120530, use.names=TRUE, fill = TRUE)
comppud_moddt2 <- rbind(comppud_moddt2, comppud_214642, use.names=TRUE)

write.xlsx(comppud_moddt2, paste0(out_path, "/3196_moddt_grpsadj", date, ".xlsx"), col.names=TRUE)

comppud_moddt2_viz <- comppud_moddt2[is.na(mean), mean:=cases/sample_size]
ggplot(comppud_moddt2_viz, aes(age_start, mean, color = sex)) + geom_point()
ggsave(paste0(out_path, "/comppud_moddt2_", date, ".pdf"), width=10, height=7, limitsize=FALSE)

## Apply sex-ratio
# Get remaining both-sex modeling data
comppud_bothsxmod <- comppud_moddt2[sex=="Both", ]

## Save data that don't require this processing to bind back later
comppud_mod_bothsx_dropped <- comppud_moddt2[sex!="Both", ]

## Make mid-age variable for making visualization and then visualize observations to be processed
comppud_bothsxmod <- comppud_bothsxmod[ , age_mid := (age_end-age_start)/2 + age_start]
print(comppud_bothsxmod[ , c("age_start", "age_end", "age_mid")])

ggplot(comppud_bothsxmod, aes(age_start, mean)) + geom_point()
ggsave(paste0(out_path, "/bothsx_PRE_ratio", date, ".pdf"), width=10, height=7, limitsize=FALSE)

## Estimate sample sex-ratios in modeling data as ppln sex-ratios
comppud_bothsxmod <- get_sex_ppln(comppud_bothsxmod, "step2", age_dt = age_dt2)

## Get estimated sex-ratio from MR-BRT, and transform both-sex prevalence to separate male and female prevalence
## Predict ratios from model for training data
sexratio_new_predictions <- predict_new(sexratio3196_fit, comppud_bothsxmod, by = "none")
comppud_bothsxmod <- transform_bothsexdt(comppud_bothsxmod, sexratio_new_predictions, by = "none") 
table(comppud_bothsxmod$specificity, comppud_bothsxmod$group)

## Visualize transformed observations
ggplot(comppud_bothsxmod, aes(age_start, mean, color = sex)) + geom_point()
ggsave(paste0(out_path, "/bothsx_POST_ratio", date, ".pdf"), width=10, height=7, limitsize=FALSE)

## Update seqs in processed observations, bind back to the modeling data that didn't require this processing step
comppud_bothsxmod_seqsu <- update_seq(comppud_bothsxmod)
comppud_moddt3 <- rbind(comppud_mod_bothsx_dropped, comppud_bothsxmod_seqsu, use.names = TRUE)

write.xlsx(comppud_moddt3, paste0(out_path, "/3196_moddt_grpsadj_sexratapp_", date, ".xlsx"), col.names=TRUE)

## Apply crosswalk coefficient
## Get modeling data requiring crosswalk for non-reference case definition
comppud_mod_nonref <- comppud_moddt3[definition!="reference", ]

## Save reference modeling data to bind back later
comppud_mod_ref <- comppud_moddt3[definition=="reference", ]

## Predict crosswalks for non-reference modeling data
comppud_predictmod <- unique(predict_xw(comppud_fit, "logit_dif", comppud_mod_nonref), by = cv_alts)

## Transform nonreference modeling data
comppud_mod_nonref_x <- transform_altdt(comppud_mod_nonref, comppud_predictmod, "logit_dif")

## Visualize effect
pre_post_dt <- merge(comppud_mod_nonref, comppud_mod_nonref_x, by = c("seq", "crosswalk_parent_seq", "age_start", "sex"))
mean_v_mean(pre_post_dt, "xw_mod_dt")

## Update seqs in transformed observations, bind back to reference modeling data
comppud_mod_nonref_x_seqsu <- update_seq(comppud_mod_nonref_x)
comppud_moddt4 <- rbind(comppud_mod_ref, comppud_mod_nonref_x_seqsu, use.names = TRUE)

write.xlsx(comppud_moddt4, paste0(out_path, "/3196_moddt_grpsadj_sexratapp_xwapp_", date, ".xlsx"), col.names=TRUE)

## Apply age-pattern

#comppud_intERsplit <- copy(comppud_moddt4)
#comppud_intERsplit <- dis_age_split(comppud_intERsplit, age_dt2, gbd_round_id = 6, age_pattern_loc = 1, measure = 5, age_pattern_mod_ent = 19691, step = "step1")
#Ran this but found the only broad-age datum would have less than 1 case per age stratum so doesn't get split

## Visualize final data
ggplot(comppud_moddt4, aes(age_start, mean, color = factor(sex))) + geom_point()
ggsave(paste0(out_path, "/3196_final_dt", date, ".pdf"), width=10, height=7, limitsize=FALSE)

################## TIDY COLUMNS, MARK OUTLIERS AND SAVE CROSSWALK VERSIONS
comppud_moddt5 <- comppud_moddt4[ , cv_admin_data:=cv_admin]
comppud_moddt5 <- comppud_moddt5[ , c("cv_admin", "cv_marketscan_2000", "cv_marketscan_other"):=NULL]

#Write the data to an Excel file in order to upload it  
upload_path <- paste0(j, "FILEPATH/3196_noout", date, ".xlsx")
write.xlsx(comppud_moddt5, upload_path, col.names=TRUE, sheetName = "extraction")

#Then add a description and upload
description <- "same approach as xwv 6917, but fixed code for apply xw coefficients"
comppud_upload_xw5 <- save_crosswalk_version(bundle_version_id=7964, data_filepath=upload_path, description = description)
#Crosswalk version ID 7028

#Mark outliers based on median absolute difference
comppud_moddt6 <- auto_outlier(comppud_moddt5)
scatter_markout(comppud_moddt6)
table(comppud_moddt6$is_outlier)

#Write the data to an Excel file in order to upload it
upload_path <- paste0(j, "FILEPATH/3196_2MAD_", date, ".xlsx")
write.xlsx(comppud_moddt6, upload_path, col.names=TRUE, sheetName = "extraction")

#Then add a description and upload
description <- "same approach as 6920, but fixed code for applying xw coefficients"
comppud_upload_xw6 <- save_crosswalk_version(bundle_version_id=7964, data_filepath=upload_path, description = description)
#Crosswalk version ID 7031







