# Gastroesophageal reflux disease, re-prepare GBD 2017 data with GBD 2019 methods

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
source(paste0(h, "code/aggregate_marketscan.r"))
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

######## GET INPUT DATA

## Put reviewed data into the bundle (cleared and confirmed empty via Uploader)
upload_bundle_data(3059, "step2", paste0(j, "FILEPATH/3059_decomp2_gbd2019.xlsx"))

## Get a bundle version
gerd_bundle <- get_raw_data(3059, "step2", clin_inf = TRUE)
# Bundle version ID 10913
gerd_bundle <- get_raw_data(3059, "step2", bundle_version = 10913)
print(gerd_bundle[is_outlier==1, c("field_citation_value", "age_start")])

## Store bundle columns for later
bundle_columns <- names(gerd_bundle)
bundle_columns <- bundle_columns[!(bundle_columns %in% c("cv_admin", "cv_marketscan_data", "cv_marketscan_2000", "cv_marketscan_other"))]

# Drop excluded studies that shouldn't contribute to crosswalking OR modeling
gerd_dt <- gerd_bundle[is_outlier==0 | is.na(is_outlier),  ]
gerd_dt <- gerd_dt[location_id!=95 & location_id!=4625, ]  # DisMod will no longer accept data tagged to Great Britian (only England, Scotland, etc)
# Rename the columns that are for study-level covariates I will use in crosswalking, but hadn't been added to epi db when I was ready to upload my data
setnames(gerd_dt, old = c("work_cv", "school_cv", "profession_cv", "gi_spec_cv", "pharma_cv", "waitingrm_cv"), new = c("cv_work", "cv_school", "cv_profession", "cv_gi_spec", "cv_pharma", "cv_waitingrm"))

## Subset and format to make data set for processing
# Choose all the data extracted from published surveys (manually extracted) and household surveys (extracted and collapsed with Ubcov)
gerd_litsurv <- gerd_dt[cv_literature==1 | cv_survey ==1, ]
# Choose claims data (but not hospital)
gerd_claims <- gerd_dt[cv_marketscan_data==1, ]
# Aggregate state-level claims data to the national level and mark with appropriate cv
gerd_agg_claims <- aggregate_marketscan(gerd_claims)
gerd_agg_claims[ , `:=` (cv_admin_data = 1, cv_questionaire=0)]

# Bind bundle data and aggregated claims data
gerd_procdt <- rbind(gerd_litsurv, gerd_agg_claims)
gerd_procdt <- gerd_procdt[ , cv_admin:=NULL] # This column gets added by the get_raw_data fxn but isn't approp for this bundle

## Fill out cases, sample size, standard error using Epi Uploader formulae, add IDs for higher-order geography
gerd_procdt <- get_cases_sample_size(gerd_procdt)
gerd_procdt <- get_se(gerd_procdt)
gerd_procdt <- merge(gerd_procdt, loc_dt[ , c("location_id", "super_region_id", "super_region_name", "region_id")], by = "location_id")
print(gerd_procdt[is.na(cases)|is.na(sample_size), ])

## Derive several dichotomous study-level covariates from the additional, frequency, and duration columns
gerd_procdt <- gerd_procdt[ , cv_optsx:=0]
gerd_procdt[cv_questionaire==1 & additional_option!="none", cv_optsx:=1]
gerd_procdt <- gerd_procdt[ , cv_reqsx:=0]
gerd_procdt <- gerd_procdt[additional_requirement!="mild", ]
gerd_procdt[cv_questionaire==1 & additional_requirement!="none", cv_reqsx:=1]
table(gerd_procdt$cv_optsx, gerd_procdt$cv_reqsx)

gerd_procdt <- gerd_procdt[ , cv_long_recall:= 0]
gerd_procdt[cv_questionaire==1 & recall_type %in% c("Lifetime", "Not Set"), cv_long_recall:=1]
gerd_procdt <- gerd_procdt[ , cv_short_recall:=0]
gerd_procdt[cv_questionaire==1 & (recall_type %in% c("Point", "Period: weeks", "Period: days") | (recall_type=="Period: months" & recall_type_value!=12)), cv_short_recall:=1]
table(gerd_procdt$cv_long_recall, gerd_procdt$cv_short_recall)

# Drop observations from mutually exclusive frequency sets that were used to create inclusive categories 
gerd_procdt <- gerd_procdt[!(frequency %in% c("at least once a year but less than once a week", "at least once a year but less than monthly", "at least once a year but less than twice a week", "> monthly but less than twice a week", "> weekly but less than daily", "at least weekly but less than daily", "weekly", "monthly or less", "> monthly but less than weekly", "at least monthly but less than weekly", "once every two months", "monthly", "sometimes")), ]
table(gerd_procdt$frequency)

gerd_procdt <- gerd_procdt[ , cv_more_req_freq:=0]
gerd_procdt[cv_questionaire==1 & frequency %in% c("daily", "variablem", "more than daily", "montreal", "at least twice a week", "usually", "at least three times a week", "at least four times a week", "mostly/always", "continuously"), cv_more_req_freq:=1]
gerd_procdt <- gerd_procdt[ , cv_less_req_freq_dur:=0]
gerd_procdt[cv_questionaire==1 & frequency %in% c("any", "at least monthly", "at least every 2 weeks", "sometimes", "at least 6 times in a year", "> twice per month", "variablel"), cv_less_req_freq_dur:=1]
# This leaves frequency categories treated as reference as "at least weekly" and "frequently, almost constantly or daily" and "often or very often"
# Manually assign CVs for data using unusual duration categories (shorter than recall) that get mismarked by assignments above 
gerd_procdt[duration=="3 months" & frequency=="at least twice a week", `:=` (cv_more_req_freq = 0, cv_less_req_freq_dur = 1)]
gerd_procdt[duration=="3 months" & frequency=="at least weekly", cv_less_req_freq_dur := 1]
gerd_procdt[duration=="3 months" & frequency=="daily", `:=` (cv_more_req_freq = 1, cv_less_req_freq_dur = 0)]
gerd_procdt[duration=="1 month" & frequency=="at least weekly", cv_less_req_freq_dur := 1]
table(gerd_procdt$cv_more_req_freq, gerd_procdt$cv_less_req_freq_dur)

gerd_procdt[ , cv_score:=0]
gerd_procdt[cv_questionaire==1 & frequency=="score", cv_score:=1]
table(gerd_procdt$cv_score)
gerd_procdt <- gerd_procdt[cv_score==1, c("cv_optsx", "cv_reqsx", "cv_more_req_freq", "cv_less_req_freq_dur", "cv_long_recall", "cv_short_recall"):=0]

# Effect of the above:
# Applies one or more study-level covariates to cv_questionaire data with non-std questionaire interpretation; reference data and cv_questionaire = 0 data are marked 0 for all these new cvs; cv_score incorporates all other features and are marked 0 for them

# To get predictions later, will need values for all cvs being crosswalked (can run model with missings for 0s, but can't predict)
gerd_procdt[cv_admin_data==1, `:=` (cv_heartburn_only = 0, cv_regurgitation_only = 0)]

############### MAKE COLUMN LISTS FOR LATER
## Make cv lists for making case definitions and MR-BRT covariate lists

# List of cvs that are useful tags for manipulating data, but not bias variables to crosswalk
cv_manip <- c("cv_literature", "cv_survey", "cv_marketscan_2000", "cv_marketscan_other", "cv_marketscan_data")

# List of cvs that are marked in the raw data and I looked at possibly crosswalking, but saw no evidence of systematic bias and collapsed into an aggregate category
cv_collapse <- c("cv_mail", "cv_facility", "cv_home", "cv_phone", "cv_internet", "cv_public", "cv_waitingrm", "cv_work", "cv_school", "cv_profession", "cv_clinic_or_hospital", "cv_gi_spec", "cv_pharma", "cv_diag_selfreport", "cv_endoscopy", "cv_chart_review", "cv_long_recall", "cv_score")

# List of cvs that will be marked positive in some or all reference data
cv_ref <- c("cv_questionaire", "cv_chart_review")

# Combined list of cvs to drop in match-finding (but keep in raw data)
cv_drop <- c(cv_manip, cv_collapse, cv_ref)

# List of cvs that positively identify components of alternative case defs
cv_alts <- c("cv_admin_data", "cv_heartburn_only", "cv_regurgitation_only", "cv_optsx", "cv_reqsx", "cv_short_recall", "cv_more_req_freq", "cv_less_req_freq_dur")

############## ADD CASE DEFINITIONS and SUBSET DATA NEEDING TRANSFORMATION AND MODELING DATA FOR FUTURE USE
gerd_procdt <- get_definitions(gerd_procdt)
table(gerd_procdt$definition)
write.xlsx(gerd_procdt, paste0(out_path, "/3059_unadj_procdt_", date, ".xlsx"), col.names=TRUE)

gerd_proctotranform <- gerd_procdt[definition!="reference", ]
gerd_proc_ref <- gerd_procdt[definition=="reference", ]

## Create modeling set
gerd_moddt <- gerd_procdt[(group_review==1 | is.na(group_review)) & cv_marketscan_data!=1, ]
write.xlsx(gerd_moddt, paste0(out_path, "/3059_unadj_moddt", date, ".xlsx"), col.names=TRUE)

######################### CROSSWALKING

############## FIND MATCHES
gerd_findmatch <- copy(gerd_procdt)
gerd_findmatch <- subnat_to_nat(gerd_findmatch)
gerd_findmatch <- calc_year(gerd_findmatch)

scatter_bydef(gerd_findmatch, upper = 1, width = 50, height = 20)
#scatter_bydef(gerd_findmatch[definition=="reference",], upper = 1)

age_dts <- get_age_combos(gerd_findmatch)
gerd_findmatch <- age_dts[[1]]
age_match_dt <- age_dts[[2]] 

pairs <- combn(gerd_findmatch[, unique(definition)], 2)
#combn function generates all combinations of the elements of vector x (in this case, a vector of the unique values found in the definition column of the data.table tpud_xw) taken m (in this case, 2) at a time

matches <- rbindlist(lapply(1:dim(pairs)[2], function(x) get_matches(n = x, pair_dt = gerd_findmatch)))
matches[ , nid_comb:=paste0(nid, nid2)]

write.xlsx(matches, paste0(out_path, "/3059_allmatches2_", date, ".xlsx"), col.names=TRUE)

## Previously trialed a model with all matches where necessary and only intra-study matches for the contrasts where this is possible, also trialed a model using only interstudy matches, had similar betas to using all matches

########################## FORMAT AND VISUALIZE MATCHES
ratios <- as.data.table(read_excel(paste0(out_path, "/3059_allmatches2_", date, ".xlsx")))

## Calculate ratios and their standard errors, drop unneeded variables, calculate dummies, visualize ratios, take their logs and calc standard error of their logs
#Log-ratios are no longer the preferred input variable for MR-BRT modeling, but still useful visualization
ratios <- calc_ratio(ratios)
#This line is for dropping matches with infinite or zero values for the ratios
ratios <- drop_stuff(ratios)
ratios <- add_compdumminter(ratios)

lratios <- logtrans_ratios(ratios)

## Calculate logits, logit differences, and standard errors to go into meta-regression
logit_differences <- calc_logitdf(lratios)

## Make Forest Plots
for (covar in cv_alts) {
  logit_diff_cv <- logit_differences[(grepl(covar, def)|grepl(covar, def2)) & !(grepl(covar, def) & grepl(covar, def2)), ]
  logit_diff_cv[grepl(covar, def), group:="num"]
  logit_diff_cv[grepl(covar, def2), group:="den"]
  logit_diff_cv <- logit_diff_cv[order(group)]
  
  pdf(file = paste0(out_path, "/", covar, "_", date, "_forest.pdf"), width=40, height=40)
  forest_plot <- forest(logit_diff_cv$lratio, logit_diff_cv$log_ratio_se, slab=paste0(logit_diff_cv$nid, ", ", logit_diff_cv$nid2, ", ", logit_diff_cv$comparison))
  dev.off()
}

## Pick list of cvs for components of alternative case definitions and make into a list MR-BRT will understand

for(c in cv_alts){
  cov <- cov_info(c, "X", "network")
  if(c == cv_alts[1]){
    cov_list <- list(cov)
  } else {
    cov_list <- c(cov_list, list(cov))
  }
}

## Run various MR-BRT models to decide on best fit
## Chosen MR-BRT model
gerd_fit2 <- run_mr_brt(
  output_dir = out_path,
  model_label = paste0("gerd_allmatches2_", date),
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

##### MAKE MODEL PREDICTIONS

## Get predictions for training data (proc data with matches)
gerd_trainingprediction2 <- unique(predict_xw(gerd_fit2, "logit_dif"), by = cv_alts)
write.xlsx(gerd_trainingprediction2, paste0(out_path, "/3059_trainpred2", date, ".xlsx"), col.names=TRUE)

## Get predictions for all non-ref proc data (with or without matches)
gerd_predictnew2 <- unique(predict_xw(gerd_fit2, "logit_dif", gerd_proctotranform), by = cv_alts)
write.xlsx(gerd_predictnew2, paste0(out_path, "/3059_newpred2", date, ".xlsx"), col.names=TRUE)

##### TRANSFORM DATA TO VISUALIZE EFFECT
 
## Select and transform non-reference processing data to visualize the effect of crosswalking
gerd_transform2 <- transform_altdt(gerd_proctotranform, gerd_predictnew2, "logit_dif")
 
by_vars <- c("nid", "location_id", "year_start", "year_end", "age_start", "age_end", "case_name", "specificity")
pre_post_dt <- merge(gerd_proctotranform, gerd_transform2, by = by_vars)
mean_v_mean(pre_post_dt, "allmatches_mod2")

############################## SEX RATIO ESTIMATION

# Use sex-specific rows in processing set to estimate sex-ratio in MR-BRT
gerd_sxproc <- gerd_procdt[sex != "Both", ]

gerd_sxproc <- sex_yearage(gerd_sxproc)
sex_ratios <- get_sex_ratios(gerd_sxproc)
sex_ratios <- drop_stuff(sex_ratios)

ratio_byage(sex_ratios)

sex_lratios <- logtrans_ratios(sex_ratios)
sex_histo(sex_lratios)
 
## No apparent differences in sex-ratio by age or super-region 
 
## Model sex ratios from sex-specific data
 
sexratio3059_fit <- run_mr_brt(
  output_dir = out_path,
  model_label = paste0("sexratio_gerd_", date),   
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
sexratio_training_predictions <- predict_fromtrain(sexratio3059_fit)
write.xlsx(sexratio_training_predictions, paste0(out_path, "/3059_sexrat_trainpred", date, ".xlsx"), col.names=TRUE)

############################ INTRA-STUDY AGE-SEX SPLITTING

## Look in modeling data for groups where both age-specific and sex-specific data have been included (but age-sex-specific data have not been)
gerd_intRAsplit <- copy(gerd_moddt)
# Within a group, count how many rows have specificity for age AND sex
gerd_intRAsplit <- gerd_intRAsplit[ , split := length(specificity[grepl("age", specificity) & grepl("sex", specificity)]), by = group]
table(gerd_intRAsplit$split)
# Pick out rows that are age-specific OR sex-specific, and not part of groups that have age-sex specific rows
gerd_intRAsplit <- gerd_intRAsplit[(grepl("age", specificity)|grepl("sex", specificity)) & split==0, ]
table(gerd_intRAsplit$split)
table(gerd_intRAsplit$specificity)

# Pick out the groups that have both age-specific AND sex-specific rows (among those that don't have age-sex-specific rows), added to shared code version because specificity variable for groups was defined using more features of data than just age and sex
gerd_intRAsplit[ , has_age := length(specificity[grepl("age", specificity)]), by = group]
gerd_intRAsplit[ , has_sex := length(specificity[grepl("sex", specificity)]), by = group]
table(gerd_intRAsplit$has_age, gerd_intRAsplit$has_sex, useNA = "always")
gerd_intRAsplit <- gerd_intRAsplit[has_age!=0 & has_sex!=0, ]
table(gerd_intRAsplit$has_age, gerd_intRAsplit$has_sex, useNA = "always")

## Hold on to observations that don't require this processing to bind back later
intRA_drop <- unique(gerd_intRAsplit$seq)
gerd_mod_intra_dropped <- gerd_moddt[!(seq %in% intRA_drop), ]

## Visualize observations to be processed
ggplot(gerd_intRAsplit, aes(age_start, mean, color = sex)) + geom_point()
ggsave(paste0(out_path, "/intRAsplit_agePRE_", date, ".pdf"), width=10, height=7, limitsize=FALSE)

## Make the sex ratios and assoc uncertainty columns within each group that needs splitting; all of these steps also calculate age ratios, but we will drop those and use the sex ones
# Calculate the proportion of cases of the total represented by each age-specific or sex-specific observation
gerd_intRAsplit[ , cases_total := sum(cases), by = list(group, specificity, case_name, location_id)]
gerd_intRAsplit[ , prop_cases := round(cases / cases_total, digits = 3)]
print(gerd_intRAsplit[is.na(cases_total), ])

# Calculate the proportion of sample size of the total represented by each age-specific or sex-specific observation
gerd_intRAsplit[ , samp_total := sum(sample_size), by = list(group, specificity, case_name, location_id)]
gerd_intRAsplit[ , prop_samp := round(sample_size / samp_total, digits = 3)]

# CALC SE
gerd_intRAsplit[, se_cases:= sqrt(prop_cases*(1-prop_cases) / cases_total)]
gerd_intRAsplit[, se_samp:= sqrt(prop_samp*(1-prop_samp) / samp_total)]

# Calculate sex ratios
gerd_intRAsplit[, intRA_ratio := round(prop_cases / prop_samp, digits = 3)]
gerd_intRAsplit[, se_ratio:= round(sqrt( (prop_cases^2 / prop_samp^2)  * (se_cases^2/prop_cases^2 + se_samp^2/prop_samp^2) ), digits = 3)]
intRA_ratio <- gerd_intRAsplit[grepl("sex", specificity), list(nid, location_id, group, sex, measure, intRA_ratio, se_ratio, prop_cases, prop_samp, year_start, year_end, case_name)]

# Copy age-specific data, rename as age-sex-specific, drop the ratios and assoc columns created above for age
intRA_new <- copy(gerd_intRAsplit[grepl("age", specificity),])
table(intRA_new$specificity)
intRA_new$specificity <- gsub("age", "age, sex", intRA_new$specificity)
table(intRA_new$specificity)
intRA_new[ , c("intRA_ratio", "se_ratio", "split","cases_total", "prop_cases", "samp_total", "prop_samp", "se_cases", "se_samp") := NULL]

# Make additional copies for Male and Female, bind into a single table, merge on the appropriate sex-ratio from above
intRA_female <- copy(intRA_new[ , sex:= "Female"])
intRA_male <- copy(intRA_new[ , sex:= "Male"])
intRA_split <- rbind(intRA_female, intRA_male)
intRA_split <- merge(intRA_split, intRA_ratio, by = c("nid", "measure", "group", "case_name", "year_start", "year_end", "sex", "location_id") )

# Calculate new means for female and male rows from original both-sex, age-specific means and the sex-ratios from their groups

## CALC MEANS and new standard error and sample size; drop ratio-related columns when no longer needed and wipe out uncertainty info that wasn't updated in these steps
intRA_split[, standard_error := round(sqrt(standard_error^2 * se_ratio^2 + standard_error^2 * intRA_ratio^2 + se_ratio^2 * mean^2), digits = 4)]
intRA_split[, mean := mean * intRA_ratio]
intRA_split[, cases := round(cases * prop_cases, digits = 0)]
intRA_split[, sample_size := round(sample_size * prop_samp, digits = 0)]
intRA_split[, `:=` (input_type = "split", age_issue = 1, sex_issue = 1)]
intRA_split[,note_modeler := paste(note_modeler, "| age-specific row split using intra-study sex ratio", round(intRA_ratio, digits = 2))]
intRA_split[,c("intRA_ratio", "se_ratio", "prop_cases", "prop_samp") := NULL]
intRA_split[, c("lower", "upper", "uncertainty_type_value") := NA]

## Visualize processed observations
ggplot(intRA_split, aes(age_start, mean, color = sex)) + geom_point()
ggsave(paste0(out_path, "/intRAsplit_POST_", date, ".pdf"), width=10, height=7, limitsize=FALSE)

## Update seq and crosswalk_parent_seq columns for processed observations, bind back to data that didn't require this step
intRA_split_seqsupdated <- update_seq(intRA_split)
gerd_moddt2 <- rbind(gerd_mod_intra_dropped, intRA_split_seqsupdated, use.names = TRUE, fill=TRUE)

write.xlsx(gerd_moddt2, paste0(out_path, "/3059_moddt_agesexsp_applied", date, ".xlsx"), col.names=TRUE)
setdiff(gerd_moddt$nid, gerd_moddt2$nid)

#################### APPLY MR-BRT SEX RATIO TO BOTH-SEX MODELING DATA
## Get remaining both-sex modeling data
gerd_bothsxmod <- gerd_moddt2[sex=="Both", ]

## Save data that don't require this processing to bind back later
gerd_mod_bothsx_dropped <- gerd_moddt2[sex!="Both", ]

## Make mid-age variable for making visualization and then visualize observations to be processed
gerd_bothsxmod <- gerd_bothsxmod[ , age_mid := (age_end-age_start)/2 + age_start]
print(gerd_bothsxmod[ , c("age_start", "age_end", "age_mid")])

ggplot(gerd_bothsxmod, aes(age_start, mean)) + geom_point()
ggsave(paste0(out_path, "/bothsx_PRE_ratio", date, ".pdf"), width=10, height=7, limitsize=FALSE)

## Estimate sample sex-ratios in modeling data as ppln sex-ratios
gerd_bothsxmod <- get_sex_ppln(gerd_bothsxmod, "step2", age_dt = age_dt2)

## Get estimated sex-ratio from MR-BRT, and transform both-sex prevalence to separate male and female prevalence
## Predict ratios from model for training data
sexratio_new_predictions <- predict_new(sexratio3059_fit, gerd_bothsxmod, by = "none")
gerd_bothsxmod <- transform_bothsexdt(gerd_bothsxmod, sexratio_new_predictions, by = "none") 
table(gerd_bothsxmod$specificity, gerd_bothsxmod$group)

## Visualize transformed observations
ggplot(gerd_bothsxmod, aes(age_start, mean, color = sex)) + geom_point()
ggsave(paste0(out_path, "/bothsx_POST_ratio", date, ".pdf"), width=10, height=7, limitsize=FALSE)

## Update seqs in processed observations, bind back to the modeling data that didn't require this processing step
gerd_bothsxmod_seqsu <- update_seq(gerd_bothsxmod)
gerd_moddt3 <- rbind(gerd_mod_bothsx_dropped, gerd_bothsxmod_seqsu, use.names = TRUE)

write.xlsx(gerd_moddt3, paste0(out_path, "/3059_moddt_agesex_sexrat_applied", date, ".xlsx"), col.names=TRUE)

###################### APPLY CROSSWALK COEFFICIENTS FOR NON-REFERENCE STUDY DESIGNS FROM MR-BRT
## Get modeling data requiring crosswalk for non-reference case definition
gerd_mod_nonref <- gerd_moddt3[definition!="reference", ]

## Save reference modeling data to bind back later
gerd_mod_ref <- gerd_moddt3[definition=="reference", ]

## Predict crosswalks for non-reference modeling data
gerd_predictmod <- unique(predict_xw(gerd_fit1, "logit_dif", gerd_mod_nonref), by = cv_alts)

## Transform nonreference modeling data
gerd_mod_nonref_x <- transform_altdt(gerd_mod_nonref, gerd_predictmod, "logit_dif")

## Visualize effect
pre_post_dt <- merge(gerd_mod_nonref, gerd_mod_nonref_x, by = c("seq", "crosswalk_parent_seq", "age_start", "sex"))
mean_v_mean(pre_post_dt, "xw_mod_dt")

## Update seqs in transformed observations, bind back to reference modeling data
gerd_mod_nonref_x_seqsu <- update_seq(gerd_mod_nonref_x)
gerd_moddt4 <- rbind(gerd_mod_ref, gerd_mod_nonref_x_seqsu, use.names = TRUE)

write.xlsx(gerd_moddt4, paste0(out_path, "/3059_moddt_agesex_sexrat_xw_applied", date, ".xlsx"), col.names=TRUE)

###################### APPLY EXTERNAL AGE-PATTERN TO REMAINING BROAD-AGE DATA
gerd_intERsplit <- copy(gerd_moddt4)

gerd_intERsplit <- dis_age_split(gerd_intERsplit, age_dt2, gbd_round_id = 6, age_pattern_loc = 1, measure = 5, age_pattern_mod_ent = 18664, step = "step1")

gerd_moddt5 <- update_seq(gerd_intERsplit)

write.xlsx(gerd_moddt5, paste0(out_path, "/3059_moddt_alltransforms_applied", date, ".xlsx"), col.names=TRUE)

##################### SAVE A XW VERSION WITH NO OUTLIERS MARKED
# Revert cv_* to *_cv
setnames(gerd_moddt5, old = c("cv_work", "cv_school", "cv_profession", "cv_gi_spec", "cv_pharma", "cv_waitingrm"), new = c("work_cv", "school_cv", "profession_cv", "gi_spec_cv", "pharma_cv", "waitingrm_cv"))
print(names(gerd_moddt5))

ggplot(gerd_moddt5, aes(age_start, mean)) + geom_point()
ggsave(paste0(out_path, "/3059_final_dt", date, ".pdf"), width=10, height=7, limitsize=FALSE)

gerd_moddt6 <- gerd_moddt5[ , c(bundle_columns, "crosswalk_parent_seq"), with=FALSE]

#Write the data to an Excel file in order to upload it  
upload_path <- paste0(j, "FILEPATH/3059_noout_xw1", date, ".xlsx")
write.xlsx(gerd_moddt6, upload_path, col.names=TRUE, sheetName = "extraction")

#Then add a description and upload
description <- "same approach as 7058, but collapse long recall"
gerd_upload_xw6 <- save_crosswalk_version(bundle_version_id=10913, data_filepath=upload_path, description = description)
#Crosswalk version ID 7091

##################### MARK OUTLIERS AND UPLOAD ADD'L XW VERSION
# Outlier young subnational data with excessive effect on pseudo-random effects in GBD 2017, plus similar Sardegna point
gerd_moddt7 <- gerd_moddt6[nid==323975, is_outlier:=1]
gerd_moddt7[nid==323910 & (age_start==0 | age_start==11), is_outlier:=1]
gerd_moddt7[nid==323879, is_outlier:=1]
gerd_moddt7[nid==323933, is_outlier:=1]
gerd_moddt7[nid==323863, is_outlier:=1]

#Write the data to an Excel file in order to upload it  
upload_path <- paste0(j, "FILEPATH/3059_youngoutlier", date, ".xlsx")
write.xlsx(gerd_moddt7, upload_path, col.names=TRUE, sheetName = "extraction")

#Then add a description and upload
description <- "same approach as 7061, but collapse long recall"
gerd_upload_xw7 <- save_crosswalk_version(bundle_version_id=10913, data_filepath=upload_path, description = description)
#Crosswalk version ID 7094






