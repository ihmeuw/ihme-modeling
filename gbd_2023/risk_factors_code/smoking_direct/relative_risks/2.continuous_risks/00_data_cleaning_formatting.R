#---------------------------------------------------
# Purpose: cleaning the tobacco data for mr-brt es
# Date: 11/01/2020
#---------------------------------------------------

rm(list=ls())

library(tidyverse)
library(data.table)
source(FILEPATH)
args <- commandArgs(trailingOnly = TRUE)


if(interactive()){
  # NOTE: the ro_pair for this script does not include age-specific info
  ro_pair <- "stroke" #cataracts
  out_dir <- FILEPATH
  WORK_DIR <- FILEPATH
  setwd(WORK_DIR)
  source(FILEPATH)
} else {
  ro_pair <- args[1]
  out_dir <- args[2]
  WORK_DIR <- args[3]
  setwd(WORK_DIR)
  source("./config.R")
}

cause <- ro_pair

# read in data
input_dir <- FILEPATH
input_file <- list.files(input_dir, full.names = T)[grepl(cause, list.files(input_dir, full.names = T))]

# pick the most recent file
input_file <- sort(input_file, decreasing = T)[1]

dt_orig <- fread(input_file)

confounders <- names(dt_orig)[grepl('confounders_', names(dt_orig))]
cvs <- names(dt_orig)[grepl('cv_', names(dt_orig))]

if(ro_pair %in% c("ihd", "stroke", "afib_and_flutter", "peripheral_artery_disease", "aortic_aneurism")){
  var_names <- c('include_in_model', 'nid', 'effect_size_log', 'se_effect_log', 'custom_exp_level_lower', 'custom_exp_level_upper', 'custom_unexp_level_lower', 'custom_unexp_level_upper',
                 'mean_exp', "age_start", "age_end", "age_mean", "age_sd", "duration_fup_measure","duration_fup_units","value_of_duration_fup",
                 "adjustment", "percent_male", confounders, cvs)
} else {
  var_names <- c('include_in_model', 'nid', 'effect_size_log', 'se_effect_log', 'custom_exp_level_lower', 'custom_exp_level_upper', 'custom_unexp_level_lower', 'custom_unexp_level_upper',
                 'mean_exp', "age_start", "age_end", "adjustment", "percent_male", confounders, cvs)
}

# remove the 413798 b/c it is a cross-sectional study
if(ro_pair=="diabetes"){
  dt_orig[nid==413798, include_in_model := 0]
}

# fix the wrong NID
dt_orig[nid==385161, nid := 358161]

# fix some include_in_model column for lung_cancer
if(ro_pair=="lung_cancer"){
  dt_orig[nid==426275, include_in_model := 1]
}

# keep only include_in_model==1 data
dt_sub <- dt_orig[include_in_model==1, var_names, with=F]

# check stroke duplicated data
dt_sub[nid %in% c(344308, 343400) & include_in_model == 1]

# remove 343400 for stroke and 356277 for copd
dt_sub <- dt_sub[nid != 343400]
dt_sub <- dt_sub[nid != 356277]

# check the missingness of the data again
summary(dt_sub)

# for confounders, replace NA with 0
for (j in c(confounders, "adjustment")){
  set(dt_sub,which(is.na(dt_sub[[j]])),j,0)
}

# for cv covariates, 1 is bad, so replacing NA to 1 if necessary (very rare)
for (j in c(cvs)){
  set(dt_sub,which(is.na(dt_sub[[j]])),j,1)
}

# replace missing age_start and age_end with the median value (this is fine for non-cvd outcomes since age is not relavent. May be problematic for CVDs though)
set(dt_sub, which(is.na(dt_sub[["age_start"]])),"age_start", median(dt_sub[["age_start"]], na.rm = T))
set(dt_sub, which(is.na(dt_sub[["age_end"]])),"age_end", median(dt_sub[["age_end"]], na.rm = T))
dt_sub[is.na(percent_male), percent_male := 0.5]

# check the missingness of the data again
summary(dt_sub)

# adding cv_older
dt_sub[ ,cv_older := 0]
dt_sub[age_start>50, cv_older := 1]

dt_sub[ , cv_adj := as.numeric()]

dt_sub[,adj_age_sex := 0]
dt_sub[confounders_age==1 & confounders_sex==1, adj_age_sex := 1]

# cv_adj=3 if age or sex is not adjusted
dt_sub[adj_age_sex==0, cv_adj := 3]

# cv_adj=2 if only age and sex are adjusted
dt_sub[adj_age_sex==1 & adjustment==2, cv_adj := 2]

# cv_adj=1 if age+sex + 3 more covariates are adjusted
dt_sub[adj_age_sex==1 & adjustment>2 & adjustment<=5, cv_adj := 1]

# cv_adj=0 if age+sex + more than 3 covariates are adjusted
dt_sub[adj_age_sex==1 & adjustment>5, cv_adj := 0]

# check whether there is missing in cv_adj
message("there is ", nrow(dt_sub[is.na(cv_adj)]), " missing values in cv_adj")

# add cascading dummies
dt_sub[, cv_adj_L0 := 1]
dt_sub[, cv_adj_L1 := 1]
dt_sub[, cv_adj_L2 := 1]

# if cv_adj==0, change all dummies to be 0
dt_sub[cv_adj==0, c("cv_adj_L0", "cv_adj_L1", "cv_adj_L2") := 0]
# if cv_adj==1, change cv_adj_L1 and cv_adj_L2 to be 0
dt_sub[cv_adj==1, c("cv_adj_L1", "cv_adj_L2") := 0]
# if cv_adj==2, change cv_adj_L2 to be 0
dt_sub[cv_adj==2, c("cv_adj_L2") := 0]

# remove cv_adj
dt_sub[, cv_adj := NULL]

# change the exposed and unexposed name
dt_sub <- setnames(dt_sub, old = c("custom_exp_level_lower", "custom_exp_level_upper", "custom_unexp_level_lower", "custom_unexp_level_upper"), 
                        new = c("b_0", "b_1", "a_0", "a_1"))

dt_sub <- setnames(dt_sub, old = c("effect_size_log", "se_effect_log"), 
                   new = c("ln_effect", "ln_se"))

# fix ro_pair specific issues
if(cause=="copd"){
  dt_sub[age_end > 99 , age_end := 99]
}

# maybe this should be applied to all ro_pair
# for nasopharyngeal_cancer, there are three rows that have b1<=b0
if(cause=="nasopharyngeal_cancer"){
  print(paste0('nids of studies where b_0 > b_1: ',unique(dt_sub[b_0>b_1, nid])))
  print(paste0('nids of studies where b_0 = b_1 = 0: ',unique(dt_sub[b_0==0 & b_1==0, nid])))
  dt_sub <- dt_sub[b_0 < b_1]
}

# remove extreme values for ihd
if(cause == "ihd"){
}

# remove extreme values for peripheral_artery_disease
if(cause == "peripheral_artery_disease"){
}

# remove extreme values for copd
if(cause == "copd"){
}

# remove extreme values for liver_cancer
if(cause == "liver_cancer"){
}

age_dt <- get_age_metadata(19,gbd_round_id = 7)

cvd_ro <- c("ihd", "stroke", "afib_and_flutter", "peripheral_artery_disease", "aortic_aneurism")

if(ro_pair %in% cvd_ro){
  # for CVD outcomes, adjust the data here
  # if no age_mean, then use the mid point of age_start and age_end as the age_mean
  dt_sub[is.na(age_mean), age_mean := (age_start+age_end)/2]
  
  # calculate the missing mean age: mean_age = mean age at enrollment + mean/median fup time
  dt_sub[duration_fup_measure %in% c("mean", "median") & duration_fup_units=="years", mean_age := age_mean + value_of_duration_fup]
  
  # if unit is max, mean fup duration= max fup duration/2
  dt_sub[duration_fup_measure=="max", value_of_duration_fup := value_of_duration_fup/2]
  dt_sub[is.na(mean_age) & duration_fup_units=="years", mean_age := age_mean + value_of_duration_fup]
  
  # if mean_age is still missing, using the mean of duration of fup from the data
  mean_of_age <- dt_sub[duration_fup_units=="years", value_of_duration_fup] %>% mean(., na.rm = T)
  dt_sub[is.na(mean_age), mean_age := (age_start+age_end)/2 + mean_of_age]
}

##-------------------------------------------------------------------------
## include the re-extracted RR data (from Gabi) and format the data a bit 
##-------------------------------------------------------------------------
extra_data <- fread(FILEPATH)[cause==ro_pair & include==1]
# create effect_size_log and se of effect_size_log
extra_data[,ln_effect := log(mean)]
extra_data[, ln_se := (log(upper)-log(lower))/3.92]
# create exposure range of the exposed and the unexposed group
extra_data[, `:=`(b_0= exp_lower, b_1= exp_upper)]
# assume the exposure range for the unexposed is (0,0)
extra_data[, `:=`(a_0= 0, a_1= 0, mean_exp=(exp_lower+exp_upper)/2)]
# create a percent male variable, assuming 0.5 when sex==both
extra_data[sex=="Male", percent_male := 1]
extra_data[sex=="Female", percent_male := 0]
extra_data[sex=="Both", percent_male := 0.5]

# for CVD outcomes, calculate age=age.mid+length.of.follow-up/2 for cohort study
if(ro_pair %in% cvd_ro){
  extra_data[, mean_age := ifelse(grepl("Cohort",study.type),age.mid + `length.of.follow-up`/2, age.mid)]
} else {
  extra_data[, mean_age := age.mid]
}

cv_name <- names(extra_data)[grepl("cv_", names(extra_data))]
if(!is_empty(extra_data)){
  # for cv covariates, 1 is bad, so replacing NA to 1 if necessary (very rare)
  for (j in c(cv_name)){
    set(extra_data,which(is.na(extra_data[[j]])),j,1)
  }
  
  # replace missing age_start and age_end with the median value (this is fine for non-cvd outcomes since age is not relavent. May be problematic for CVDs though)
  set(extra_data, which(is.na(extra_data[["age_start"]])),"age_start", median(dt_sub[["age_start"]], na.rm = T))
  set(extra_data, which(is.na(extra_data[["age_end"]])),"age_end", median(dt_sub[["age_end"]], na.rm = T))
  extra_data[is.na(percent_male), percent_male := 0.5]
}

# keep variable that are needed
keep.var <- c("nid","ln_effect","ln_se","b_0","b_1","a_0","a_1",
              "mean_exp","age_start","age_end", "age.mid", "mean_age", "percent_male", "cv_subpopulation","cv_exposure_selfreport",                 
              "cv_exposure_study","cv_outcome_selfreport","cv_older","cv_adj")
extra_data <- extra_data[,keep.var, with=F]

# change some var type
extra_data[, (cv_name) := lapply(.SD, as.integer), .SDcols=cv_name]

summary(extra_data)

# add cascading dummies
extra_data[, cv_adj_L0 := 1]
extra_data[, cv_adj_L1 := 1]
extra_data[, cv_adj_L2 := 1]

# if cv_adj==0, change all dummies to be 0
extra_data[cv_adj==0, c("cv_adj_L0", "cv_adj_L1", "cv_adj_L2") := 0]
# if cv_adj==1, change cv_adj_L1 and cv_adj_L2 to be 0
extra_data[cv_adj==1, c("cv_adj_L1", "cv_adj_L2") := 0]
# if cv_adj==2, change cv_adj_L2 to be 0
extra_data[cv_adj==2, c("cv_adj_L2") := 0]

##----------------------------------------------

# add extra data to dt_sub
dt_sub <- rbindlist(list(dt_sub, extra_data), use.names = T, fill = T)

# remove cv_confounding_uncontrolled, cv_adj accounts for this already
dt_sub[, cv_confounding_uncontrolled := NULL]
dt_sub[, cv_adj := NULL]

# create reference age group (specifically for CVD outcomes)
dt_sub[, age_ref := ifelse(ro_pair %in% cvd_ro, weighted.mean(mean_age, 1/ln_se, na.rm=T), 15)]

bias_covs <- names(dt_sub)[grepl('cv_', names(dt_sub))]
incl_vars <- c("nid", "ln_effect", "ln_se", ALT_EXPOSURE_COLS, REF_EXPOSURE_COLS, 'percent_male', 'age_start', 'age_end', "age_ref", bias_covs)

# check whether there is any missing value in the included variables
if(dt_sub[, incl_vars, with=F] %>% is.na %>% any){
  message("there are missing values in the required variables... please check")
} else {
  message("there are no missing values! Good to go!")
}

summary(dt_sub)
  
# save the data
write.csv(dt_sub, file.path(FILEPATH, paste0(cause, '.csv')), row.names = F)

# save the .RDS file
obs_var <- "ln_effect"; obs_se_var <- "ln_se"
ref_vars <- c("a_0", "a_1"); alt_vars <- c("b_0", "b_1")
allow_ref_gt_alt = FALSE
drop_x_covs <- drop_z_covs <- keep_x_covs <- keep_z_covs <- NA
diet_dir <- FILEPATH
study_id_var <- "nid"
verbose <- TRUE

# NOTE: this function will drop any incomplete rows. No missing values is allow.  
out <- prep_diet_data(ro_pair=ro_pair, obs_var, obs_se_var, ref_vars, alt_vars, allow_ref_gt_alt = FALSE,
                      study_id_var = study_id_var,
                      drop_x_covs = NA, keep_x_covs = NA, drop_z_covs = NA, keep_z_covs = NA,
                      diet_dir = diet_dir,
                      verbose = TRUE)
print(out)
message(paste0('saving cleaned data..'))
saveRDS(out, paste0(FILEPATH, "00_prepped_data/", ro_pair, ".RDS"))
message(paste0('saved cleaned data to: ', paste0(FILEPATH, "00_prepped_data/", ro_pair, ".RDS")))

