#-------------------------------------------------------
# Purpose: cleaning the extracted age-specific RR data
#-------------------------------------------------------

rm(list = ls())
options(max.print=5000)
library(dplyr)
library(mrbrt001, lib.loc = "FILEPATH")
library(stringr)
library(readr)
library(lme4)
library(car)
library(msm)

source("FILEPATH/get_age_metadata.R")
np <- import("numpy")
np$random$seed(as.integer(123))

args <- commandArgs(trailingOnly = TRUE)

if(interactive()){
  ro_pair <- "aortic_aneurism"
  out_dir <- "FILEPATH"
  WORK_DIR <- 'FILEPATH'
  inlier_pct <- 0.9
  setwd(WORK_DIR)
  source("FILEPATH/config.R")
} else {
  ro_pair <- args[1]
  out_dir <- args[2]
  WORK_DIR <- args[3]
  inlier_pct <- as.numeric(args[4])
  setwd(WORK_DIR)
  source("./config.R")
}

##################################################
## Estimate the relationship between age and RR ## 
##################################################

# load the age-stratified RR data
age_rr_dt_dir <- 'FILEPATH'
age_rr_dt <- fread(file.path(age_rr_dt_dir, 'all_extractions_full.csv'))

#-------------------------------------
# data cleaning

# fix remove duplicated rows for nid 165539
age_rr_dt[, nid := as.integer(parse_number(nid))]
age_rr_dt[nid==165539 & !grepl('United States',location_name), exclusion_indicator := 1]

# remove exclusion_indicator==1
age_rr_dt <- age_rr_dt[is.na(exclusion_indicator),]
age_rr_dt[, row_num := as.integer(rownames(age_rr_dt))]

# add a few columns------------------
age_rr_dt[, log_effect_size := log(effect_size)]
age_rr_dt[, log_se := (log(upper)-log(lower))/3.92]


# add outcome categories-------------
age_rr_dt[, outcome] %>% unique
age_rr_dt[, cv_outcome_stroke := ifelse(grepl("stroke|hemorrh", outcome, ignore.case = T),1,0)]
age_rr_dt[, cv_outcome_ihd := ifelse(grepl("heart disease", outcome, ignore.case = T),1,0)]
age_rr_dt[, cv_outcome_pad := ifelse(grepl("peripheral", outcome, ignore.case = T),1,0)]
age_rr_dt[, cv_outcome_afib := ifelse(grepl("fibrillation", outcome, ignore.case = T),1,0)]
age_rr_dt[, cv_outcome_aa := ifelse(grepl("aneurysm", outcome, ignore.case = T),1,0)]
age_rr_dt[, cv_outcome_cvds := ifelse(grepl("cardiovascular", outcome, ignore.case = T),1,0)]

# add design indicator prospective cohort vs. others-------
age_rr_dt[, design] %>% unique
age_rr_dt[, cv_design_pc := ifelse(grepl("prospective", design, ignore.case = T),1,0)]

# add measurement indicator------
age_rr_dt[, effect_size_measure] %>% unique
age_rr_dt[, cv_measurement_hr := ifelse(grepl("hazard", effect_size_measure, ignore.case = T),1,0)]
age_rr_dt[, cv_measurement_rr := ifelse(grepl("relative", effect_size_measure, ignore.case = T),1,0)]
age_rr_dt[, cv_measurement_or := ifelse(grepl("odds", effect_size_measure, ignore.case = T),1,0)]

# add cv_population and cv_rep_geography indicator----
age_rr_dt[, cv_population := custom_rep_population]
age_rr_dt[, cv_rep_geography := rep_geography]

# multilocation are population and geographically representative
age_rr_dt[is.na(rep_geography),location_name] %>% unique
age_rr_dt[is.na(rep_geography), cv_rep_geography:=1]

age_rr_dt[is.na(custom_rep_population),location_name] %>% unique
age_rr_dt[is.na(custom_rep_population), cv_population:=1]

# count number of adjusted covaraites------
age_rr_dt[, adjustment := 0]

# for confounders, replace NA with 0
confounders <- names(age_rr_dt)[grepl('confounders',names(age_rr_dt)) & !grepl('other',names(age_rr_dt))]
for (j in c(confounders)){
  set(age_rr_dt,which(is.na(age_rr_dt[[j]])),j,0)
}

# count number of other confounders
age_rr_dt[, has_comma := ifelse(grepl(',',confounders_other), 1, 0)]
age_rr_dt[has_comma==1, confounders_other]

# count number of ";" for number of covariates
age_rr_dt[, confounders_other_num := str_count(confounders_other, ';')]
# number of covariate = number of ";" + 1 for non-missing cell
age_rr_dt[confounders_other!='', confounders_other_num := confounders_other_num + 1]

age_rr_dt[, adjustment := apply(.SD, 1, sum), .SDcols= confounders]
age_rr_dt[, adjustment := adjustment + confounders_other_num]

# mark ind for fully adjusted model
age_rr_dt[, adj_max := max(adjustment), by=c("nid", "outcome", "location_name", "age_end", "age_start", "age_mean", "percent_male")]
age_rr_dt[nid==462795,]
age_rr_dt[ ,fully_adj_ind := ifelse(adjustment==adj_max, 1, 0)]

# add cascading dummies for number of adjusted covariates. 
age_rr_dt[ , cv_adj := as.numeric()]
age_rr_dt[,adj_age_sex := 0]
age_rr_dt[confounders_age==1 & confounders_sex==1, adj_age_sex := 1]

# cv_adj=3 if age or sex is not adjusted
age_rr_dt[adj_age_sex==0, cv_adj := 3]

# cv_adj=2 if only age and sex are adjusted
age_rr_dt[adj_age_sex==1 & adjustment==2, cv_adj := 2]

# cv_adj=1 if age+sex + 3 more covariates are adjusted
age_rr_dt[adj_age_sex==1 & adjustment>2 & adjustment<=5, cv_adj := 1]

# cv_adj=0 if age+sex + more than 3 covariates are adjusted
age_rr_dt[adj_age_sex==1 & adjustment>5, cv_adj := 0]

# check whether there is missing in cv_adj
message("there is ", nrow(age_rr_dt[is.na(cv_adj)]), " missing values in cv_adj")

# add cascading dummies
age_rr_dt[, cv_adj_L0 := 1]
age_rr_dt[, cv_adj_L1 := 1]
age_rr_dt[, cv_adj_L2 := 1]

# if cv_adj==0, change all dummies to be 0
age_rr_dt[cv_adj==0, c("cv_adj_L0", "cv_adj_L1", "cv_adj_L2") := 0]
# if cv_adj==1, change cv_adj_L1 and cv_adj_L2 to be 0
age_rr_dt[cv_adj==1, c("cv_adj_L1", "cv_adj_L2") := 0]
# if cv_adj==2, change cv_adj_L2 to be 0
age_rr_dt[cv_adj==2, c("cv_adj_L2") := 0]

# remove cv_adj
age_rr_dt[, cv_adj := NULL]

# make sure the exposured/unexposed def are consistent----
age_rr_dt[, custom_exposed_def] %>% unique

# add indicators of current and former smoker as exposed group----
age_rr_dt[grepl('former|ever', custom_exposed_def, ignore.case = T) & !grepl('never|non-smoker', custom_exposed_def, ignore.case = T), custom_exposed_def] %>% unique
age_rr_dt[, cv_exp_former := ifelse(grepl('former|ever', custom_exposed_def, ignore.case = T) & !grepl('never|non-smoker', custom_exposed_def, ignore.case = T) , 1, 0)]

age_rr_dt[grepl('current|ever', custom_exposed_def, ignore.case = T) & !grepl('never|non-smoker', custom_exposed_def, ignore.case = T), custom_exposed_def] %>% unique
age_rr_dt[, cv_exp_current := ifelse(grepl('current|ever', custom_exposed_def, ignore.case = T) & !grepl('never|non-smoker', custom_exposed_def, ignore.case = T) , 1, 0)]

# examine the indicators
age_rr_dt[cv_exp_former==1 & cv_exp_current==1, custom_exposed_def] %>% unique
age_rr_dt[cv_exp_former==1, custom_exposed_def] %>% unique
age_rr_dt[cv_exp_current==1, custom_exposed_def] %>% unique

# fix the indicator for some surveys manually
age_rr_dt[grepl('Former smokers that are not currently smoking', custom_exposed_def, ignore.case = T), cv_exp_current :=0]
age_rr_dt[grepl('A subject was defined as a smoker if he had ever smoked on a regular basis', custom_exposed_def, ignore.case = T), cv_exp_former := 0]

# current and former should not be both 0, fix those surveys
age_rr_dt[cv_exp_current==0 & cv_exp_former==0, custom_exposed_def] %>% unique
age_rr_dt[grepl('Smoking one or more cigarettes daily, bidis and cigar for preceeding 3 months', custom_exposed_def, ignore.case = T), cv_exp_current := 1]
age_rr_dt[grepl('Smokers that smoked one or more cigarette per day either prior to admission or at the time of the Regional Health Survey', custom_exposed_def, ignore.case = T), cv_exp_current := 1]
age_rr_dt[grepl('Smoking at least one cigarette a day in the year before the event', custom_exposed_def, ignore.case = T), cv_exp_current := 1]
age_rr_dt[custom_exposed_def=="Smokers", c("cv_exp_current", "cv_exp_former") := 1 ]
age_rr_dt[custom_exposed_def=="History of smoking", c("cv_exp_current", "cv_exp_former") := 1 ]

# flip exp def of non-smoker for some surveys----
age_rr_dt[grepl('never|non-smoker', custom_exposed_def, ignore.case = T), .(custom_exposed_def, custom_unexp_def)] %>% unique
age_rr_dt[, cv_exp_nonsmoker := ifelse(grepl('never|non-smoker', custom_exposed_def, ignore.case = T), 1, 0)]

age_rr_dt[cv_exp_nonsmoker==1, effect_size := 1/effect_size]
age_rr_dt[cv_exp_nonsmoker==1, lower_flip := 1/upper]
age_rr_dt[cv_exp_nonsmoker==1, upper_flip := 1/lower]

age_rr_dt[cv_exp_nonsmoker==1, lower := lower_flip]
age_rr_dt[cv_exp_nonsmoker==1, upper := upper_flip]
age_rr_dt[cv_exp_nonsmoker==1]

# recalculate log effects for the flipped RR
age_rr_dt[cv_exp_nonsmoker==1, log_effect_size := log(effect_size)]
age_rr_dt[cv_exp_nonsmoker==1, log_se := (log(upper)-log(lower))/3.92] 

# for the non-smoker exposure, fix the exposure indicator
age_rr_dt[cv_exp_nonsmoker==1, cv_exp_current := 1 ]

# check the unexposed def, cv_unexp_never=1 for never smoker =0 for both never and former smokers.
age_rr_dt[, custom_unexp_def] %>% unique
age_rr_dt[, cv_unexp_never := ifelse(grepl('never', custom_unexp_def, ignore.case = T) & !grepl('non|former|ex|current|quit|daily', custom_unexp_def, ignore.case = T), 1, 0)]
# age_rr_dt[, cv_unexp_non := ifelse(grepl('non', custom_unexp_def, ignore.case = T), 1, 0)]

age_rr_dt[cv_unexp_never==0, custom_unexp_def] %>% unique
age_rr_dt[cv_unexp_never==1, custom_unexp_def] %>% unique

# manually fixed some unexposed def indicator
age_rr_dt[custom_exposed_def=="Never smokers", cv_unexp_never := 1]

# add the mean follow up time to mean/median age for prospective studies only but not for other designs------
age_rr_dt[, duration_fup_units] %>% unique # only has year unit
age_rr_dt[cv_design_pc==1, duration_fup_measure] %>% unique
age_rr_dt[cv_design_pc==1 & is.na(value_of_duration_fup)]

# for nid 359223 follow up from 2001 to 2004, assume mean follow up to be 2 years. 
age_rr_dt[nid==359223, value_of_duration_fup := 2]
age_rr_dt[nid==359223, duration_fup_measure := "mean"]
age_rr_dt[nid==359223, duration_fup_units := "years"]

# make copy of some columns
age_rr_dt[, value_of_duration_fup_orig := value_of_duration_fup]
age_rr_dt[, age_start_orig := age_start]
age_rr_dt[, age_end_orig := age_end]
age_rr_dt[, age_mean_orig := age_mean]

# for duration_fup_measure== "max", half the years of follow up
age_rr_dt[duration_fup_measure == "max", value_of_duration_fup := value_of_duration_fup_orig/2]

# add the fu time to the mean age, age_start and age_end for pc design
age_rr_dt[grepl("Prospective", design, ignore.case = T), age_mean := age_mean_orig + value_of_duration_fup]
age_rr_dt[grepl("Prospective", design, ignore.case = T), age_start := age_start_orig + value_of_duration_fup]
age_rr_dt[grepl("Prospective", design, ignore.case = T), age_end := age_end_orig + value_of_duration_fup]


# In case we want to use age_start and age_end to model, we estimate the missing age_start and age_end using age_mean and age_sd with 95% CI
# calculate the age_start and age_end before prediting age_mean using age_midpoint
age_rr_dt[is.na(age_start_orig) & is.na(age_end_orig) & is.na(age_sd)]
age_rr_dt[, age_start_pred := age_mean-1.96*age_sd]
age_rr_dt[, age_end_pred := age_mean+1.96*age_sd]

# cross-walk age_midpoint to age_mean
age_rr_dt[, age_midpoint := (age_end - age_start)/2 ]

plot(age_rr_dt[, age_mean], age_rr_dt[, age_midpoint], ylim = c(0,100), xlim = c(0,100))
mean_midpoint_model <- lmer(age_mean ~ age_midpoint + (1|nid), data = age_rr_dt)
summary(mean_midpoint_model)

# predict age_mean using age_midpoint and examine the model performance
age_rr_dt[, age_mean_pred := predict(mean_midpoint_model, age_rr_dt, allow.new.levels = T)]
# age_rr_dt[, age_mean_pred := predict(mean_midpoint_model, age_rr_dt, re.form=~0)]
plot(age_rr_dt[, age_mean], age_rr_dt[, age_mean_pred], ylim = c(0,100), xlim = c(0,100))
abline(coef = c(0,1))

age_rr_dt[is.na(age_mean), age_mean := age_mean_pred]

# for estimates with SE, use delta method to transform the se of OR to se of log OR.
row_nums <- age_rr_dt[(is.na(upper) | is.na(lower)) & !is.na(nonCI_uncertainty_value), row_num]

for(rnum in row_nums){
  age_rr_dt[row_num==rnum, log_se := deltamethod(g=~log(x1), mean = effect_size, cov = nonCI_uncertainty_value^2)]
}

# for OR estimates with a b c d, calcuate the se of log(OR)
age_rr_dt[(is.na(upper) | is.na(lower)) & !is.na(cohort_number_events_exp) & !is.na(cohort_number_events_unexp) & 
            !is.na(cohort_sample_size_exp) & !is.na(cohort_sample_size_unexp) & cv_measurement_or==1, 
          log_se := sqrt(1/cohort_number_events_exp + 1/cohort_number_events_unexp + 
                           1/(cohort_sample_size_exp-cohort_number_events_exp) + 1/(cohort_sample_size_unexp-cohort_number_events_unexp))]

# for RR estimates with a b c d, calcuate the se of log(RR)
age_rr_dt[(is.na(upper) | is.na(lower)) & !is.na(cohort_number_events_exp) & !is.na(cohort_number_events_unexp) & 
            !is.na(cohort_sample_size_exp) & !is.na(cohort_sample_size_unexp) & cv_measurement_rr==1, 
          log_se := sqrt((cohort_sample_size_exp-cohort_number_events_exp)/(cohort_sample_size_exp*cohort_number_events_exp) + 
                           (cohort_sample_size_unexp-cohort_number_events_unexp)/(cohort_sample_size_unexp*cohort_number_events_unexp))]

# use age_start_pred and age_end_pred if age_start or age_end is missing
age_rr_dt[is.na(age_start), age_start := age_start_pred]
age_rr_dt[is.na(age_end), age_end := age_end_pred]

# save the fully cleaned dataset
write.csv(age_rr_dt, file.path(age_rr_dt_dir, 'all_extractions_cleaned.csv'), row.names = F)








