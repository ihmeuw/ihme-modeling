# cleaning the tobacco data for mr-brt es

rm(list=ls())

library(tidyverse)
library(data.table)
source('FILEPATH/prep_data_function.R')
source("FILEPATH/get_age_metadata.R")
args <- commandArgs(trailingOnly = TRUE)

## NEED TO CHANGE THE RO_PAIR HERE

if(interactive()){
  ro_pair <- "change to the ro_pair of interest"
  out_dir <- "FILEPATH"
  WORK_DIR <- 'FILEPATH'
  setwd(WORK_DIR)
  source("FILEPATH/config.R")
} else {
  ro_pair <- args[1]
  out_dir <- args[2]
  WORK_DIR <- args[3]
  setwd(WORK_DIR)
  source("./config.R")
}

cause <- ro_pair

# read in data
input_dir <- "/FILEPATH"
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


dt_sub <- dt_orig[include_in_model==1, var_names, with=F]

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

set(dt_sub, which(is.na(dt_sub[["age_start"]])),"age_start", median(dt_sub[["age_start"]], na.rm = T))
set(dt_sub, which(is.na(dt_sub[["age_end"]])),"age_end", median(dt_sub[["age_end"]], na.rm = T))
dt_sub[is.na(percent_male), percent_male := 0.5]

# check the missingness of the data again
summary(dt_sub)

# adding cv_older
dt_sub[ ,cv_older := 0]
dt_sub[age_start>50, cv_older := 1]

# adding adjustment level covariate
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

if(cause=="nasopharyngeal_cancer"){
  print(paste0('nids of studies where b_0 > b_1: ',unique(dt_sub[b_0>b_1, nid])))
  print(paste0('nids of studies where b_0 = b_1 = 0: ',unique(dt_sub[b_0==0 & b_1==0, nid])))
  dt_sub <- dt_sub[b_0 < b_1]
}

age_dt <- get_age_metadata(19,gbd_round_id = 7)

#if(ro_pair %in% c("ihd", "stroke", "afib_and_flutter", "peripheral_artery_disease", "aortic_aneurism")){
if(ro_pair %in% c("none")){
  # for CVD outcomes, adjust the data here
  dt_sub[is.na(age_mean), age_mean := (age_start+age_end)/2]
  
  # calculate the missing mean age: mean_age = mean age at enrollment + mean/median fup time
  dt_sub[duration_fup_measure %in% c("mean", "median") & duration_fup_units=="years", mean_age := age_mean + value_of_duration_fup]
  
  # if unit is max, mean fup duration= max fup duration/2
  dt_sub[duration_fup_measure=="max", value_of_duration_fup := value_of_duration_fup/2]
  dt_sub[is.na(mean_age) & duration_fup_units=="years", mean_age := age_mean + value_of_duration_fup]
  
  # if mean_age is still missing, using the mean of duration of fup from the data
  mean_of_age <- dt_sub[duration_fup_units=="years", value_of_duration_fup] %>% mean(., na.rm = T)
  dt_sub[is.na(mean_age), mean_age := (age_start+age_end)/2 + mean_of_age]
  
  # age adjusted function
  # problems: if the orginal rr is protective (log_rr <0), the adjustment is very bad...
  cvd_age <- function(data, ages=seq(25+2,95+2,5), terminal.age = 110, extrapolate = T){
    #' @description For CVD causes IHD and stroke, create data for causes that is more granular in age, using
    #' Steve's approach as explained by Joey. Assumes log-linear relation between age and RR. For imputing uncertainty, couple options.
    #' Currently assuming effect of age on RR is independent of effect of cig/day on RR.
    #' @terminal.age The age at which the log(RR) should reach 0. Currently finding this age from analysis of CPS-II data used in
    #' GBD 2016 last year.
    #' @ages The ages at which the RR's should be calculated
    #' @extrapolate Whether when calculating age-specific RR's to go beyond a given data points age range,or just split within data points range
    #' @return Data table with new column for calculated age and new RR's.
    #' For imputing the standard error of each age group, I would expect it would be somewhat quadratic. The higher standard
    #' errors will be at the youngest and oldest age groups due to small sample size, rather than linear.
    
    data_all <- NULL
    
    for(a in ages){
      if (a == 25+2){
        d <- copy(data[,this.age := a])
        data_all <- copy(d)
      } else{
        data_temp <- copy(d)
        data_temp[,this.age := a]
        data_all <- rbind(data_all,data_temp)
      }
      
    }
    
    setnames(data_all, "ln_effect", "old.ln_effect")
    setnames(data_all, "ln_se", "old.ln_se")
    data_all[, ln_effect := ((old.ln_effect - 0)/(mean_age - terminal.age)) * (as.numeric(this.age) - terminal.age)]
    data_all[this.age >= terminal.age, ln_effect := 0]
    data_all[, ln_se := ((old.ln_se - 0)/(mean_age - terminal.age)) * (as.numeric(this.age) - terminal.age)]
    
    data_all[ln_se<0, ln_se:=0.0001]
    data_all[, `:=`(lower = exp(ln_effect - 1.96*ln_se),
                    upper = exp(ln_effect + 1.96*ln_se),
                    rr = exp(ln_effect),
                    old_rr = exp(old.ln_effect))]
    return(data_all)
  }
  
  # adjust data to the same age groups
  dt_sub_cvd <- cvd_age(dt_sub)
  dt_sub_cvd <- dt_sub_cvd[ln_effect>0] 
  dt_sub_cvd[, `:=`(age_group_years_start=this.age-2,
                    age_group_years_end=this.age+3)]
  dt_sub_cvd <- merge(dt_sub_cvd, age_dt[,.(age_group_id, age_group_name, age_group_years_start, age_group_years_end)], 
                      by=c("age_group_years_start", "age_group_years_end"), all.x = T)
  dt_sub_cvd[this.age==97, age_group_id := 235]
  dt_sub_cvd[this.age==97, age_group_name:= "95 plus"]
  
  # save the data
  write.csv(dt_sub, file.path(out_dir, paste0(cause, '.csv')), row.names = F)
  
  # save the age-specific dataset
  for(age in unique(dt_sub_cvd[,age_group_id])){
    d <- dt_sub_cvd[age_group_id==age]
    write.csv(d, file.path(out_dir, paste0(cause, "_",age, '.csv')), row.names = F)
    
    
    # save the .RDS file
    obs_var <- "ln_effect"; obs_se_var <- "ln_se"
    ref_vars <- c("a_0", "a_1"); alt_vars <- c("b_0", "b_1")
    allow_ref_gt_alt = FALSE
    drop_x_covs <- drop_z_covs <- keep_x_covs <- keep_z_covs <- NA
    diet_dir <- 'FILEPATH'
    study_id_var <- "nid"
    verbose <- TRUE
    
    # change the ro_pair by adding the age group
    ro_pair_age_spec <- paste0(ro_pair,"_",age)
    out <- prep_diet_data(ro_pair=ro_pair_age_spec, obs_var, obs_se_var, ref_vars, alt_vars, allow_ref_gt_alt = FALSE,
                          study_id_var = study_id_var,
                          drop_x_covs = NA, keep_x_covs = NA, drop_z_covs = NA, keep_z_covs = NA,
                          diet_dir = diet_dir,
                          verbose = TRUE)
    message(paste0('saving cleaned data..'))
    saveRDS(out, paste0(out_dir, "00_prepped_data/", ro_pair, "_", age, ".RDS"))
    message(paste0('saved cleaned data to: ', paste0(out_dir, "00_prepped_data/", ro_pair, "_", age, ".RDS")))
    
  }
  
  
} else {
  # save the data
  write.csv(dt_sub, file.path(out_dir, paste0(cause, '.csv')), row.names = F)
  
  # save the .RDS file
  obs_var <- "ln_effect"; obs_se_var <- "ln_se"
  ref_vars <- c("a_0", "a_1"); alt_vars <- c("b_0", "b_1")
  allow_ref_gt_alt = FALSE
  drop_x_covs <- drop_z_covs <- keep_x_covs <- keep_z_covs <- NA
  diet_dir <- 'FILEPATH'
  study_id_var <- "nid"
  verbose <- TRUE
  
  out <- prep_diet_data(ro_pair=ro_pair, obs_var, obs_se_var, ref_vars, alt_vars, allow_ref_gt_alt = FALSE,
                        study_id_var = study_id_var,
                        drop_x_covs = NA, keep_x_covs = NA, drop_z_covs = NA, keep_z_covs = NA,
                        diet_dir = diet_dir,
                        verbose = TRUE)
  print(out)
  message(paste0('saving cleaned data..'))
  saveRDS(out, paste0(out_dir, "00_prepped_data/", ro_pair, ".RDS"))
  message(paste0('saved cleaned data to: ', paste0(out_dir, "00_prepped_data/", ro_pair, ".RDS")))
}

