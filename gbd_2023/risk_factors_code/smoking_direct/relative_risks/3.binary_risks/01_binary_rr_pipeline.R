#------------------------------------------------------------------
# Purpose: Estimate binary RR of smoking-fracture and create draws
# Date: 12/03/2020
#-----------------------------------------------------------------

rm(list=ls())

library(tidyverse)
library(data.table)
library(dplyr)
library(msm)
library(mrbrt001, lib.loc = FILEPATH)
source(FILEPATH)
args <- commandArgs(trailingOnly = TRUE)

## NEED TO CHANGE THE RO_PAIR HERE

if(interactive()){
  # NOTE: the ro_pair for this script does not include age-specific info
  ro_pair <- "fractures"
  cov_setting <- "cov_finder_no_sex" # option: ['cov_finder', 'cov_finder_no_sex', 'no_cov','percent_male_only','self_selected'(no percent_male)]
  trm <- 0.9
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

dt_orig <- fread(FILEPATH)

confounders <- names(dt_orig)[grepl('confounders_', names(dt_orig))]
cvs <- names(dt_orig)[grepl('cv_', names(dt_orig))]

dt_orig[, effect_size_log := log(effect_size)]

dt_orig$se_effect_log <- sapply(1:nrow(dt_orig), function(i) {
  effect_size_i <- dt_orig[i, "effect_size"]
  se_effect_i <- dt_orig[i, "se_effect"]
  deltamethod(~log(x1), effect_size_i, se_effect_i^2)
})



var_names <- c('include_in_model', 'nid', 'effect_size_log', 'se_effect_log', 
                 "age_start", "age_end", "adjustment", "percent_male", confounders, cvs)


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
dt_sub <- setnames(dt_sub, old = c("effect_size_log", "se_effect_log"), 
                   new = c("ln_effect", "ln_se"))

# create ref and alt
dt_sub[, ref := 0]; dt_sub[, alt := 1]


# save the data
write.csv(dt_sub, file.path(FILEPATH, paste0(cause, '.csv')), row.names = F)

# 1. use cov finder to select significant covariates --------------------------------------------------------------------------------------------------------------------------------------------------------------

# covariate selection
if(cov_setting=="cov_finder_no_sex"){
  candidate_covs <-c(names(dt_sub)[grepl('cv_', names(dt_sub))])
} else {
  candidate_covs <-c(names(dt_sub)[grepl('cv_', names(dt_sub))],"percent_male")
}

candidate_covs <- candidate_covs[!grepl('confounding', candidate_covs)]

candidate_covs <- candidate_covs[grepl('cv_adj|male|study', candidate_covs)]

data <- MRData()
data$load_df(
  data=dt_sub,
  col_obs='ln_effect', #log space since bounded by 0
  col_obs_se='ln_se', 
  col_covs=as.list(candidate_covs),
  col_study_id='nid' 
)


if(cov_setting=="no_cov"){
  new_covs <- c()
  
  pred_data <- as.data.table(expand.grid('intercept'=c(1)))
  
} else if(cov_setting %in% c("cov_finder", 'cov_finder_no_sex')) {
  covfinder <- CovFinder(
    data = data,
    covs = as.list(candidate_covs),
    pre_selected_covs = list('intercept'),
    normalized_covs = FALSE,
    num_samples = 1000L,
    power_range = list(-4, 4),
    power_step_size = 0.05,
    inlier_pct = trm,
    laplace_threshold = 1e-5
  )
  
  covfinder$select_covs(verbose = FALSE)
  
  new_covs <- covfinder$selected_covs
  new_covs <- new_covs[!grepl('intercept', new_covs)]
  print(new_covs)
  
  if(grepl('percent_male', new_covs)){
    pred_data_m <- as.data.table(expand.grid("intercept"=c(1), "percent_male"=1, 'cv_adj_L1'=0 ,'cv_adj_L0'=0))
    pred_data_f <- as.data.table(expand.grid("intercept"=c(1), "percent_male"=0, 'cv_adj_L1'=0 ,'cv_adj_L0'=0))
  } else {
    pred_data <- as.data.table(expand.grid("intercept"=c(1), 'cv_adj_L1'=0))
  }
  
  
} else if(cov_setting=="percent_male_only"){
  new_covs <- c('percent_male')
  
  pred_data_m <- as.data.table(expand.grid("intercept"=c(1), "percent_male"=1))
  pred_data_f <- as.data.table(expand.grid("intercept"=c(1), "percent_male"=0))
  
} else {
  new_covs <- c('cv_adj_L1')
  
  pred_data <- as.data.table(expand.grid("intercept"=c(1), "cv_adj_L1"=0)) 
}

# 2. Prep data with covariates selected and run standard mixed effects model ------------------------------------------------------------------------------------------------------------------------------

if(cov_setting=="no_cov"){
  
  data1 <- MRData()
  data1$load_df(
    data=dt_sub,
    col_obs='ln_effect', #log space since bounded by 0
    col_obs_se='ln_se', 
    col_study_id='nid'
    )
  
} else {
  
  data1 <- MRData()
  data1$load_df(
    data=dt_sub,
    col_obs='ln_effect', #log space since bounded by 0
    col_obs_se='ln_se', 
    col_covs=as.list(new_covs),
    col_study_id='nid'
    )
  
} 


#Using use_re with covariates: 
# can the variation between different studies be explained with some covariates? 
# if bias covariates have their own random effect, then the covariates will try to explain the same thing that the intercept is
# if you do not have much data to work with, it is recommended to set use_re=F 
# for intercept, set use_re=T

cov_models <- list(
  LinearCovModel("intercept", use_re = T)
)

for (cov in new_covs) cov_models <- append(cov_models,
                                           list(do.call(
                                             LinearCovModel,
                                             c(list(alt_cov=cov, use_re = F)
                                             ))))

model <- MRBRT(
  data=data1, 
  cov_models=cov_models,
  inlier_pct=trm
)

model$fit_model(inner_print_level = 5L, inner_max_iter = 1000L)

# 3. Make predictions ----------------------------------------------------------------------------------------------------------------------------------------------


if(cov_setting=="no_cov"){
  data_pred1 <- MRData()
  data_pred1$load_df(
    data = pred_data,
    col_covs = as.list("intercept")
    )
} else if(cov_setting %in% c("cov_finder", "percent_male_only")) {
  data_pred_m <- MRData()
  data_pred_f <- MRData()
  
  # predict male
  data_pred_m$load_df(
    data = pred_data_m,
    col_covs = as.list(new_covs)
  )
  
  # predict female
  data_pred_f$load_df(
    data = pred_data_f,
    col_covs = as.list(new_covs)
  )
} else {
  data_pred1 <- MRData()
  
  # predict 
  data_pred1$load_df(
    data = pred_data,
    col_covs = as.list(new_covs)
  )
}


#In GBD 2020 we are including uncertainty from gamma (between study heterogeneity)
n_samples <- 1000L
samples <- model$sample_soln(sample_size=n_samples)

if(cov_setting %in% c("no_cov", "self_selected", "cov_finder_no_sex")){
  draws <- model$create_draws(
    data = data_pred1, 
    beta_samples = samples[[1]],
    gamma_samples = samples[[2]],
    random_study=TRUE)
  
  pred_data$Y_mean <- model$predict(
    data = data_pred1, 
    predict_for_study = T, 
    sort_by_data_id = T) 
  
  pred_data$Y_mean_lo_re <- apply((draws), 1, function(x) quantile(x, 0.025)) 
  pred_data$Y_mean_hi_re <- apply((draws), 1, function(x) quantile(x, 0.975)) 
  
  #summary
  exp(pred_data$Y_mean) %>% print
  exp(pred_data$Y_mean_lo_re) %>% print
  exp(pred_data$Y_mean_hi_re) %>% print
  
} else if(cov_setting %in% c("cov_finder", "percent_male_only")){
  # create draws for male
  draws_m <- model$create_draws(
    data = data_pred_m, 
    beta_samples = samples[[1]],
    gamma_samples = samples[[2]],
    random_study=TRUE)
  
  pred_data_m$Y_mean <- model$predict(
    data = data_pred_m, 
    predict_for_study = T, 
    sort_by_data_id = T) 
  
  pred_data_m$Y_mean_lo_re <- apply((draws_m), 1, function(x) quantile(x, 0.025)) 
  pred_data_m$Y_mean_hi_re <- apply((draws_m), 1, function(x) quantile(x, 0.975))
  
  draws_fe_m <- model$create_draws(
    data = data_pred_m, 
    beta_samples = samples[[1]],
    gamma_samples = samples[[2]],
    random_study=FALSE)
  
  pred_data_m$Y_mean_lo_fe <- apply((draws_fe_m), 1, function(x) quantile(x, 0.025)) 
  pred_data_m$Y_mean_hi_fe <- apply((draws_fe_m), 1, function(x) quantile(x, 0.975)) 
  
  #summary
  print(exp(pred_data_m$Y_mean))
  print(exp(pred_data_m$Y_mean_lo_re))
  print(exp(pred_data_m$Y_mean_hi_re))
  
  # create draws for female
  draws_f <- model$create_draws(
    data = data_pred_f, 
    beta_samples = samples[[1]],
    gamma_samples = samples[[2]],
    random_study=TRUE)
  
  pred_data_f$Y_mean <- model$predict(
    data = data_pred_f, 
    predict_for_study = T, 
    sort_by_data_id = T) 
  
  pred_data_f$Y_mean_lo_re <- apply((draws_f), 1, function(x) quantile(x, 0.025)) 
  pred_data_f$Y_mean_hi_re <- apply((draws_f), 1, function(x) quantile(x, 0.975))
  
  draws_fe_f <- model$create_draws(
    data = data_pred_f, 
    beta_samples = samples[[1]],
    gamma_samples = samples[[2]],
    random_study=FALSE)
  
  pred_data_f$Y_mean_lo_fe <- apply((draws_fe_f), 1, function(x) quantile(x, 0.025)) 
  pred_data_f$Y_mean_hi_fe <- apply((draws_fe_f), 1, function(x) quantile(x, 0.975)) 

  
  #summary
  print(exp(pred_data_f$Y_mean))
  print(exp(pred_data_f$Y_mean_lo_re))
  print(exp(pred_data_f$Y_mean_hi_re))
  
}

# 4. Format and save  observation data for plotting ----------------------------------------------------------------------------------------------------------------------------------------
if (cov_setting %in% c('cov_finder', 'percent_male_only')){
  obs_data <- cbind("val" = data1$obs , "se"= data1$obs_se, "study" = data1$study_id, "included" = model$w_soln, 'sample_sex' = data1$covs$percent_male) %>% data.table
} else {
  obs_data <- cbind("val" = data1$obs , "se"= data1$obs_se, "study" = data1$study_id, "included" = model$w_soln) %>% data.table
}

obs_data[, lower:=val-1.96*se]
obs_data[, upper:=val+1.96*se]
obs_data <- obs_data[order(val)]
obs_data[, data:= 1]
obs_data[included > 0 & included < 1, included:=0.5]

if(cov_setting %in% c('cov_finder', 'percent_male_only')){
  # add results for male
  results_m <- data.table("val" = exp(pred_data_m$Y_mean), "study" = c("Result w/ gamma"), 
                          "lower" = exp(pred_data_m$Y_mean_lo_re), 
                          "upper" = exp(pred_data_m$Y_mean_hi_re))
  results_m[,data:= 2]
  results_m[, included := 1]
  results_m[, sample_sex := 1]
  
  # add results for female
  results_f <- data.table("val" = exp(pred_data_f$Y_mean), "study" = c("Result w/ gamma"), 
                          "lower" = exp(pred_data_f$Y_mean_lo_re), 
                          "upper" = exp(pred_data_f$Y_mean_hi_re))
  results_f[,data:= 2]
  results_f[, included := 1]
  results_f[, sample_sex := 0]
  
  obs_data <- rbindlist(list(results_m, results_f, obs_data), fill = T)
  obs_data[, row := 1:nrow(obs_data)]
  
} else {
  # add results for male
  results <- data.table("val" = exp(pred_data$Y_mean), "study" = c("Result w/ gamma"), 
                          "lower" = exp(pred_data$Y_mean_lo_re), 
                          "upper" = exp(pred_data$Y_mean_hi_re))
  results[,data:= 2]
  results[, included := 1]
  
  obs_data <- rbindlist(list(results, obs_data), fill = T)
  obs_data[, row := 1:nrow(obs_data)]
  
}

# save model objects and obs_data dt
write.csv(obs_data, paste0(FILEPATH, cov_setting, '_', trm, '.csv'), row.names=F)
py_save_object(object = model, filename = paste0(FILEPATH, cause, '_', cov_setting, '_', trm, '.pkl'), pickle = "dill")


# 5. Save Draws --------------------------------------------------------------------------------------------------------------------------------------------------

# for GBD 2020, use draws that incorporate between-study heterogeneity (i.e. with gamma)

if(cov_setting %in% c("no_cov", "self_selected", "cov_finder_no_sex")){
  save_draws <- copy(draws) %>% data.table
  
  write.csv(save_draws, paste0(FILEPATH,cause, '_', cov_setting, '_', trm,'_','draws_raw.csv'), row.names = F)
} else {
  save_draws_m <- copy(draws_m) %>% data.table
  save_draws_f <- copy(draws_f) %>% data.table
  
  save_draws_m[, sex_id := 1]
  save_draws_f[, sex_id := 2]
  
  save_draws <- rbindlist(list(save_draws_m, save_draws_f), fill = T)
  
  write.csv(save_draws, paste0(FILEPATH,cause, '_', cov_setting, '_', trm,'_', 'draws_raw.csv'), row.names = F)
}




