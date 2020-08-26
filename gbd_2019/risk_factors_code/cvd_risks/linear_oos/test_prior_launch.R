################################
## Purpose: launch ensemble prior process

if(!exists("rm_ctrl"))
  {
    rm(list=objects())
  }

j <- "FILEPATH"
h <- "FILEPATH"

date<-gsub("-", "_", Sys.Date())

library(plyr)
library(dplyr)
suppressMessages(library(data.table))
library(ini)

################### SCRIPTS #########################################
source("FILEPATH/test_prior.R")
source("FILEPATH/assemble_prior.R")
source("FILEPATH/get_recent.R")
source("FILEPATH/bind_covariates.R")
source("FILEPATH/get_user_input.R")

################### ARGS #########################################
if(!exists("me"))
  {
    me<-get_me_from_user()
    decomp_step<-get_step_from_user()
  }

step_num <- gsub('step', '', decomp_step)

test_mods<-T
average<-T
save_ensemble<-T
n_mods<-50
plot_aves<-F
age_trend<-F

output<-paste0("FILEPATH/", me, "/models/", me, "_prior_test_priorsignT_subnatsT_", date, ".csv")
data_output<-paste0("FILEPATH/", me, "/data/", me,"_best_data.csv")
ensemble_output<-paste0("FILEPATH/", me, "_custom_stage1/", me, "_custom_stage1_decomp", step_num, "_", date, ".csv")

plot_mods_path<-paste0("FILEPATH/", me, "/")

################### ARGUMENTS: EXAMPLE 1 #########################################
## This is a simple example. Many of the arguments I specify are the defaults, but I'm being more explicit here
if(me=="ldl"){
  xwalk_version_id <- get_id_from_user(me, 'crosswalk_version_id')
  cov_list<-c("Mean BMI", "Prevalence of obesity",
              'Age- and sex-specific SEV for Low fruit', 'Age- and sex-specific SEV for Low nuts and seeds', 'Age- and sex-specific SEV for Low vegetables',
              "Healthcare access and quality index", "Socio-demographic Index")
  prior_sign<-c(1, 1,
                1, 1, 1,
                -1, 0)
  
  custom_covs<-NULL
  ban_pairs<-list(c("mean_BMI", "prev_obesity"))
  polynoms<-NULL
  modtype<-"lmer"
  count_mods<-T ## only use this after running with count_mods=T first!
  rank_method="oos.rmse"
  forms_per_job<-10
  drop_nids<-F
  remove_subnats<-F
  
}

################### ARGUMENTS: EXAMPLE 2 #########################################
##A more complicated example with some of the optional arguments specified.. prior_sign, polynomials, ban_pairs, etc
if(me=="sbp"){
  xwalk_version_id <- get_id_from_user(me, 'crosswalk_version_id')
  cov_list<-c("Socio-demographic Index", 'Liters of alcohol consumed per capita',
              'Age- and sex-specific SEV for Low vegetables',
              "Mean BMI", "Prevalence of obesity", "Healthcare access and quality index",
              'Age- and sex-specific SEV for Low omega-3', "Age- and sex-specific SEV for Smoking", 'Age- and sex-specific SEV for Low fruit',
              'Age- and sex-specific SEV for High sodium')
  prior_sign<-c(0, 0,
                1,
                1, 1, -1,
                1, 1, 1,
                1) 
  
  
  ##If pulling in custom covariates::need covariate name short(as stored in custom covariate file) and filepath to custom cov
  custom_covs <- vector()
  ban_pairs<-list(c("mean_BMI", "prev_obesity_agestd"))
  polynoms<-NULL
  modtype<-"lmer"
  rank_method="oos.rmse"
  forms_per_job<-30
  drop_nids<-F
  remove_subnats<-F
  
}

if(me=="cod"){
  cov_list<-c("Socio-demographic Index",
              "Healthcare access and quality index", "LDI (I$ per capita)")
  custom_covs<-NULL
  ban_pairs<-NULL
  prior_sign<-NULL
  polynoms<-NULL
  modtype<-"lmer"
  rank_method="oos.rmse"
  forms_per_job<-4
  drop_nids<-F
  remove_subnats<-F
}

################### ARGUMENTS: BOTH RISKS #########################################
fixed_covs <- NULL
random_effects<-c("(1|super_region_name)", "(1|region_name)")  ##Need to be in random effects formula format

## specify other model arguments
intrxn<-NULL  
by_sex<-T
data_transform<-"log"

## ko args
ko_prop<-0.15
proj<-"PROJECT"
slots_per_job<-3
kos<-5

count_mods<-F
username<-"USERNAME"
location_set_id <- 35
age_group_set_id <- 12

################### LAUNCH MODEL TESTS #########################################
if(test_mods==T){
  message("Testing submodels")
  rmses_and_data<-test_prior(crosswalk_version_id=xwalk_version_id, decomp_step="step2", cov_list=cov_list, data_transform=data_transform,
                             count_mods=count_mods, rank_method=rank_method, modtype=modtype,
                             custom_covs=custom_covs, fixed_covs=fixed_covs, random_effects=random_effects,
                             ban_pairs=ban_pairs, prior_sign = prior_sign,
                             by_sex=by_sex,
                             polynoms=polynoms,
                             ko_prop=ko_prop, kos=kos, drop_nids=drop_nids, remove_subnats=T,
                             proj=proj, slots_per_job=slots_per_job, username=username, forms_per_job=forms_per_job
  )
  
  rmses<-rmses_and_data[[1]]
  data<-rmses_and_data[[2]]
  
  write.csv(data, file=data_output, row.names=F)
  write.csv(rmses, file=output, row.names=F)
}

################### CREATE ENSEMBLE PREDICTION #########################################

if(average==T){
  message("Averaging submodels and predicting")
  rmses<-get_recent(output<-paste0("FILEPATH", me, "/models/"))
  data_path<-paste0("FILEPATH", me, "/data/", me,"_best_data.csv")
  data<-fread(data_path)
  
  ## This is for creating an ensemble model of linear priors. 
  ave_models<-assemble_prior(data, rmses[drop==0,], cov_list, data_transform, decomp_step=decomp_step,
                             custom_cov_list=custom_covs, polynoms=polynoms, n_mods=n_mods,
                             plot_mods=plot_aves, age_trend=age_trend, plot_mods_path=plot_mods_path, username=username, proj,
                             weight_col="out_rmse", by_sex=by_sex, location_set_id=22)
  
  
  setnames(ave_models, "ave_result", "cv_custom_stage_1")
  ##sy: drop some uneccessary cols for saving
  ave_models[, c("location_name", "region_name", "super_region_name", "age_group_name"):=NULL]
  
  if(save_ensemble==T){
    write.csv(ave_models, file=ensemble_output, row.names=F)
    print('saved at', ensemble_output)
  }
}