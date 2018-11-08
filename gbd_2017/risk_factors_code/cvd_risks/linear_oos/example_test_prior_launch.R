##oos launch-examples
##for using the test_prior function

################################
##author:USERNAME
##date: 3/30/2017
#
#


if(!exists("rm_ctrl")){
  rm(list=objects())
}


date<-gsub("-", "_", Sys.Date())

library(data.table)
library(ini, lib.loc="FILEPATH")


################### ARGS #########################################
#####################################################
if(!exists("me")){
  me<-"ldl"
  data_id<-15569
}

test_mods<-F
average<-T
save_ensemble<-T
n_mods<-50
plot_aves<-T
age_trend<-T

output<-paste0("FILEPATH", me, "_prior_test_yes_prior_sign", date, ".csv")  ##USERNAME: this is where I want to save the object returned by test_prior. Not necessary
data_output<-paste0("FILEPATH/", me,"_best_data.csv")
ensemble_output<-paste0("FILEPATH/", me,"_custom_prior_", date, ".csv")

plot_mods_path<-paste0("FILEPATH", me, "/")

################### SCRIPTS #########################################
#####################################################

source("FILEPATH/linear_oos/test_prior.R")
source("FILEPATH/linear_oos/assemble_prior.R")
source("FILEPATH/utility/get_recent.R")
source("FILEPATH/utility/bind_covariates.R")


################### ARGUMENTS: EXAMPLE 1 #########################################
#####################################################
##USERNAME: this is a more simple example, for ldl cholesterol.  Many of the arguments I specify are the defaults, but I'm being more explicit here
if(me=="ldl"){
  cov_list<-c("Mean BMI", "Prevalence of obesity",
              "fruits adjusted(g)", "nuts seeds adjusted(g)", "vegetables adjusted(g)",
              "Healthcare access and quality index", "Socio-demographic Index")
  prior_sign<-c(1, 1,
                -1, -1, -1,
                -1, 0)
  #prior_sign<-NULL
  custom_covs<-NULL
  ban_pairs<-list(c("mean_BMI", "prev_obesity"))
  # data_id<-10074
  # data_id<-7700 ##USERNAME:this one has only un-crosswalked data
  polynoms<-NULL
  modtype<-"lmer"
  count_mods<-F ##USERNAME: only use this after running with count_mods=T first!
  rank_method="oos.rmse"
  forms_per_job<-10
  drop_nids<-F
  remove_subnats<-T

}




################### ARGUMENTS: EXAMPLE 2 #########################################
#####################################################
##USERNAME:A more complicated example with some of the optional arguments specified.. prior_sign, polynomials, ban_pairs, etc
if(me=="sbp"){
  cov_list<-c("Socio-demographic Index", "Alcohol (liters per capita)",
              "vegetables adjusted(g)",
              "Mean BMI", "Prevalence of obesity", "Healthcare access and quality index",
              "omega 3 adjusted(g)", "Smoking Prevalence", "fruits adjusted(g)")
  prior_sign<-c(0, 0,
                -1,
                1, 1, -1,
                -1, 1, -1,
                1, -1) ##USERNAME: last row for custom covariates


  ##USERNAME: if pulling in custom covariates::need covariate name short(as stored in custom covariate file) and filepath to custom cov
  custom_covs<-list(c("cv_sodium_age_spec", paste0("FILEPATH/sodium_age_specific_filled.csv")),
                    c("cv_nuts_seeds_g_adj", paste0("FILEPATH/nuts_seeds_utla_fix.csv")))
  #ban_pairs<-list(c("prev_overweight", "mean_BMI", "prev_obesity")) ##USERNAME: specify banned sets in the form of a list of character vectors
  ban_pairs<-list(c("mean_BMI", "prev_obesity"))
  #prior_sign<-NULL
  #data_id<-15569
  #polynoms<-c("sdi^2")
  polynoms<-NULL
  modtype<-"lmer"
  rank_method="oos.rmse"
  forms_per_job<-30
  drop_nids<-F
  remove_subnats<-T

}

if(me=="cod"){
  cov_list<-c("Socio-demographic Index",
              "Healthcare access and quality index", "LDI (I$ per capita)")
  custom_covs<-NULL
  ban_pairs<-NULL
  prior_sign<-NULL
  #data_id<-12337
  #polynoms<-c("sdi^2")
  polynoms<-NULL
  modtype<-"lmer"
  rank_method="oos.rmse"
  forms_per_job<-4
  drop_nids<-F
  remove_subnats<-F
}

################### ARGUMENTS: BOTH RISKS #########################################
#####################################################
##USERNAME: these arguments are the same for both of my risks

fixed_covs<-c("as.factor(age_group_id)")
random_effects<-c("(1|super_region_name)", "(1|region_name)")  ##USERNAME: need to be in random effects formula format



##USERNAME: specify other model arguments
intrxn<-NULL  ##USERNAME: working on this, not yet implemented.
by_sex<-T
data_transform<-"log"

##USERNAME: ko args
ko_prop<-0.15
proj<-"proj_crossval"
slots_per_job<-3
kos<-5

count_mods<-F
username<-"syadgir"
location_set_id<-35



################### LAUNCH MODEL TESTS #########################################
#####################################################

if(test_mods==T){
  message("Testing submodels")
  rmses_and_data<-test_prior(data_id=data_id, cov_list=cov_list, data_transform=data_transform,
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
#####################################################
if(average==T){
  message("Averaging submodels and predicting")
  rmses<-get_recent(output<-paste0("FILEPATH/", me, "/models/"))  ##USERNAME: this is where I want to save the object returned by test_prior. Not necessary)
  data_path<-paste0("FILEPATH/", me, "/data/", me,"_best_data.csv")
  data<-fread(data_path)

  ##USERNAME: I don't have documentation for this function yet. This is for creating an ensemble model of linear priors. Currently not implementable in STGPR
  ave_models<-assemble_prior(data, rmses[drop==0,], cov_list, data_transform,
                             custom_cov_list=custom_covs, polynoms=polynoms, n_mods=n_mods,
                             plot_mods=plot_aves, age_trend=age_trend, plot_mods_path=plot_mods_path, username=username, proj, ##USERNAME: this line is all just for plotting
                             weight_col="out_rmse", by_sex=by_sex, location_set_id=22)


  setnames(ave_models, "ave_result", "cv_custom_prior")
  ##USERNAME: drop some uneccessary cols for saving
  ave_models[, c("location_name", "region_name", "super_region_name", "age_group_name"):=NULL]

  if(save_ensemble==T){
    write.csv(ave_models, file=ensemble_output, row.names=F)
  }
}

