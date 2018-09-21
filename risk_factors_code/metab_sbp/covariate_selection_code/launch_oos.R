##oos launch
##for using the test_prior function

################################
##author:USERNAME
##date: 3/30/2017
##Had to recover 6/5, not re-built for new system
#
#


rm(list=ls())
os <- .Platform$OS.type
if (os=="windows") {
  lib_path <- "FILEPATH"
  j<- "FILEPATH"
  h<-"FILEPATH"
} else {
  lib_path <- "FILEPATH"
  j<- "FILEPATH"
  h<-"FILEPATH"
}

date<-gsub("-", "_", Sys.Date())

source("FILEPATH")


################### ARGS #########################################
#####################################################
me<-"sbp"

output<-paste0(j, "FILEPATH", me, "_prior_test_", date, ".csv")





data_transform<-"logit"

data_id<-3906
#num_tempfiles<-10  ##USERNAME: number of knockouts+1 (the extra one is for the training dataset, to calculate is.rmse)

##USERNAME:model specifications
if(me=="sbp"){
  cov_list<-c("mean_BMI", "prev_obesity", "smoking_prev", "fruits_g_adj", "omega_3_g_adj", "sodium_age_spec","alcohol_lpc",  "vegetables_g_adj", "sdi", "prev_overweight", "haqi", "nuts_seeds_g_adj")  ##USERNAME: alcohol binge has some missing values, is breaking stuffs
  ##USERNAME: if pulling in custom covariates::need covariate name short(as stored in custom covariate file) and filepath to custom cov
  custom_covs<-list(c("sodium_age_spec", paste0(j, "FILEPATH")))
  ban_pairs<-list(c("prev_overweight", "mean_BMI", "prev_obesity")) #, c("mean_BMI", "prev_obesity", "prev_overweight"))  ##USERNAME: specify banned sets in the form of a list of character vectors
  run_id<-11381
  intrxn<-NULL      #c("prev_overweight:factor(age_group_id")
  polynoms<-c("sdi^2")
  modtype<-"lmer"
  rank_method="oos.rmse"
}

if(me=="chl"){
  cov_list<-c("pufa_adj_pct", "fiber_g_adj", "fruits_g_unadj", "haqi", "nuts_seeds_g_adj", "sdi", "prev_overweight")
  custom_covs<-NULL
  ban_pairs<-NULL  ##USERNAME: specify banned sets in the form of a list of character vectors
  run_id<-12683
  intrxn<-NULL
  polynoms<-NULL
  modtype<-"lmer"
}



##USERNAME: specify model covs
#cov_list<-c("sdi", "haqi", "fruits_g_adj", "fruits_g_unadj", "omega_3_g_adj")  ##USERNAME: testing list
#custom_covs<-NULL
#ban_pairs<-list(c("fruits_g_adj", "fruits_g_unadj"), c("sdi", "haqi", "omega_3_adj"))
#prior_sign<-c(1, -1, 0)
fixed_covs<-c("as.factor(age_group_id)")
random_effects<-c("(1|super_region_name)", "(1|region_name)")  ##USERNAME: need to be in random effects formula format

##USERNAME: specify other model arguments
intrxn<-NULL  ##USERNAME: working on this, it should work for simple cases.  If no intrxn, must set to NULL
##USERNAME: haven't incorporated this yet
prior_sign<-NULL  ##USERNAME:place to input expected direction of covariate.  Not yet implemented
by_sex<-T




################### LAUNCH #########################################
#####################################################

#cov_list<-"sdi" for testing

rmses.t<-test_prior(run_id=run_id, cov_list=cov_list, data_transform = data_transform,
                    count_mods=F, rank_method=rank_method,
                    custom_covs=custom_covs, fixed_covs=fixed_covs, random_effects = random_effects, ban_pairs=ban_pairs,
                    by_sex=by_sex,
                    polynoms=polynoms
)



write.csv(rmses.t, file=output)