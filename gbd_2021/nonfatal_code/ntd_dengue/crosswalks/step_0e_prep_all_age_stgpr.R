### NTDs Dengue
#Description: Prepare all age data to ST-GPR


source("FILEPATH/get_location_metadata.R")
source("FILEPATH/get_demographics.R")
source("FILEPATH/get_population.R")
source("FILEPATH/get_ids.R")
source("FILEPATH/get_covariate_estimates.R")
source("FILEPATH/save_results_epi.R")
source("FILEPATH/get_age_metadata.R")
source("FILEPATH/get_draws.R")

#read all-age, sex-split cases here:
cases<-read.csv("FILEPATH/sex_split.csv")

#format for st/gpr

cases$val<-cases$mean
cases$age_group_id<-22

cases$variance<-cases$standard_error**2

write.csv(cases,"FILEPATH/allage_cases.csv")

location_set<- get_location_metadata(location_set_id = 35, gbd_round_id = ADDRESS, decomp_step=ADDRESS)

sa2<-merge(cases,location_set, by="location_id")

no_sa<-sa2[sa2$region_id!=159,]
no_sa_cv<-no_sa[no_sa$location_id!=203,]

write.csv(no_sa_cv,"FILEPATH/allage_cases_noSA_CV.csv")
