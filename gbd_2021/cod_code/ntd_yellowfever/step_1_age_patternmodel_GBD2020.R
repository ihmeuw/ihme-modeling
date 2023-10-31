# Purpose: Update age distribution file for GBD 2020 - yellow fever


user <- Sys.info()[["user"]] ## Get current user name
path <- paste0("FILEPATH")



library(data.table)
library(MASS)
library(splines)
require(dplyr)
library(tibble)
library(lme4)
rm(list = ls())

os <- .Platform$OS.type
if (os == ADDRESS) {
  prefix <- "FILEPATH"
} else {
  prefix <- "FILEPATH"
}		

source("FILEPATH/get_location_metadata.R")
source("FILEPATH/get_demographics.R")
source("FILEPATH/get_population.R")
source("FILEPATH/get_ids.R")
source("FILEPATH/get_covariate_estimates.R")
source("FILEPATH/save_results_epi.R")
source("FILEPATH/get_bundle_data.R")


#import csv of age groups
ages<-read.csv("FILEPATH")

#make a copy to create females
ages_f<-ages

#add sex id for males
ages$sex_id<-1
ages_f$sex_id<-2

#append them together
ages_t<-rbind(ages_f,ages)


#pull in age-specific data from bundle
###NEED TO GET THIS TO WORK
bundle_id <- ADDRESS
decomp_step <- ADDRESS
gbd_round_id = ADDRESS
bundle_df <- get_bundle_data(bundle_id, decomp_step, gbd_round_id=gbd_round_id)

#create age_mid variable
bundle_df$age_mid<-((bundle_df$age_start+bundle_df$age_end)/2)

bundle_df<-bundle_df[bundle_df$sex!="Both",]

bundle_df2<- bundle_df %>% mutate(sex_id=recode(sex, 
                         "Male"=1,
                         "Female"=2,
                          "Both"=0))

#model age pattern

#create cubic spline variables, knots at 0.01, 1, 10 and 40 -for age)


knots=c(.01,1, 10, 40)
mod1<-glm.nb(cases~as.factor(sex_id)+ns(age_mid, knots=knots)+offset(log(sample_size)), data=bundle_df2)
summary(mod1)

#extract fixed effects and store
fe<-as.data.frame(coef(mod1))

write.csv(fe,"FILEPATH")

#need an effective sample size variable to impute age curve - impute with brazil


ages_ids<-unique(ages_t$age_group_id)
braz<-get_population(gbd_round_id=ADDRESS,location_id=135, age_group_id=ages_ids,year_id=2000,decomp_step=ADDRESS,sex_id=c(1,2))

ages_t2<-merge(ages_t,braz,by=c("sex_id","age_group_id"))
ages_t2$sample_size<-ages_t2$population

ages_t2$phat <- predict(mod1, ages_t2, type = "response")
head(ages_t2)
ages_t2$inc<-ages_t2$phat/ages_t2$population

plot(ages_t2$age_mid,ages_t2$inc)


write.csv(ages_t2,"FILEPATH")
