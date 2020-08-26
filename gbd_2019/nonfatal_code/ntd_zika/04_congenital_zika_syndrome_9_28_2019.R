
library(dplyr)
library(data.table)
library(lme4)

source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")

### Read in analysis data set - one row per year*location, with Zika births for that year*location
czs_data<- read.csv("FILEPATH")        ## analysis_dataset_czs_v2

##get population age_group < 1 year 
czs_data<-czs_data[!is.na(czs_data$location_id),]
locs<-unique(czs_data$location_id)

##data are only from these years
under_1<-get_population(age_group_id =ADDRESS, decomp_step = "step4", location_id=c(locs),location_set_id = 35, year_id=c(2015,2016,2017))

mod_dat<-merge(czs_data,under_1,by=c("location_id","year_id"))

plot(mod_dat$sample_size,mod_dat$population)

##estimate ratio of zika births to pop <1
mod_dat$ratio<-mod_dat$zikaBirths/mod_dat$sample_size

##correct for possible bias in the zika births estimate- if ratio is below the mean 0.0037
summary(mod_dat$ratio)

mod_dat2<-as.data.table(mod_dat)

mod_dat2<-mod_dat2%>%
    mutate(zikaBirths_new=ifelse(zikaBirths<100,zikaBirths+100,zikaBirths))

plot(mod_dat2$zikaBirths,mod_dat2$zikaBirths_new)

mod_p<-glmer(cases~ offset(log(zikaBirths))+(1|modLocation)+(1|year_id), family=poisson, data=subset(czs_data, czs_data$zikaBirths>0))



#PULL THE RANDOM EFFECTS FROM THE MODEL
random_effects<-as.data.frame((ranef(mod_p)))
random_effects$exp<-(random_effects$condval)

##read this output for the model intercept
summary(mod_p)
intercept<-(-2.7626)

#extract the random effects- location
loc_random<-random_effects[random_effects$grpvar=="modLocation",]
loc_random$modLocation<-as.numeric(as.character((loc_random$grp)))
loc_random$exp2<-loc_random$exp
loc_random$exp<-NULL

#extract the random effects - year
yr_random<-random_effects[random_effects$grpvar=="year_id",]
yr_random$year_id<-as.numeric(as.character((yr_random$grp)))

#merge on loc and yr
dat3<-merge(czs_data,loc_random, by="modLocation")
dat4<-merge(dat3,yr_random, by="year_id")

#predict rate = exp(intercept + year random effect + loc random effect)
dat4$prediction_czs<-exp(intercept+dat4$exp+dat4$exp2)

dat4$cases_pred<-dat4$prediction_czs*dat4$zikaBirths

#look at the distribution, does it seem reasonable? 
summary(dat4$prediction_czs)
summary(dat4$cases_pred)
#two data points are outlier needs to be dropped - prediction is way too high

test<-dat4[dat4$cases<2000,]

plot(test$cases,test$cases_pred)
summary(dat4$cases_pred)

#remove two outlier predictions
dat4<-dat4%>%
    mutate(cases_pred = ifelse(cases_pred>2000, cases, cases_pred))

#from cases_pred, simulate uncertainty from a Poisson distribution
#generate cases from a Poisson distribution



#GENERATE 1000 DRAWS OF CASES OF CZS
draws.required <- 1000
draw.cols <- paste0("draw_", 0:999)


#keep only location_id, cases_pred, year_id
keeps <- c("location_id", "cases_pred", "year_id")
df_zk = dat4[keeps]

df_zk2<-as.data.table(df_zk)

df_zk2$cases_pred<-ceiling(df_zk2$cases_pred)

df_zk2[, id := .I]
df_zk2[, (draw.cols) := as.list(rpois(draws.required, cases_pred)), by=id]


#divide them by the under-1-y-old population to get the rate overall
unique_locs<- df_zk2$location_id
under_1_pop<-get_population(age_group_id =ADDRESS, decomp_step = "step4", location_id=c(unique_locs),location_set_id = 35, year_id=c(2015,2016,2017))

df_zk3<-merge(df_zk2,under_1_pop,by=c("location_id","year_id"))

#split by birth prevalence
#split into the age groups that you need to submit
#submit the draws 
