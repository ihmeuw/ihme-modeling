
###Read in analysis data set - one row per year*location, with Zika births for that year*location
czs_data<- read.csv("FILEPATH")

library(lme4)


##POISSON MODEL 
mod_p<-glmer(cases~ offset(log(zikaBirths))+(1|modLocation)+(1|year_id), family=poisson, data=subset(czs_data, czs_data$zikaBirths>0))

#get model intercept
summary(mod_p)


##PULL PREDICTED RANDOM EFFECTS
random_effects<-as.data.frame((ranef(mod_p)))
random_effects$exp<-(random_effects$condval)

#manually put the intercept here
intercept<-(-2.7626)

#extract the random effects - location
loc_random<-random_effects[random_effects$grpvar=="modLocation",]
loc_random$modLocation<-as.numeric(as.character((loc_random$grp)))
loc_random$exp2<-loc_random$exp
loc_random$exp<-NULL

#extract the random effects - year
yr_random<-random_effects[random_effects$grpvar=="year_id",]
yr_random$year_id<-as.numeric(as.character((yr_random$grp)))

#merge on location and year
dat3<-merge(czs_data, loc_random, by="modLocation", all.x = TRUE)
dat4<-merge(dat3, yr_random, by="year_id", all.x = TRUE)

#remove NAs in exp(year) or exp(location)
dat4$intercept<- (-2.7626)

dat4$exp_year<- dat4$exp
dat4$exp_year[is.na(dat4$exp)] <- 0

dat4$exp_loc<- dat4$exp2
dat4$exp_loc[is.na(dat4$exp2)] <- 0


#predict rate = exp(intercept+year random effect + loc random effect)
dat4$prediction_czs_log_space<- dat4$intercept + dat4$exp_year + dat4$exp_loc
dat4$prediction_czs_exp<- exp(dat4$prediction_czs_log_space)


#cleaning and formatting prediction table
dat5 <- dat4[c(2,1,3,4,5,6,7,30,80,86,87,88,89,90,91)]
dat5 <- dat5[order(dat5$modLocation, dat5$year_id),]


###SAVE PREDCITIONS 
#summary(dat5$prediction_czs_exp)

write.csv(dat5, "FILEPATH")

