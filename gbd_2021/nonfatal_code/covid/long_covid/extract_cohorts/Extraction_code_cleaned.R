#pull all data together for meta analysis
## Environment Prep ---------------------------------------------------- ----
#remove the history and call the GBD functions
rm(list=ls(all.names=T))

version <- 1

if (!exists(".repo_base")) {
  .repo_base <- 'FILEPATH'
}

source(paste0(.repo_base, 'FILEPATH/utils.R'))
source(paste0(ROOT,"FILEPATH/get_location_metadata.R"))
source(paste0(ROOT ,"FILEPATH/get_age_metadata.R"))

setwd("FILEPATH")
#import russia
Russia <- read.csv('FILEPATH/Russian_byAgeSex_10.csv')
Russia_res<-read.csv('FILEPATH/Russia_bySex_v10.csv')[,c('res_mild', 'res_moderate', 'res_severe', 'res_combine')]
Russia_cog <- read.csv('FILEPATH/Russia_bySex_v10.csv')[,c('cog_mild', 'cog_moderate','cognitive')]

Russia_res<-Russia_res %>% 
  summarize_if(is.numeric, sum, na.rm=TRUE) %>%
  rename(N= res_combine)
Russia_cog<-Russia_cog %>%
  summarize_if(is.numeric, sum, na.rm=TRUE) %>%
  rename(N= cognitive)

Russia_res <- gather(Russia_res, condition, measurement, res_mild:res_severe, factor_key=TRUE)
Russia_cog <- gather(Russia_cog, condition, measurement, cog_mild:cog_moderate, factor_key=TRUE)

Russia_cog <- rbind(Russia_cog, Russia_res)
Russia_cog$source <- "Russia"
#the end of special case

#by age sex Russia
Russia_nocogres <- subset(Russia, select=-c(X, age_group_id, res_mild, res_moderate, res_severe, cog_mild, cog_moderate))
data_Russia <- gather(Russia_nocogres,condition, measurement, Cog_Fat:Res_Fat, factor_key=TRUE)
data_Russia$source <- "Russia"
names(data_Russia)[names(data_Russia) == "Sex"] <- "sex"

#Russia both gender
Russia_both <- read.csv('FILEPATH/Russia_bySex_v10.csv')
Russia_both <- subset(Russia_both , select= -c(X, Sex, res_mild, res_moderate, res_severe, cog_mild, cog_moderate))
Russia_both<-Russia_both %>% 
  summarize_if(is.numeric, sum, na.rm=TRUE) 
Russia_both <- gather(Russia_both , condition, measurement, Cog_Fat:Res_Fat, factor_key=TRUE)
Russia_both$source <- "Russia"

#import Iran
Iran_res <- read.csv('FILEPATH/Iran_bySex_v1.csv')[,c('res_mild', 'res_moderate', 'res_severe', 'res_combine')]
Iran_res<-Iran_res %>% 
  summarize_if(is.numeric, sum, na.rm=TRUE) %>%
  rename(N= res_combine)
Iran_res <- gather(Iran_res, condition, measurement, res_mild:res_severe, factor_key=TRUE)
Iran_res$source <- "Iran"

#by age sex
Iran_by_agesex <- read.csv('FILEPATH/Iran_byAgeSex_1.csv')
Iran_by_agesex <- subset(Iran_by_agesex, select= -c(X,res_moderate,res_severe,res_mild))
Iran_age_sex <- gather(Iran_by_agesex , condition, measurement, Cog_Fat:Res_Fat, factor_key=TRUE)
names(Iran_age_sex)[names(Iran_age_sex) == "Dm3"] <- "sex"
Iran_age_sex$source <- "Iran"

#by sex
Iran_by_sex <- read.csv('FILEPATH/Iran_bySex_v1.csv')
Iran_by_sex <- subset(Iran_by_sex , select= -c(X,res_moderate,res_severe,res_mild))
Iran_by_sex <- gather(Iran_by_sex , condition, measurement, Cog_Fat:Res_Fat, factor_key=TRUE)
names(Iran_by_sex)[names(Iran_by_sex) == "Dm3"] <- "sex"
Iran_by_sex$source <- "Iran"

#both sex
Iran_both <- read.csv('FILEPATH/Iran_bySex_v1.csv')
Iran_both <- subset(Iran_both , select= -c(X,res_moderate,res_severe,res_mild, Dm3))
Iran_both<-Iran_both %>% 
  summarize_if(is.numeric, sum, na.rm=TRUE) 
Iran_both <- gather(Iran_both , condition, measurement, Cog_Fat:Res_Fat, factor_key=TRUE)
Iran_both$source <- "Iran"

#import italian Adult age 
Italy_adult_age <- read.csv('FILEPATH/Italy_adult_bySex_v1.csv')
Italy_adult_age <- subset(Italy_adult_age, select=-c(X))
Italy_adult_age <- gather(Italy_adult_age , condition, measurement, cog:res, factor_key=TRUE)
Italy_adult_age$source <-"Italy"

#Italian Adult Age Sex
Italy_adult_age_sex <- read.csv('FILEPATH/Italy_adult_byAgeSex_1.csv')
Italy_adult_age_sex <- subset(Italy_adult_age_sex, select=-c(X))
Italy_adult_age_sex <- gather(Italy_adult_age_sex, condition, measurement, cog:res, factor_key=TRUE)
Italy_adult_age_sex$source <-"Italy"

#Italian adult both
Italy_both_adult <- read.csv('FILEPATH/Italy_adult_bySex_v1.csv')
Italy_both_adult <- subset(Italy_both_adult, select=-c(X, sex))
Italy_both_adult<-Italy_both_adult %>% 
  summarize_if(is.numeric, sum, na.rm=TRUE) 
Italy_both_adult <- gather(Italy_both_adult , condition, measurement, cog:res, factor_key=TRUE)
Italy_both_adult$source <-"Italy"


#Italian ped age 
Italy_ped_age <- read.csv('FILEPATH/Italy_ped_bySex_v5.csv')
Italy_ped_age <- subset(Italy_ped_age, select=-c(X))
Italy_ped_age <- gather(Italy_ped_age , condition, measurement, cog:Res_Fat, factor_key=TRUE)
Italy_ped_age$source <-"Italy"

#Italian ped age sex 
Italy_ped_age_sex <- read.csv('FILEPATH/Italy_ped_byAgeSex_5.csv')
Italy_ped_age_sex  <- subset(Italy_ped_age_sex , select=-c(X))
Italy_ped_age_sex  <- gather(Italy_ped_age_sex, condition, measurement, cog:Res_Fat, factor_key=TRUE)
Italy_ped_age_sex $source <-"Italy"

#Italian ped both
Italy_ped_both <- read.csv('FILEPATH/Italy_ped_bySex_v5.csv')
Italy_ped_both <- subset(Italy_ped_both, select=-c(X, sex))
Italy_ped_both<-Italy_ped_both %>% 
  summarize_if(is.numeric, sum, na.rm=TRUE) 
Italy_ped_both <- gather(Italy_ped_both , condition, measurement, cog:Res_Fat, factor_key=TRUE)
Italy_ped_both$source <-"Italy"

#Austria sex
austria_age <- read.csv('FILEPATH/Austria_bySex_v0.csv')
austria_age <- subset(austria_age, select=-c(X))
austria_age <- gather(austria_age , condition, measurement, cog:post_acute, factor_key=TRUE)
austria_age$source <-"Austria"
#Austria Age sex
austria_age_sex <- read.csv('FILEPATH/Austria_byAgeSex_0.csv')
austria_age_sex  <- subset(austria_age_sex , select=-c(X))
austria_age_sex  <- gather(austria_age_sex, condition, measurement, cog:post_acute, factor_key=TRUE)
austria_age_sex$source <-"Austria"

#Austria Both
austria_both <- read.csv('FILEPATH/Austria_bySex_v0.csv')
austria_both <- subset(austria_both, select=-c(X, sex))
austria_both <- austria_both %>% 
  summarize_if(is.numeric, sum, na.rm=TRUE) 
austria_both  <- gather(austria_both, condition, measurement, cog:post_acute, factor_key=TRUE)
austria_both $source <-"Austria"

#UW HAARVIE Sex
UW <- read.csv('FILEPATH/UW_bySex_v2.csv')
colnames(UW)
UW_age <- subset(UW, select=-c(X))
UW_age <- gather(UW_age , condition, measurement, Cog_Fat:Res_Fat, factor_key=TRUE)
UW_age$source <-"UW"
write.csv(UW_age, 'FILEPATH/uw_age.csv')


#UW HAARVIE Age Sex
UW_age_sex <- read.csv('FILEPATH/UW_byAgeSex_2.csv')
UW_age_sex  <- subset(UW_age_sex, select=-c(X))
UW_age_sex  <- gather(UW_age_sex, condition, measurement, Cog_Fat:Res_Fat, factor_key=TRUE)
UW_age_sex$source <-"UW"
write.csv(UW_age_sex, 'FILEPATH/uw_age_sex.csv')

#Germany Both
Germen_both <- read.csv('FILEPATH/Germany_byICU_Hospitalized_followup_0.csv')
Germen_both <- subset(Germen_both, select=-c(X))
Germen_both <- gather(Germen_both , condition, measurement, Cog_Fat:res_severe, factor_key=TRUE)
Germen_both$source <-"Germany"
write.csv(Germen_both, 'FILEPATH/Germany_both.csv')
#Germany Sex
Germany_sex <- read.csv('FILEPATH/Germany_bySexICU_Hospitalized_followup_0.csv')
colnames(Germany_sex)
Germany_sex <- subset(Germany_sex, select=-c(X))
Germany_sex <- gather(Germany_sex , condition, measurement, Cog_Fat:res_severe, factor_key=TRUE)
Germany_sex$source <-"Germany"
write.csv(Germany_sex, 'FILEPATH/Germany_sex.csv')

#Germany Age Sex
Germany_age_sex <- read.csv('FILEPATH/Germany_byAgeSexICU_Hospitalized_followup_0.csv')
Germany_age_sex <- subset(Germany_age_sex, select=-c(X))
Germany_age_sex <- gather(Germany_age_sex, condition, measurement, Cog_Fat:res_severe, factor_key=TRUE)
Germany_age_sex$source <-"Germany"
write.csv(Germany_age_sex, 'FILEPATH/Germany_age_sex.csv')

#append the cog res datasets together, only Iran and Russia
All_special <- rbind(Russia_cog, Iran_res)

#append by sex together
All_by_sex <- rbind(Iran_by_sex, austria_age)
#Italy_by_sex <- rbind( Italy_adult_age, Italy_ped_age)

#append by sex age together
All_by_sex_age <- rbind(data_Russia, Iran_age_sex, austria_age_sex)
#Italy_by_age_sex <-rbind(Italy_adult_age_sex, Italy_ped_age_sex)

#append both sex together
All_both <- rbind(Russia_both, Iran_both, austria_both )
#Italy_both <- rbind(Italy_both_adult,Italy_ped_both)



write.csv(All_special, 'FILEPATH/cog_res.csv')
write.csv(All_by_sex, 'FILEPATH/bySex.csv')
write.csv(All_by_sex_age, 'FILEPATH/bySexAge.csv')
write.csv(All_both, 'FILEPATH/both_sex.csv')

write.csv(Italy_by_sex, 'FILEPATH/Italy_by_sex.csv')
write.csv(Italy_by_age_sex, 'FILEPATH/Italy_by_age_sex.csv')
write.csv(Italy_both, 'FILEPATH/Italy_both.csv')
