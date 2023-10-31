## Docstring ----------------------------------------------------------- ----
## Project: NF COVID
## Script: 
## Description: Russian Data recoded_using 1000+ data
## Date 1/27/2021
## --------------------------------------------------------------------- ----

library(readxl)
library(data.table)
## Environment Prep ---------------------------------------------------- ----
#remove the history and call the GBD functions
rm(list=ls(all.names=T))

version <- 0

if (!exists(".repo_base")) {
  .repo_base <- 'FILEPATH'
}

source(paste0(.repo_base, 'FILEPATH/utils.R'))
source(paste0("ROOT" ,"FILEPATH/get_location_metadata.R"))
source(paste0("ROOT" ,"FILEPATH/get_age_metadata.R"))

## --------------------------------------------------------------------- ----
#

#module 1
#selecting patients by ICD dx, rather than PCR
mod1 <- read_excel('FILEPATH',
                 sheet="Core Module 1")[,c("subjid","Age","Sex","Diagnosis (ICD)")]
mod1 <- mod1[mod1$`Diagnosis (ICD)` %ni% c("NA"),]

summary(mod1$Age)
#creating age group by GBD
ages <- get_age_metadata(age_group_set_id = 19, 
                         gbd_round_id = roots$gbd_round)[, !c('most_detailed')][,c('age_group_id','age_group_name')]


mod12 <- mutate(mod1, age_group_name = ifelse(Age %in% 15:19, "15 to 19",
                  ifelse(Age %in% 20:24, "20 to 24",
                  ifelse(Age %in% 25:29, "25 to 29",
                  ifelse(Age %in% 30:34, "30 to 34",
                  ifelse(Age %in% 35:39, "35 to 39",
                  ifelse(Age %in% 40:44, "40 to 44",
                  ifelse(Age %in% 45:49, "45 to 49",
                  ifelse(Age %in% 50:54, "50 to 54", 
                  ifelse(Age %in% 55:59, "55 to 59",
                  ifelse(Age %in% 60:64, "60 to 64",
                  ifelse(Age %in% 65:69, "65 to 69",
                  ifelse(Age %in% 70:74, "70 to 74",
                  ifelse(Age %in% 75:79, "75 to 79",
                  ifelse(Age %in% 80:84, "80 to 84",
                  ifelse(Age %in% 85:89, "85 to 89",
                  ifelse(Age %in% 90:94, "90 to 94", "95_plus")))))))))))))))))

age_mod12 <- merge(mod12, ages, by='age_group_name')

#warning messages due to "NR" missing values
followup <- read_excel('FILEPATH/StopCovid Cohort_Database for GBD full cohort.xlsx', 
                       sheet="Follow-up module")[,c("subjid", "flw_confusion",
                                                    "flw_eq5d_ad_2",'per_fat','flw_eq5d_ua_2', 'flw_eq5d_pd_2', 'flw_eq5d_pd',
                                                    'flw_breathless_now','flw_limb_weakness','flw_fatigue','for_cov',
                                                    'flw_remember_today','flw_pers_cough','phq_4_stress',
                                                    'phq_4_worries')]


# for some reason, in the raw followup data, flw_eq5d_pd contains the responses at follow-up, and flw_eq5d_pd_2 contains the 
#  responses pre-COVID.  Switching them in the code now so the algorithm is logical later
followup$flw_eq5d_pd_2_orig <- followup$flw_eq5d_pd_2
followup$flw_eq5d_pd_orig <- followup$flw_eq5d_pd
followup$flw_eq5d_pd_2 <- followup$flw_eq5d_pd_orig
followup$flw_eq5d_pd <- followup$flw_eq5d_pd_2_orig

mod12_fol <- merge(age_mod12, followup)

#getting the number of patients by age and by sex 
sample_n <-mod12_fol %>% 
  group_by(age_group_id,age_group_name,Sex ) %>%
  summarise(N=n()) 

#getting the number of patients by sex 
sample_n_bysex <-mod12_fol %>% 
  group_by(Sex) %>%
  summarise(N=n()) 

#post-acute consequences of infectious disease
# •	Post-acute consequences of infectious disease (fatigue, emotional lability, insomnia)
# lay description: “is always tired and easily upset. The person feels pain all over the body and is depressed”
post <- mod12_fol[,c("subjid",'flw_limb_weakness','per_fat','flw_fatigue','flw_eq5d_ua_2', 'flw_eq5d_pd_2', "flw_eq5d_ad_2")]
post$flw_fatigue <- as.numeric(post$flw_fatigue)

post <- mutate(post, post_acute = ifelse((per_fat %in% 1 | flw_limb_weakness %in% 1) & (flw_fatigue >=3 | is.na(flw_fatigue)) & (flw_eq5d_ad_2 > 2 | flw_eq5d_pd_2 > 2 ), 1,0))

table(post$post_acute)
summary(post$post_acute)

#•	cognition problems 
cog <- mod12_fol[,c("subjid","flw_confusion",'for_cov','flw_remember_today',
                    'flw_limb_weakness','per_fat','flw_fatigue','flw_eq5d_ua_2', 'flw_eq5d_pd_2')]

# o	lay description for mild dementia: “has some trouble remembering recent events, 
# and finds it hard to concentrate and make decisions and plans”

#removing 
"o	Allow one of the defining items (per_fat, flw_limb_weakness, 
#flw_fatigue, flw_eq5d_ad_2, and flw_eq5d_pd_2) to have missing value"
cog$na_count <- apply(cog, 1, function(x) {sum(is.na(x)) + sum(x == "NR", na.rm=TRUE)})
cog <- cog[cog$na_count <=1,]

## o	lay description for mild dementia: “has some trouble remembering recent events, 
# and finds it hard to concentrate and make decisions and plans”

#mild cognitive problem
cog <- mutate(cog, cog_mild = ifelse((((for_cov %in% 1 | flw_confusion  %in% 1) & (flw_remember_today %in% 2) & (flw_eq5d_ua_2 >=3)) |
                                        ((for_cov %in% 1 | flw_confusion  %in% 1) & (flw_remember_today %in% 3) & ((flw_eq5d_ua_2 %in% c(2,3)) ))),1,0))
table(cog$cog_mild)
summary(cog$cog_mild)

#moderate cognitive problem
# o	lay description for moderate dementia: “has memory problems and confusion, feels disoriented, at times hears voices that are not real, and needs help with some daily activities”
#mild
cog <- mutate(cog, cog_moderate = ifelse((((for_cov %in% 1 | flw_confusion  %in% 1) & (flw_remember_today %in% 3) & (flw_eq5d_ua_2 >=4)) |
                                            ((for_cov %in% 1 | flw_confusion  %in% 1) & (flw_remember_today %in% 4) & (flw_eq5d_ua_2 >=2))),1,0))
table(cog$cog_moderate)

#combined cognitive category(if cog_mild =1 | cog_moderate =1, then cognitive =1 )
cog <- mutate(cog, cognitive = ifelse((cog_mild %in% 1 | cog_moderate %in% 1), 1, 0))
table(cog$cognitive)
summary(cog$cognitive)

# •	respiratory problems 
# o	lay description for mild COPD: “has cough and shortness of breath after heavy physical activity, 
# but is able to walk long distances and climb stairs”
# mild 
res <- mod12_fol[, c('subjid','flw_breathless_now', 'flw_pers_cough','flw_eq5d_ua_2','flw_fatigue','phq_4_stress', 'phq_4_worries')]
res$na_count <- apply(res, 1, function(x) {sum(is.na(x)) + sum(x == "NR", na.rm=TRUE)})
res <- res[res$na_count <=1,]

res <- mutate(res, res_mild = ifelse((((flw_breathless_now %in% 2 | (flw_pers_cough %in% 1 & flw_breathless_now <=3 )) & flw_eq5d_ua_2 %in% 2) | 
                                        ((flw_breathless_now %in% 3) & (flw_pers_cough %in% 1) & (flw_eq5d_ua_2 %in% 2)) |
                                        (flw_breathless_now %in% 3 & flw_pers_cough %in% 0 & flw_eq5d_ua_2 >= 3)), 1, 0))
table(res$res_mild)
summary(res$res_mild)
#res_moderate
res <- mutate(res, res_moderate = ifelse(((flw_breathless_now %in% 3 & flw_pers_cough %in% 1 & (flw_eq5d_ua_2 >= 3 | flw_fatigue %in% c(4,5,6))) | 
                                          (flw_breathless_now %in% 4 & flw_eq5d_ua_2 %in% 2 & flw_fatigue <7 & (phq_4_stress <3 & phq_4_worries <3)) |
                                          (flw_breathless_now %in% 5 & (flw_eq5d_ua_2 %in% 2 | flw_fatigue %in% c(4,5,6) ))), 1, 0))
table(res$res_moderate)
summary(res$res_moderate)

#res_severe 
#o	lay description for severe COPD: “has cough, wheezing and shortness of breath all the time. 
#The person has great difficulty walking even short distances or climbing any stairs, 
#feels tired when at rest, and is anxious”
res <- mutate(res, res_severe = ifelse(((flw_breathless_now %in% 4 & (flw_eq5d_ua_2 >= 3 | flw_fatigue >=7 | phq_4_stress %in% c(3,4) | phq_4_worries %in% c(3,4))) | 
                                          (flw_breathless_now %in% 5 & (flw_eq5d_ua_2 >= 3 | flw_fatigue >=7))), 1, 0))
table(res$res_severe)
summary(res$res_severe)
#combined to the respiratory category 
res <- mutate(res, res_combine = ifelse((res_mild %in% 1 | res_moderate %in% 1 | res_severe %in% 1), 1, 0))


a1 <- merge(age_mod12, post[,c('subjid','post_acute')])
a2 <- merge(a1, res[, c('subjid', 'res_mild',"res_moderate","res_severe", "res_combine")])
a3 <- merge (a2,cog[, c('subjid','cog_mild','cog_moderate','cognitive')])

#combine the resp, cognitive and fatigue categories 
a3 <- mutate(a3, Cog_Res = ifelse((cognitive %in% 1 & res_combine %in% 1), 1, 0 ))
a3 <- mutate(a3, Cog_Fat = ifelse((cognitive %in% 1 & post_acute %in% 1), 1, 0 ))
a3 <- mutate(a3, Res_Fat = ifelse((res_combine %in% 1 & post_acute %in% 1), 1, 0 ))
a3 <- mutate(a3, Cog_Res_Fat = ifelse((res_combine %in% 1 & post_acute %in% 1
                                                 & cognitive %in% 1), 1, 0 ))
#all long term category
a3 <- mutate(a3, long_term = ifelse((cognitive %in% 1 | res_combine %in% 1 | post_acute %in% 1), 1, 0 ))
table(a3$long_term)
summary(a3$long_term)

a3$overlap <- rowSums(a3[,c("res_mild","res_moderate","res_severe")])
#Handling the cases that the patients falling in more than one(mild, moderate, severe) category
#they always move to the more severe state

DT = as.data.table(a3)
DT[(overlap >1 & res_mild==res_moderate), res_mild := 0]
DT[(overlap >1 & res_moderate== res_severe), res_moderate := 0]
write.csv(DT, paste0("FILEPATH/Russia_all_data_Wo_pre_v", version, ".csv"))

#merge with age 
inter <- merge(ages, DT)[,!c('subjid','Age','overlap','Diagnosis (ICD)')]

#tabulate 
#cols <- c("age_group_id","age_group_name", "sex","icu_hoterm","COVID_PCR")
data_long <- gather(inter , measure, value, c(post_acute:long_term) )

cocurr_mu <-data_long %>%
  group_by(age_group_id,age_group_name,Sex, measure) %>%
  filter(value=='1') %>%
  summarise(var=n()) %>%
  spread(measure, var)

cocurr_mu <- merge(x=cocurr_mu, y=sample_n, all.y=TRUE)
cocurr_mu[is.na(cocurr_mu)] <- 0

write.csv(cocurr_mu, paste0("FILEPATH/Russian_byAgeSex_wo_pre_v", version, ".csv"))

#table by sex only 
cocurr_sex <-data_long %>% 
  group_by(Sex,measure) %>%
  filter(value=='1') %>%
  summarise(var=n()) %>%
  spread(measure, var) 


cocurr_sex <- merge(x=cocurr_sex, y=sample_n_bysex, all.y=TRUE)
cocurr_sex[is.na(cocurr_sex)] <- 0
write.csv(cocurr_sex, paste0("FILEPATH/Russia_bySex_wo_pre_v", version, ".csv"))


