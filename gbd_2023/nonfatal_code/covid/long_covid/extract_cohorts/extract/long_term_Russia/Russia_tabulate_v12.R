## Docstring ----------------------------------------------------------- ----
## Project: NF COVID
## Script: 
## Description: Russian Data recoded_using 1000+ data
## Contributors: NAME
## Date 1/13/2022
## Time stamp: 
## 8/2/2021 Made updates in the path and also included PCR=1 patients into the analysis
## 1/13/2022 working on the new Russian data
## --------------------------------------------------------------------- ----

library(readxl)
library(data.table)
## Environment Prep ---------------------------------------------------- ----
#remove the history and call the GBD functions
rm(list=ls(all.names=T))

version <- 15

if (!exists(".repo_base")) {
  .repo_base <- 'FILEPATH'
}

source(paste0(.repo_base, 'FILEPATH/utils.R'))
source(paste0(roots$k ,"FILEPATH/get_location_metadata.R"))
source(paste0(roots$k ,"FILEPATH/get_age_metadata.R"))

## --------------------------------------------------------------------- ----
#

tabulation <- function(path1, path2, wave_number, outpath, followup_time, typeData){
  
  if(wave_number=='wave_1'){
    mod1 <- 
      read_excel(path1)[,c("PIN","Age","Sex","COVID PCR")]
    mod1 <- mod1[mod1$`COVID PCR` %in% 1,]
  }
  
  else{
  mod1 <- 
    read_excel(path1)[,c("PIN","Age","Sex","Diagnosis (ICD)")]
  mod1 <- mod1[mod1$`Diagnosis (ICD)` %ni% c("NA"),]
  }
  
  message(paste("Average age: "), mean(mod1$Age))
  
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
  
  followup <- read_excel(path2)[,c("subjid", "flw_confusion", "flw_eq5d_ad",
                                   "flw_eq5d_ad_2",'per_fat','flw_eq5d_ua', 'flw_eq5d_ua_2', 'flw_eq5d_pd_2', 'flw_eq5d_pd',
                                   'flw_breathless_now','flw_breathless_pre_c19','flw_limb_weakness','flw_fatigue','for_cov',
                                   'flw_remember_today','flw_remember_pre_c19','flw_pers_cough','phq_4_stress',
                                   'phq_4_worries')]
  names(age_mod12)[names(age_mod12) == "PIN"] <- "subjid"
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
  post <- mod12_fol[,c("subjid",'flw_limb_weakness','per_fat','flw_fatigue','flw_eq5d_ua', 'flw_eq5d_ua_2', 'flw_eq5d_pd_2', 'flw_eq5d_pd',"flw_eq5d_ad","flw_eq5d_ad_2")]
  post$flw_fatigue <- as.numeric(post$flw_fatigue)
  
  post <- mutate(post, post_acute = ifelse((per_fat %in% 1 | flw_limb_weakness %in% 1) & (flw_fatigue >=3 | is.na(flw_fatigue)) & (flw_eq5d_ad_2 > 2 | flw_eq5d_pd_2 > 2 )
                                           & ((flw_eq5d_ad_2 > flw_eq5d_ad) |(flw_eq5d_pd_2 > flw_eq5d_pd) |(flw_eq5d_ua_2 > flw_eq5d_ua)), 1,0))
  
  
  #•	cognition problems 
  cog <- mod12_fol[,c("subjid","flw_confusion",'for_cov','flw_remember_today','flw_remember_pre_c19', 
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
  cog <- mutate(cog, cog_mild = ifelse((((for_cov %in% 1 | flw_confusion  %in% 1) & (flw_remember_today %in% 2) & (flw_eq5d_ua_2 >=3) & (flw_remember_today > flw_remember_pre_c19)) |
                                          ((for_cov %in% 1 | flw_confusion  %in% 1) & (flw_remember_today %in% 3) & ((flw_eq5d_ua_2 %in% c(2,3)) ) & (flw_remember_today > flw_remember_pre_c19))),1,0))

  cog <- mutate(cog, cog_moderate = ifelse((((for_cov %in% 1 | flw_confusion  %in% 1) & (flw_remember_today %in% 3) & (flw_eq5d_ua_2 >=4) & (flw_remember_today > flw_remember_pre_c19)) |
                                              ((for_cov %in% 1 | flw_confusion  %in% 1) & (flw_remember_today %in% 4) & (flw_eq5d_ua_2 >=2) & (flw_remember_today > flw_remember_pre_c19))),1,0))
  
  #combined cognitive category(if cog_mild =1 | cog_moderate =1, then cognitive =1 )
  cog <- mutate(cog, cognitive = ifelse((cog_mild %in% 1 | cog_moderate %in% 1), 1, 0))
  
  # •	respiratory problems 
  # o	lay description for mild COPD: “has cough and shortness of breath after heavy physical activity, 
  # but is able to walk long distances and climb stairs”
  # mild 
  res <- mod12_fol[, c("subjid",'flw_breathless_now', 'flw_pers_cough','flw_eq5d_ua_2','flw_eq5d_ua','flw_breathless_pre_c19','flw_fatigue','phq_4_stress', 'phq_4_worries')]
  res$na_count <- apply(res, 1, function(x) {sum(is.na(x)) + sum(x == "NR", na.rm=TRUE)})
  res <- res[res$na_count <=1,]
  
  res <- mutate(res, res_mild = ifelse((((flw_breathless_now %in% 2 | (flw_pers_cough %in% 1 & flw_breathless_now <=3 )) & flw_eq5d_ua_2 %in% 2) | 
                                          ((flw_breathless_now %in% 3) & (flw_pers_cough %in% 1) & (flw_eq5d_ua_2 %in% 2)) |
                                          (flw_breathless_now %in% 3 & flw_pers_cough %in% 0 & flw_eq5d_ua_2 >= 3)) &
                                         (flw_breathless_now > flw_breathless_pre_c19), 1, 0))

  #res_moderate
  res <- mutate(res, res_moderate = ifelse(((flw_breathless_now %in% 3 & flw_pers_cough %in% 1 & (flw_eq5d_ua_2 >= 3 | flw_fatigue %in% c(4,5,6))) | 
                                              (flw_breathless_now %in% 4 & flw_eq5d_ua_2 %in% 2 & flw_fatigue <7 & (phq_4_stress <3 & phq_4_worries <3)) |
                                              (flw_breathless_now %in% 5 & (flw_eq5d_ua_2 %in% 2 | flw_fatigue %in% c(4,5,6) ))) &
                                             (flw_breathless_now > flw_breathless_pre_c19), 1, 0))
  
  #res_severe 
  #o	lay description for severe COPD: “has cough, wheezing and shortness of breath all the time. 
  #The person has great difficulty walking even short distances or climbing any stairs, 
  #feels tired when at rest, and is anxious”
  res <-  mutate(res, res_severe = ifelse(((flw_breathless_now %in% 4 & (flw_eq5d_ua_2 >= 3 | (flw_fatigue >=7 & !is.na(flw_fatigue)) | phq_4_stress %in% c(3,4) | phq_4_worries %in% c(3,4))) |
                                             (flw_breathless_now %in% 5 & (flw_eq5d_ua_2 >= 3 | (flw_fatigue >=7 & !is.na(flw_fatigue))))) &
                                            (flw_breathless_now > flw_breathless_pre_c19), 1, 0))

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
  
  a3$overlap <- rowSums(a3[,c("res_mild","res_moderate","res_severe")])
  
  DT = as.data.table(a3)
  DT[(overlap >1 & res_mild==res_moderate), res_mild := 0]
  DT[(overlap >1 & res_moderate== res_severe), res_moderate := 0]
  DT$follow_up <- followup_time
  
  write.csv(DT, paste0(outpath ,wave_number, "_",followup_time,"_",typeData,"_all_data_marked_v_", version, ".csv"))
  
  inter <-merge(ages, DT)[,!c('subjid','Age','overlap')]
  
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
  cocurr_mu$follow_up <- followup_time
  
  write.csv(cocurr_mu, paste0(outpath ,wave_number,"_",followup_time, "_",typeData,"_byAgeSex_",version, ".csv"))
  
  #table by sex only 
  cocurr_sex <-data_long %>% 
    group_by(Sex,measure) %>%
    filter(value=='1') %>%
    summarise(var=n()) %>%
    spread(measure, var) 
  
  cocurr_sex <- merge(x=cocurr_sex, y=sample_n_bysex, all.y=TRUE)
  cocurr_sex[is.na(cocurr_sex)] <- 0
  cocurr_sex$follow_up <- followup_time
  
  write.csv(cocurr_sex, paste0(outpath ,wave_number,"_",followup_time,"_",typeData, "_bySex_v_", version, ".csv"))
}


#follow up time, 6 month, adult, wave 2
path1 <-'FILEPATH/Part1_2 wave U07.1.xlsx'
path2 <- 'FILEPATH/Adults Follow-up 6-8 months_2 wave.xlsx'
wave_number <- "wave_2"
outpath<-"FILEPATH"
followup_time <-"6_month"
tabulation(path1, path2, wave_number, outpath, followup_time, typeData="_")


#follow up time, 6-8 month, adult, wave 1
path1 <-'FILEPATH/Part-1-PCR-Upd-5060 copy.xlsx'
path2 <- 'FILEPATH/6-8 months Follow-up.xlsx'
wave_number <- "wave_1"
outpath<-"FILEPATH"
followup_time <-"6_month"
tabulation(path1, path2, wave_number, outpath, followup_time, typeData ="_" )

#follow up time, 12_month, adult, wave 1, initial
path1 <-'FILEPATH/Part-1-PCR-Upd-5060 copy.xlsx'
path2 <-'FILEPATH/Initial_12 months Follow-up.xlsx'
wave_number <- "wave_1"
outpath<-"FILEPATH"
followup_time <-"12_month"
tabulation(path1, path2, wave_number, outpath, followup_time, typeData ="Initial")

##########################################################################
##########################################################################
##########################################################################
#ongoing is missing some key variables, like the comparison variables. 
#ongoing is not included in the tabulation.
#ongoing 12 months
#wave 1
#the ongoing dataset have many variables with different names

#module 1
#read in the patient info
mod1 <- 
  read_excel('FILEPATH/Part-1-PCR-Upd-5060 copy.xlsx')[,c("PIN","Age","Sex","COVID PCR")]
#read in the follow up data
#comor <- read_excel('FILEPATH/Adults Follow-up 6-8 months_2 wave.xlsx')

mod1 <- mod1[mod1$`COVID PCR` %in% 1,]

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
followup_12 <- read_excel('FILEPATH/Ongoing_ 12 months Follow_up.xlsx')[,c("subjid", "flw_confusion_0006ff",
                                                    "flw_eq5d_ad_e29260",'flw_eq5d_ua_dd0e43',
                                                    'flw_breathless_now_6d83c8','flw_breathless_c67847','flw_limb_weakness_3b26c2','flw_fatigue_e07109','flw2_for_cov',
                                                    'flw_remember_today_3a83ec','flw_pers_cough_bb302b','flw_eq5d_pd_2_eafcd1')]
#using the 6-8 months as the pre, read in the 6-8 months
followup_6m <- read_excel('FILEPATH/6-8 months Follow-up.xlsx')[,c("subjid","for_cov",'per_fat','flw_eq5d_ua',"flw_eq5d_ad","flw_remember_pre_c19","flw_eq5d_pd")]
#in the dataset for_cov, flw2_for_cov in the 12 month dataset
#getting per fat, the flow_eq5d_ua and flw_eq5d_ad, remember from the 6-8 months followup data
#per fat is totally missing in the 12 month ongoing dataset use per fat from 6-8 months instead, because per_fat is binary.
follow_up <- merge(followup_12, followup_6m)

names(age_mod12)[names(age_mod12) == "PIN"] <- "subjid"
mod12_fol <- merge(age_mod12, follow_up)

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
names(mod12_fol)[names(mod12_fol) == "flw_limb_weakness_3b26c2"] <- "flw_limb_weakness"
names(mod12_fol)[names(mod12_fol) == "flw_fatigue_e07109"] <- "flw_fatigue"
names(mod12_fol)[names(mod12_fol) == "flw_eq5d_ua_dd0e43"] <- "flw_eq5d_ua_2"
names(mod12_fol)[names(mod12_fol) == "flw_confusion_0006ff"] <- "flw_confusion"
names(mod12_fol)[names(mod12_fol) == "flw_eq5d_ad_e29260"] <- "flw_eq5d_ad_2"
names(mod12_fol)[names(mod12_fol) == "flw_breathless_now_6d83c8"] <- "flw_breathless_now"
names(mod12_fol)[names(mod12_fol) == "flw_breathless_c67847"] <- "flw_breathless_pre_c19"
names(mod12_fol)[names(mod12_fol) == "flw2_for_cov"] <- "for_cov"
names(mod12_fol)[names(mod12_fol) == "flw_pers_cough_bb302b"] <- "flw_pers_cough"
names(mod12_fol)[names(mod12_fol) == "flw_remember_today_3a83ec"] <- "flw_remember_today"
names(mod12_fol)[names(mod12_fol) == "flw_eq5d_pd_2_eafcd1"] <- "flw_eq5d_pd_2"

post <- mod12_fol[,c("subjid","flw_limb_weakness",'per_fat','flw_fatigue','flw_eq5d_ua', 'flw_eq5d_ua_2', 'flw_eq5d_pd_2', 'flw_eq5d_pd',"flw_eq5d_ad","flw_eq5d_ad_2")]
post$flw_fatigue <- as.numeric(post$flw_fatigue)

post <- mutate(post, post_acute = ifelse((per_fat %in% 1 | flw_limb_weakness %in% 1) & (flw_fatigue >=3 | is.na(flw_fatigue)) & (flw_eq5d_ad_2 > 2 | flw_eq5d_pd_2 > 2 )
                                         & ((flw_eq5d_ad_2 > flw_eq5d_ad) |(flw_eq5d_pd_2 > flw_eq5d_pd) |(flw_eq5d_ua_2 > flw_eq5d_ua)), 1,0))

summary(post$post_acute)
table(post$post_acute)
#•	cognition problems 
cog <- mod12_fol[,c("subjid","flw_confusion",'for_cov','flw_remember_today','flw_remember_pre_c19', 
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
cog <- mutate(cog, cog_mild = ifelse((((for_cov %in% 1 | flw_confusion  %in% 1) & (flw_remember_today %in% 2) & (flw_eq5d_ua_2 >=3) & (flw_remember_today > flw_remember_pre_c19)) |
                                        ((for_cov %in% 1 | flw_confusion  %in% 1) & (flw_remember_today %in% 3) & ((flw_eq5d_ua_2 %in% c(2,3)) ) & (flw_remember_today > flw_remember_pre_c19))),1,0))
table(cog$cog_mild)
summary(cog$cog_mild)

#moderate cognitive problem
# o	lay description for moderate dementia: “has memory problems and confusion, feels disoriented, at times hears voices that are not real, and needs help with some daily activities”
#mild
cog <- mutate(cog, cog_moderate = ifelse((((for_cov %in% 1 | flw_confusion  %in% 1) & (flw_remember_today %in% 3) & (flw_eq5d_ua_2 >=4) & (flw_remember_today > flw_remember_pre_c19)) |
                                            ((for_cov %in% 1 | flw_confusion  %in% 1) & (flw_remember_today %in% 4) & (flw_eq5d_ua_2 >=2) & (flw_remember_today > flw_remember_pre_c19))),1,0))
table(cog$cog_moderate)
summary(cog$cog_moderate)

#combined cognitive category(if cog_mild =1 | cog_moderate =1, then cognitive =1 )
cog <- mutate(cog, cognitive = ifelse((cog_mild %in% 1 | cog_moderate %in% 1), 1, 0))
table(cog$cognitive)
summary(cog$cognitive)

# •	respiratory problems 
# o	lay description for mild COPD: “has cough and shortness of breath after heavy physical activity, 
# but is able to walk long distances and climb stairs”
# mild 
res <- mod12_fol[, c("subjid",'flw_breathless_now', 'flw_pers_cough','flw_eq5d_ua_2','flw_eq5d_ua','flw_breathless_pre_c19','flw_fatigue')]
res$na_count <- apply(res, 1, function(x) {sum(is.na(x)) + sum(x == "NR", na.rm=TRUE)})
res <- res[res$na_count <=1,]

res <- mutate(res, res_mild = ifelse((((flw_breathless_now %in% 2 | (flw_pers_cough %in% 1 & flw_breathless_now <=3 )) & flw_eq5d_ua_2 %in% 2) | 
                                        ((flw_breathless_now %in% 3) & (flw_pers_cough %in% 1) & (flw_eq5d_ua_2 %in% 2)) |
                                        (flw_breathless_now %in% 3 & flw_pers_cough %in% 0 & flw_eq5d_ua_2 >= 3)) &
                                       (flw_breathless_now > flw_breathless_pre_c19), 1, 0))
table(res$res_mild)
summary(res$res_mild)
#res_moderate
res <- mutate(res, res_moderate = ifelse(((flw_breathless_now %in% 3 & flw_pers_cough %in% 1 & (flw_eq5d_ua_2 >= 3 | flw_fatigue %in% c(4,5,6))) | 
                                          (flw_breathless_now %in% 4 & flw_eq5d_ua_2 %in% 2 & flw_fatigue <7) |
                                          (flw_breathless_now %in% 5 & (flw_eq5d_ua_2 %in% 2 | flw_fatigue %in% c(4,5,6) ))) &
                                          (flw_breathless_now > flw_breathless_pre_c19), 1, 0))
table(res$res_moderate)
summary(res$res_moderate)

#res_severe 
#o	lay description for severe COPD: “has cough, wheezing and shortness of breath all the time. 
#The person has great difficulty walking even short distances or climbing any stairs, 
#feels tired when at rest, and is anxious”
res <- mutate(res, res_severe = ifelse(((flw_breathless_now %in% 4 & (flw_eq5d_ua_2 >= 3 | flw_fatigue >=7)) | 
                                          (flw_breathless_now %in% 5 & (flw_eq5d_ua_2 >= 3 | flw_fatigue >=7))) & 
                                         (flw_breathless_now > flw_breathless_pre_c19), 1, 0))
table(res$res_severe)
summary(res$res_severe)
#combined to the respiratory category 
res <- mutate(res, res_combine = ifelse((res_mild %in% 1 | res_moderate %in% 1 | res_severe %in% 1), 1, 0))

#drop the res severity because not going to use it, the phd stress and anxiety are missing
a1 <- merge(age_mod12, post[,c('subjid','post_acute')])
a2 <- merge(a1, res[, c('subjid',"res_combine")])
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
#a3$overlap <- rowSums(a3[,c("res_mild","res_moderate","res_severe")])
#Handling the cases that the patients falling in more than one(mild, moderate, severe) category
#they always move to the more severe state

DT = as.data.table(a3)
#DT[(overlap >1 & res_mild==res_moderate), res_mild := 0]
#DT[(overlap >1 & res_moderate== res_severe), res_moderate := 0]
DT$follow_up <- "12_months"
write.csv(DT, paste0("FILEPATH", version, ".csv"))

#merge with age 
inter <- merge(ages, DT)[,!c('subjid','Age')]

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
cocurr_mu$follow_up <- "12_months"

write.csv(cocurr_mu, paste0("FILEPATH", version, ".csv"))

#table by sex only 
cocurr_sex <-data_long %>% 
  group_by(Sex,measure) %>%
  filter(value=='1') %>%
  summarise(var=n()) %>%
  spread(measure, var) 


cocurr_sex <- merge(x=cocurr_sex, y=sample_n_bysex, all.y=TRUE)
cocurr_sex[is.na(cocurr_sex)] <- 0
cocurr_sex$follow_up <- "12_months"

write.csv(cocurr_sex, paste0("FILEPATH", version, ".csv"))


