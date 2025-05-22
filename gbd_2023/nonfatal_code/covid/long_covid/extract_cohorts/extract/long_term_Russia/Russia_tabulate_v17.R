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

# remove the history and call the GBD functions
rm(list=ls(all.names=T))

library(readxl)
library(data.table)
library(dplyr)
## Environment Prep ---------------------------------------------------- ----

# rm(list=ls(all.names=T))

version <- 18

# 17 do not count 12 mo ongoing long COVID cases if they did not have the given symptom cluster at their 6 month follow-up
# 16 fix eq5d pd variable names

if (!exists(".repo_base")) {
  .repo_base <- 'FILEPATH'
}

source(paste0(.repo_base, 'FILEPATH/utils.R'))
source(paste0(roots$k ,"FILEPATH/get_location_metadata.R"))
source(paste0(roots$k ,"FILEPATH/get_age_metadata.R"))

## --------------------------------------------------------------------- ----

tabulation <- function(path1, path2, wave_number, outpath, followup_time, typeData){

  if(wave_number=='wave_1'){
    mod1 <-
      read_excel(path1)[,c("PIN","Age","Sex","COVID PCR")]
    mod1 <- mod1[mod1$`COVID PCR` %in% 1,]
  } else{
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

  followup <- read_excel(path2)[,c("subjid", "flw_confusion",'per_fat','flw_breathless',
                                   "flw_eq5d_ad","flw_eq5d_ad_2",'flw_eq5d_ua', 'flw_eq5d_ua_2', 'flw_eq5d_pd_2', 'flw_eq5d_pd',
                                   'flw_breathless_now','flw_breathless_pre_c19','flw_limb_weakness','flw_fatigue','for_cov',
                                   'flw_remember_today','flw_remember_pre_c19','flw_pers_cough','phq_4_stress','phq_4_worries',
                                   'mus_pain','flw_joint_pain','rig_mus','flw_chest_pains','flw_pain_breathing','anx_cov','depr_mood',
                                   'flw_eq5d5l_vas','flw_eq5d5l_vas_2')]

  # Rename ID column
  names(age_mod12)[names(age_mod12) == "PIN"] <- "subjid"

  # for some reason, in the raw followup data, flw_eq5d_pd contains the responses at follow-up, and flw_eq5d_pd_2 contains the
  # responses pre-COVID.  Switching them in the code now so the algorithm is logical later
  followup$flw_eq5d_pd_2_orig <- followup$flw_eq5d_pd_2
  followup$flw_eq5d_pd_orig <- followup$flw_eq5d_pd
  followup$flw_eq5d_pd_2 <- followup$flw_eq5d_pd_orig
  followup$flw_eq5d_pd <- followup$flw_eq5d_pd_2_orig

  # same situation occurs for flw_eq5d5l_vas and flw_eq5d5l_vas_2
  followup$flw_eq5d5l_vas_2_orig <- followup$flw_eq5d5l_vas_2
  followup$flw_eq5d5l_vas_orig <- followup$flw_eq5d5l_vas
  followup$flw_eq5d5l_vas_2 <- followup$flw_eq5d5l_vas_orig
  followup$flw_eq5d5l_vas <- followup$flw_eq5d5l_vas_2_orig

  # Delete extra cols
  followup$flw_eq5d_pd_orig <- NULL
  followup$flw_eq5d_pd_2_orig <- NULL
  followup$flw_eq5d5l_vas_orig <- NULL
  followup$flw_eq5d5l_vas_2_orig <- NULL

  # Merge age group w/ followup data
  mod12_fol <- merge(age_mod12, followup)

  # Add a new column that checks if at least one pair ua/vas has non-null values
  mod12_fol$eq5d_pair <- apply(mod12_fol, 1, function(x) {
    valid_ua = !is.na(x['flw_eq5d_ua']) | !is.na(x['flw_eq5d_ua_2']) # Check if at least one in the ua pair is not NA
    valid_vas = !is.na(x['flw_eq5d5l_vas']) | !is.na(x['flw_eq5d5l_vas_2']) # Check if at least one in the vas pair is not NA
    return(valid_ua==T | valid_vas==T) # Return TRUE if either pair is valid
  })

  # Getting the number of patients by age and by sex
  sample_n <- mod12_fol %>%
    group_by(age_group_id, age_group_name, Sex) %>%
    dplyr::summarise(N=n())

  # Getting the number of patients by sex
  sample_n_bysex <- mod12_fol %>%
    group_by(Sex) %>%
    dplyr::summarise(N=n())

  # Post-acute consequences of infectious disease
  # •	Post-acute consequences of infectious disease (fatigue, emotional stability, insomnia)
  # lay description: “is always tired and easily upset. The person feels pain all over the body and is depressed”
  post <- mod12_fol[,c("subjid",'flw_limb_weakness','per_fat','flw_fatigue','flw_eq5d_ua',
                       'flw_eq5d_ua_2', 'flw_eq5d_pd_2', 'flw_eq5d_pd',"flw_eq5d_ad","flw_eq5d_ad_2")]

  # "Allow one of the defining items (per_fat, flw_limb_weakness, flw_fatigue, flw_eq5d_ad_2, and flw_eq5d_pd_2) to have missing value"
  #post$na_count <- apply(post, 1, function(x) {sum(is.na(x)) + sum(x == "NR", na.rm=TRUE)})
  #post <- post[post$na_count <=1,]

  post <- mutate(post, post_acute = ifelse((per_fat %in% 1 | flw_limb_weakness %in% 1) &
                                           (flw_fatigue >=3 | is.na(flw_fatigue)) &
                                           (flw_eq5d_ad_2 > 2 | flw_eq5d_pd_2 > 2 ) &
                                           ((flw_eq5d_ad_2 > flw_eq5d_ad) | (flw_eq5d_pd_2 > flw_eq5d_pd) | (flw_eq5d_ua_2 > flw_eq5d_ua)),
                                           1,0))

  message(paste("Average post-acute fatigue: "), mean(post$post_acute, na.rm=T))

  # •	Cognition problems
  cog <- mod12_fol[,c("subjid","flw_confusion",'for_cov','flw_remember_today','flw_remember_pre_c19','flw_eq5d_ua_2')]

  # lay description for mild dementia: “has some trouble remembering recent events,
  # and finds it hard to concentrate and make decisions and plans”

  # "Allow one of the defining items (flw_confusion, for_cov, flw_remember_today, flw_remember_pre_c19, flw_eq5d_ua_2) to have missing value"
  cog$na_count <- apply(cog, 1, function(x) {sum(is.na(x)) + sum(x == "NR", na.rm=TRUE)})
  cog <- cog[cog$na_count <=1,]

  # Mild cognitive problem
  cog <- mutate(cog, cog_mild = ifelse((((for_cov %in% 1 | flw_confusion %in% 1) &
                                         (flw_remember_today %in% 2) &
                                         (flw_eq5d_ua_2 >=3) &
                                         (flw_remember_today > flw_remember_pre_c19)) |
                                        ((for_cov %in% 1 | flw_confusion %in% 1) &
                                         (flw_remember_today %in% 3) &
                                        ((flw_eq5d_ua_2 %in% c(2,3))) &
                                         (flw_remember_today > flw_remember_pre_c19))),
                                       1,0))
  # Moderate cognitive problem
  cog <- mutate(cog, cog_moderate = ifelse((((for_cov %in% 1 | flw_confusion %in% 1) &
                                             (flw_remember_today %in% 3) &
                                             (flw_eq5d_ua_2 >=4) &
                                             (flw_remember_today > flw_remember_pre_c19)) |
                                            ((for_cov %in% 1 | flw_confusion %in% 1) &
                                             (flw_remember_today %in% 4) &
                                             (flw_eq5d_ua_2 >=2) &
                                             (flw_remember_today > flw_remember_pre_c19))),
                                           1,0))

  # Combined cognitive category(if cog_mild =1 | cog_moderate =1, then cognitive =1 )
  cog <- mutate(cog, cognitive = ifelse((cog_mild %in% 1 | cog_moderate %in% 1), 1, 0))
  message(paste("Average cognitive: "), mean(cog$cognitive))

  # •	Respiratory problems
  # lay description for mild COPD: “has cough and shortness of breath after heavy physical activity,
  # but is able to walk long distances and climb stairs”
  # res_mild
  res <- mod12_fol[, c("subjid",'flw_breathless_now', 'flw_pers_cough','flw_eq5d_ua_2','flw_eq5d_ua',
                       'flw_breathless_pre_c19','flw_fatigue','phq_4_stress', 'phq_4_worries')]

  res$na_count <- apply(res, 1, function(x) {sum(is.na(x)) + sum(x == "NR", na.rm=TRUE)})
  res <- res[res$na_count <=1,]

  res <- mutate(res, res_mild = ifelse((((flw_breathless_now %in% 2 | (flw_pers_cough %in% 1 & flw_breathless_now <=3 )) &
                                          flw_eq5d_ua_2 %in% 2) |
                                        ((flw_breathless_now %in% 3) &
                                         (flw_pers_cough %in% 1) &
                                         (flw_eq5d_ua_2 %in% 2)) |
                                         (flw_breathless_now %in% 3 &
                                          flw_pers_cough %in% 0 & flw_eq5d_ua_2 >= 3)) &
                                         (flw_breathless_now > flw_breathless_pre_c19),
                                       1,0))

  # res_moderate
  res <- mutate(res, res_moderate = ifelse(((flw_breathless_now %in% 3 & flw_pers_cough %in% 1 &
                                            (flw_eq5d_ua_2 >= 3 | flw_fatigue %in% c(4,5,6))) |
                                            (flw_breathless_now %in% 4 & flw_eq5d_ua_2 %in% 2 & flw_fatigue <7 &
                                            (phq_4_stress <3 & phq_4_worries <3)) |
                                            (flw_breathless_now %in% 5 & (flw_eq5d_ua_2 %in% 2 | flw_fatigue %in% c(4,5,6)))) &
                                            (flw_breathless_now > flw_breathless_pre_c19),
                                           1,0))

  # res_severe
  # lay description for severe COPD: “has cough, wheezing and shortness of breath all the time.
  # The person has great difficulty walking even short distances or climbing any stairs,
  # feels tired when at rest, and is anxious”
  res <-  mutate(res, res_severe = ifelse(((flw_breathless_now %in% 4 &
                                           (flw_eq5d_ua_2 >= 3 | (flw_fatigue >=7 & !is.na(flw_fatigue)) |
                                            phq_4_stress %in% c(3,4) |
                                            phq_4_worries %in% c(3,4))) |
                                           (flw_breathless_now %in% 5 &
                                           (flw_eq5d_ua_2 >= 3 | (flw_fatigue >=7 & !is.na(flw_fatigue))))) &
                                           (flw_breathless_now > flw_breathless_pre_c19),
                                          1,0))

  # Combined to the respiratory category
  res <- mutate(res, res_combine = ifelse((res_mild %in% 1 | res_moderate %in% 1 | res_severe %in% 1), 1, 0))
  message(paste("Average res: "), mean(res$res_combine))

  # Fatigue
  fatg <- mod12_fol[,c('subjid','flw_fatigue','per_fat','eq5d_pair',
                       'flw_eq5d_ua','flw_eq5d_ua_2','flw_eq5d5l_vas','flw_eq5d5l_vas_2')]
  fatg$na_count <- apply(fatg[,c('flw_fatigue','per_fat')], 1, function(x) {sum(is.na(x)) + sum(x == "NR", na.rm=TRUE)})
  fatg <- fatg[fatg$na_count <=1 & fatg$eq5d_pair==T,]

  fatg <- mutate(fatg, fatigue = ifelse((flw_fatigue >=3 | per_fat %in% 1) &
                                          ((flw_eq5d_ua_2 > flw_eq5d_ua) | (flw_eq5d5l_vas_2 < flw_eq5d5l_vas)),
                                        1,0))
  message(paste("Average fatigue: "), mean(fatg$fatigue, na.rm=T))

  # Joint/muscle pain/tightness
  jm_pain <- mod12_fol[,c('subjid','mus_pain','flw_joint_pain','rig_mus','eq5d_pair',
                          'flw_eq5d_ua','flw_eq5d_ua_2','flw_eq5d5l_vas','flw_eq5d5l_vas_2')]

  jm_pain$na_count <- apply(jm_pain[,c('mus_pain','flw_joint_pain','rig_mus')], 1, function(x) {sum(is.na(x)) + sum(x == "NR", na.rm=TRUE)})
  jm_pain <- jm_pain[jm_pain$na_count <=1 & jm_pain$eq5d_pair==T,]

  jm_pain <- mutate(jm_pain, joint_mus_pain = ifelse((mus_pain %in% 1 | flw_joint_pain %in% 1 | rig_mus %in% 1) &
                                                       ((flw_eq5d_ua_2 > flw_eq5d_ua) | (flw_eq5d5l_vas_2 < flw_eq5d5l_vas)),
                                                     1,0))
  message(paste("Average joint/muscle pain/tightness: "), mean(jm_pain$joint_mus_pain, na.rm=T))

  # Breathless
  breath <- mod12_fol[,c('subjid','flw_breathless','flw_breathless_now','flw_breathless_pre_c19','eq5d_pair',
                         'flw_eq5d_ua','flw_eq5d_ua_2','flw_eq5d5l_vas','flw_eq5d5l_vas_2')]
  breath$na_count <- apply(breath[,c('flw_breathless','flw_breathless_now','flw_breathless_pre_c19')], 1,
                           function(x) {sum(is.na(x)) + sum(x == "NR", na.rm=TRUE)})
  breath <- breath[breath$na_count <=1 & breath$eq5d_pair==T,]

  breath <- mutate(breath, breathless = ifelse((flw_breathless %in% 1 & (flw_breathless_now > flw_breathless_pre_c19)) &
                                                 ((flw_eq5d_ua_2 > flw_eq5d_ua) | (flw_eq5d5l_vas_2 < flw_eq5d5l_vas)),
                                               1,0))
  message(paste("Average breathless: "), mean(breath$breathless, na.rm=T))

  # Cough, chest pain, pain breathing
  cough_chest_br <- mod12_fol[,c('subjid','flw_pers_cough','flw_chest_pains','flw_pain_breathing','eq5d_pair',
                                 'flw_eq5d_ua','flw_eq5d_ua_2','flw_eq5d5l_vas','flw_eq5d5l_vas_2')]
  cough_chest_br$na_count <- apply(cough_chest_br[,c('flw_pers_cough','flw_chest_pains','flw_pain_breathing')], 1,
                           function(x) {sum(is.na(x)) + sum(x == "NR", na.rm=TRUE)})
  cough_chest_br <- cough_chest_br[cough_chest_br$na_count <=1 & cough_chest_br$eq5d_pair==T,]

  cough_chest_br <- mutate(cough_chest_br, cough_chest_breath_pain =
                             ifelse((flw_pers_cough %in% 1 | flw_chest_pains %in% 1 | flw_pain_breathing %in% 1) &
                                      ((flw_eq5d_ua_2 > flw_eq5d_ua) | (flw_eq5d5l_vas_2 < flw_eq5d5l_vas)),
                                    1,0))
  message(paste("Average cough, chest pain, pain breathing: "), mean(cough_chest_br$cough_chest_breath_pain, na.rm=T))

  # Forgetfulness/lack of concentration/confusion
  forg_conf <- mod12_fol[,c('subjid','for_cov','flw_confusion','flw_remember_today','flw_remember_pre_c19','eq5d_pair',
                            'flw_eq5d_ua','flw_eq5d_ua_2','flw_eq5d5l_vas','flw_eq5d5l_vas_2')]
  forg_conf$na_count <- apply(forg_conf[,c('for_cov','flw_confusion','flw_remember_today','flw_remember_pre_c19')], 1,
                                   function(x) {sum(is.na(x)) + sum(x == "NR", na.rm=TRUE)})
  forg_conf <- forg_conf[forg_conf$na_count <=1 & forg_conf$eq5d_pair==T,]

  forg_conf <- mutate(forg_conf, forget_confused = ifelse((for_cov %in% 1 | flw_confusion %in% 1 | flw_remember_today>flw_remember_pre_c19) &
                                                            ((flw_eq5d_ua_2 > flw_eq5d_ua) | (flw_eq5d5l_vas_2 < flw_eq5d5l_vas)),
                                                          1,0))
  message(paste("Average forgetfulness/lack of concentration/confusion: "), mean(forg_conf$forget_confused, na.rm=T))

  # Anxiety/depression
  anx_depres <- mod12_fol[,c('subjid','anx_cov','depr_mood',
                             'flw_eq5d_ad','flw_eq5d_ad_2','flw_eq5d5l_vas','flw_eq5d5l_vas_2')]
  anx_depres$na_count <- apply(anx_depres[,c('anx_cov','depr_mood','flw_eq5d_ad','flw_eq5d_ad_2')], 1,
                               function(x) {sum(is.na(x)) + sum(x == "NR", na.rm=TRUE)})
  anx_depres$na_count2 <- apply(anx_depres[,c('flw_eq5d5l_vas','flw_eq5d5l_vas_2')], 1,
                                function(x) {sum(is.na(x)) + sum(x == "NR", na.rm=TRUE)})
  anx_depres <- anx_depres[anx_depres$na_count <=1 & anx_depres$na_count2 ==0,]

  anx_depres <- mutate(anx_depres, anxiety_depressed = ifelse((flw_eq5d_ad_2 > flw_eq5d_ad | anx_cov %in% 1 | depr_mood %in% 1) &
                                                                (flw_eq5d5l_vas_2 < flw_eq5d5l_vas),
                                                              1,0))
  message(paste("Average anxiety/depression: "), mean(anx_depres$anxiety_depressed, na.rm=T))

  # Merge all clusters
  a1 <- merge(age_mod12, post[,c('subjid','post_acute')])
  a2 <- merge(a1, res[, c('subjid', 'res_mild',"res_moderate","res_severe", "res_combine")])
  a3 <- merge(a2, cog[, c('subjid','cog_mild','cog_moderate','cognitive')])

  a3 <- merge(a3, fatg[, c('subjid','fatigue')], all.x=T)
  a3 <- merge(a3, jm_pain[, c('subjid','joint_mus_pain')], all.x=T)
  a3 <- merge(a3, breath[, c('subjid','breathless')], all.x=T)
  a3 <- merge(a3, cough_chest_br[, c('subjid','cough_chest_breath_pain')], all.x=T)
  a3 <- merge(a3, forg_conf[, c('subjid','forget_confused')], all.x=T)
  a3 <- merge(a3, anx_depres[, c('subjid','anxiety_depressed')], all.x=T)

  # Combine the resp, cognitive and fatigue categories
  a3 <- mutate(a3, Cog_Res = ifelse((cognitive %in% 1 & res_combine %in% 1), 1, 0 ))
  a3 <- mutate(a3, Cog_Fat = ifelse((cognitive %in% 1 & post_acute %in% 1), 1, 0 ))
  a3 <- mutate(a3, Res_Fat = ifelse((res_combine %in% 1 & post_acute %in% 1), 1, 0 ))
  a3 <- mutate(a3, Cog_Res_Fat = ifelse((res_combine %in% 1 & post_acute %in% 1
                                         & cognitive %in% 1), 1, 0 ))
  # All long term category
  a3 <- mutate(a3, long_term = ifelse((cognitive %in% 1 | res_combine %in% 1 | post_acute %in% 1), 1, 0 ))
  message(paste("Average long-term: "), mean(a3$long_term))

  # Long term + new clusters
  a3 <- mutate(a3, long_term_extended = ifelse(long_term %in% 1 | fatigue %in% 1 | joint_mus_pain %in% 1 |
                                               breathless %in% 1 | cough_chest_breath_pain %in% 1 |
                                               forget_confused %in% 1 | anxiety_depressed %in% 1, 1, 0))
  message(paste("Average long-term-extended: "), mean(a3$long_term_extended))

  a3$overlap <- rowSums(a3[,c("res_mild","res_moderate","res_severe")])

  DT = as.data.table(a3)
  DT[(overlap >1 & res_mild == res_moderate), res_mild := 0]
  DT[(overlap >1 & res_moderate == res_severe), res_moderate := 0]
  DT$follow_up <- followup_time
  DT[is.na(DT)] <- 0

  write.csv(DT, paste0(outpath ,wave_number, "_",followup_time,"_",typeData,"_all_data_marked_v_", version, ".csv"))

  inter <- merge(ages, DT)[,!c('subjid','Age','overlap')]

  # Tabulate
  # cols <- c("age_group_id","age_group_name", "sex","icu_hoterm","COVID_PCR")
  data_long <- gather(inter , measure, value, c(post_acute:long_term_extended) )

  # Table by sex and age
  cocurr_mu <-data_long %>%
    group_by(age_group_id,age_group_name, Sex, measure) %>%
    filter(value=='1') %>%
    dplyr::summarise(var=n()) %>%
    spread(measure, var)

  cocurr_mu <- merge(x=cocurr_mu, y=sample_n, all.y=TRUE)
  cocurr_mu[is.na(cocurr_mu)] <- 0
  cocurr_mu$follow_up <- followup_time
  cocurr_mu <- cocurr_mu %>% arrange(age_group_id, Sex)

  write.csv(cocurr_mu, paste0(outpath ,wave_number,"_",followup_time, "_",typeData,"_byAgeSex_",version, ".csv"))

  # Table by sex only
  cocurr_sex <- data_long %>%
    group_by(Sex,measure) %>%
    filter(value=='1') %>%
    dplyr::summarise(var=n()) %>%
    spread(measure, var)

  cocurr_sex <- merge(x=cocurr_sex, y=sample_n_bysex, all.y=TRUE)
  cocurr_sex[is.na(cocurr_sex)] <- 0
  cocurr_sex$follow_up <- followup_time

  write.csv(cocurr_sex, paste0(outpath ,wave_number,"_",followup_time,"_",typeData, "_bySex_v_", version, ".csv"))
}


# Follow up time, 6-8 month, adult, wave 1 ---------------------------------------------
path1 <-'FILEPATH/Part-1-PCR-Upd-5060 copy.xlsx'

path2 <- 'FILEPATH/6-8 months Follow-up.xlsx'

wave_number <- "wave_1"
followup_time <-"6_month"

outpath<-"FILEPATH"

tabulation(path1, path2, wave_number, outpath, followup_time, typeData ="_" )

# Follow up time, 12_month, adult, wave 1, initial -------------------------------------

path1 <-'FILEPATH/Part-1-PCR-Upd-5060 copy.xlsx'

path2 <-'FILEPATH/Initial_12 months Follow-up.xlsx'

wave_number <- "wave_1"
followup_time <-"12_month"

outpath<-"FILEPATH"

tabulation(path1, path2, wave_number, outpath, followup_time, typeData ="Initial")

# Follow up time, 6 month, adult, wave 2 -----------------------------------------------
path1 <-'FILEPATH/Part1_2 wave U07.1.xlsx'

path2 <- 'FILEPATH/Adults Follow-up 6-8 months_2 wave.xlsx'

wave_number <- "wave_2"
followup_time <-"6_month"

outpath <-"FILEPATH"

tabulation(path1, path2, wave_number, outpath, followup_time, typeData="_")

##########################################################################


##########################################################################
# ongoing is missing some key variables, like the comparison variables.
# ongoing is not included in the tabulation.
# ongoing 12 months
# wave 1
# the ongoing dataset have many variables with different names

# module 1
# Read in the patient info
mod1 <- read_excel('FILEPATH/Part-1-PCR-Upd-5060 copy.xlsx')[,c("PIN","Age","Sex","COVID PCR")]
# Read in the follow up data
# comor <- read_excel('FILEPATH/Adults Follow-up 6-8 months_2 wave.xlsx')

mod1 <- mod1[mod1$`COVID PCR` %in% 1,]

# Creating age group by GBD
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

# warning messages due to "NR" missing values
followup_12m <- read_excel('FILEPATH/Ongoing_ 12 months Follow_up.xlsx')[,c("subjid",
  "flw_confusion_0006ff",'flw_breathless_c67847', "flw_eq5d_ad_e29260",'flw_eq5d_ua_dd0e43','flw_eq5d_pd_2_eafcd1',
  'flw_breathless_now_6d83c8','flw_limb_weakness_3b26c2','flw_fatigue_e07109','flw2_for_cov',
  'flw_remember_today_3a83ec','flw_pers_cough_bb302b',
  'mus_pain_5bc80c','flw_joint_pain_0bef41','flw2_rig_mus','flw_chest_pains_c6d16d','flw_pain_breathing_6e1716',
  'flw2_anx_cov','flw2_depr_mood','flw_eq5d5l_vas_86a73f')]

# using the 6-8 months as the pre, read in the 6-8 months
followup_6m <- read_excel('FILEPATH/6-8 months Follow-up.xlsx')[,c("subjid","for_cov",'per_fat','flw_eq5d_ua',"flw_eq5d_ad",'flw_breathless_pre_c19',"flw_remember_pre_c19","flw_eq5d_pd","flw_eq5d_pd_2",'flw_eq5d5l_vas','flw_eq5d5l_vas_2')]

# in the dataset for_cov, flw2_for_cov in the 12 month dataset
# getting per fat, the flow_eq5d_ua and flw_eq5d_ad, remember from the 6-8 months followup data
# per fat is totally missing in the 12 month ongoing dataset use per fat from 6-8 months instead, because per_fat is binary.

# Read in symptom cluster assignments at 6 months
outpath <- "FILEPATH"
clusters_6m <- read.csv(paste0(outpath , "wave_1_6_month___all_data_marked_v_", version, ".csv"))

# for some reason, in the raw followup data, flw_eq5d_pd contains the responses at follow-up, and flw_eq5d_pd_2 contains the
# responses pre-COVID. Switching them in the code now so the algorithm is logical later
followup_6m$flw_eq5d_pd_orig <- followup_6m$flw_eq5d_pd
followup_6m$flw_eq5d_pd <- followup_6m$flw_eq5d_pd_2
followup_6m$flw_eq5d_pd_2 <- NULL

# same situation occurs for flw_eq5d5l_vas and flw_eq5d5l_vas_2
followup_6m$flw_eq5d5l_vas_orig <- followup_6m$flw_eq5d5l_vas
followup_6m$flw_eq5d5l_vas <- followup_6m$flw_eq5d5l_vas_2
followup_6m$flw_eq5d5l_vas_2 <- NULL

# Merge pre & post data
follow_up <- merge(followup_12m, followup_6m)

names(age_mod12)[names(age_mod12) == "PIN"] <- "subjid"
mod12_fol <- merge(age_mod12, follow_up)

# Add a new column that checks if at least one pair ua/vas has non-null values
mod12_fol$eq5d_pair <- apply(mod12_fol, 1, function(x) {
  valid_ua = !is.na(x['flw_eq5d_ua']) | !is.na(x['flw_eq5d_ua_2']) # Check if at least one in the ua pair is not NA
  valid_vas = !is.na(x['flw_eq5d5l_vas']) | !is.na(x['flw_eq5d5l_vas_2']) # Check if at least one in the vas pair is not NA
  return(valid_ua==T | valid_vas==T) # Return TRUE if either pair is valid
})

# Getting the number of patients by age and by sex
sample_n <-mod12_fol %>%
  group_by(age_group_id,age_group_name,Sex ) %>%
  dplyr::summarise(N=n())

# Getting the number of patients by sex
sample_n_bysex <-mod12_fol %>%
  group_by(Sex) %>%
  dplyr::summarise(N=n())

# post-acute consequences of infectious disease
# Post-acute consequences of infectious disease (fatigue, emotional lability, insomnia)
# lay description: “is always tired and easily upset. The person feels pain all over the body and is depressed”
names(mod12_fol)[names(mod12_fol) == "flw_limb_weakness_3b26c2"] <- "flw_limb_weakness"
names(mod12_fol)[names(mod12_fol) == "flw_fatigue_e07109"] <- "flw_fatigue"
names(mod12_fol)[names(mod12_fol) == "flw_eq5d_ua_dd0e43"] <- "flw_eq5d_ua_2"
names(mod12_fol)[names(mod12_fol) == "flw_confusion_0006ff"] <- "flw_confusion"
names(mod12_fol)[names(mod12_fol) == "flw_eq5d_ad_e29260"] <- "flw_eq5d_ad_2"
names(mod12_fol)[names(mod12_fol) == "flw_breathless_now_6d83c8"] <- "flw_breathless_now"
names(mod12_fol)[names(mod12_fol) == "flw_breathless_c67847"] <- "flw_breathless" # "flw_breathless_pre_c19"
names(mod12_fol)[names(mod12_fol) == "flw2_for_cov"] <- "for_cov"
names(mod12_fol)[names(mod12_fol) == "flw_pers_cough_bb302b"] <- "flw_pers_cough"
names(mod12_fol)[names(mod12_fol) == "flw_remember_today_3a83ec"] <- "flw_remember_today"
names(mod12_fol)[names(mod12_fol) == "flw_eq5d_pd_2_eafcd1"] <- "flw_eq5d_pd_2"
names(mod12_fol)[names(mod12_fol) == "mus_pain_5bc80c"] <- "mus_pain"
names(mod12_fol)[names(mod12_fol) == "flw_joint_pain_0bef41"] <- "flw_joint_pain"
names(mod12_fol)[names(mod12_fol) == "flw_chest_pains_c6d16d"] <- "flw_chest_pains"
names(mod12_fol)[names(mod12_fol) == "flw_pain_breathing_6e1716"] <- "flw_pain_breathing"
names(mod12_fol)[names(mod12_fol) == "flw2_anx_cov"] <- "anx_cov"
names(mod12_fol)[names(mod12_fol) == "flw2_depr_mood"] <- "depr_mood"
names(mod12_fol)[names(mod12_fol) == "flw2_rig_mus"] <- "rig_mus"
names(mod12_fol)[names(mod12_fol) == "flw_eq5d5l_vas_86a73f"] <- "flw_eq5d5l_vas_2"

# Post-acute consequences of infectious disease (fatigue, emotional stability, insomnia)
post <- mod12_fol[,c("subjid","flw_limb_weakness",'per_fat','flw_fatigue','flw_eq5d_ua', 'flw_eq5d_ua_2', 'flw_eq5d_pd_2', 'flw_eq5d_pd',"flw_eq5d_ad","flw_eq5d_ad_2")]

post <- mutate(post, post_acute = ifelse((per_fat %in% 1 | flw_limb_weakness %in% 1) &
                                           (flw_fatigue >=3 | is.na(flw_fatigue)) &
                                           (flw_eq5d_ad_2 > 2 | flw_eq5d_pd_2 > 2 ) &
                                           ((flw_eq5d_ad_2 > flw_eq5d_ad) | (flw_eq5d_pd_2 > flw_eq5d_pd) |(flw_eq5d_ua_2 > flw_eq5d_ua)),
                                         1,0))

summary(post$post_acute)
table(post$post_acute)

# Cognition problems
cog <- mod12_fol[,c("subjid","flw_confusion",'for_cov','flw_remember_today','flw_remember_pre_c19',
                    'flw_limb_weakness','per_fat','flw_fatigue','flw_eq5d_ua_2', 'flw_eq5d_pd_2')]

# lay description for mild dementia: “has some trouble remembering recent events,
# and finds it hard to concentrate and make decisions and plans”

# removing
# "Allow one of the defining items ("flw_confusion",'for_cov','flw_remember_today','flw_remember_pre_c19','flw_limb_weakness','per_fat',
# 'flw_fatigue','flw_eq5d_ua_2', 'flw_eq5d_pd_2') to have missing value"
cog$na_count <- apply(cog, 1, function(x) {sum(is.na(x)) + sum(x == "NR", na.rm=TRUE)})
cog <- cog[cog$na_count <=1,]

# lay description for mild dementia: “has some trouble remembering recent events,
# and finds it hard to concentrate and make decisions and plans”

# mild cognitive problem
cog <- mutate(cog, cog_mild = ifelse((((for_cov %in% 1 | flw_confusion %in% 1) &
                                       (flw_remember_today %in% 2) &
                                       (flw_eq5d_ua_2 >=3) &
                                       (flw_remember_today > flw_remember_pre_c19)) |
                                      ((for_cov %in% 1 | flw_confusion %in% 1) &
                                       (flw_remember_today %in% 3) &
                                      ((flw_eq5d_ua_2 %in% c(2,3))) &
                                       (flw_remember_today > flw_remember_pre_c19))),
                                     1,0))
table(cog$cog_mild)
summary(cog$cog_mild)

# moderate cognitive problem
# lay description for moderate dementia: “has memory problems and confusion, feels disoriented, at times hears voices that are not real, and needs help with some daily activities”
cog <- mutate(cog, cog_moderate = ifelse((((for_cov %in% 1 | flw_confusion %in% 1) &
                                           (flw_remember_today %in% 3) &
                                           (flw_eq5d_ua_2 >=4) &
                                           (flw_remember_today > flw_remember_pre_c19)) |
                                          ((for_cov %in% 1 | flw_confusion %in% 1) &
                                           (flw_remember_today %in% 4) &
                                           (flw_eq5d_ua_2 >=2) &
                                           (flw_remember_today > flw_remember_pre_c19))),
                                         1,0))
table(cog$cog_moderate)
summary(cog$cog_moderate)

# combined cognitive category(if cog_mild =1 | cog_moderate =1, then cognitive =1 )
cog <- mutate(cog, cognitive = ifelse((cog_mild %in% 1 | cog_moderate %in% 1), 1, 0))
table(cog$cognitive)
summary(cog$cognitive)

# respiratory problems
# lay description for mild COPD: “has cough and shortness of breath after heavy physical activity,
# but is able to walk long distances and climb stairs”
# res_mild
res <- mod12_fol[, c("subjid",'flw_breathless_now', 'flw_pers_cough','flw_eq5d_ua_2','flw_eq5d_ua','flw_breathless_pre_c19','flw_fatigue')]
res$na_count <- apply(res, 1, function(x) {sum(is.na(x)) + sum(x == "NR", na.rm=TRUE)})
res <- res[res$na_count <=1,]

res <- mutate(res, res_mild = ifelse((((flw_breathless_now %in% 2 | (flw_pers_cough %in% 1 & flw_breathless_now <=3 )) &
                                        flw_eq5d_ua_2 %in% 2) |
                                      ((flw_breathless_now %in% 3) & (flw_pers_cough %in% 1) & (flw_eq5d_ua_2 %in% 2)) |
                                       (flw_breathless_now %in% 3 & flw_pers_cough %in% 0 & flw_eq5d_ua_2 >= 3)) &
                                       (flw_breathless_now > flw_breathless_pre_c19),
                                     1,0))
table(res$res_mild)
summary(res$res_mild)

# res_moderate
res <- mutate(res, res_moderate = ifelse(((flw_breathless_now %in% 3 & flw_pers_cough %in% 1 &
                                          (flw_eq5d_ua_2 >= 3 | flw_fatigue %in% c(4,5,6))) |
                                          (flw_breathless_now %in% 4 & flw_eq5d_ua_2 %in% 2 & flw_fatigue <7) |
                                          (flw_breathless_now %in% 5 & (flw_eq5d_ua_2 %in% 2 | flw_fatigue %in% c(4,5,6) ))) &
                                          (flw_breathless_now > flw_breathless_pre_c19),
                                         1,0))
table(res$res_moderate)
summary(res$res_moderate)

# res_severe
# lay description for severe COPD: “has cough, wheezing and shortness of breath all the time.
# The person has great difficulty walking even short distances or climbing any stairs,
# feels tired when at rest, and is anxious”
res <- mutate(res, res_severe = ifelse(((flw_breathless_now %in% 4 & (flw_eq5d_ua_2 >= 3 | flw_fatigue >=7)) |
                                        (flw_breathless_now %in% 5 & (flw_eq5d_ua_2 >= 3 | flw_fatigue >=7))) &
                                        (flw_breathless_now > flw_breathless_pre_c19), 1, 0))
table(res$res_severe)
summary(res$res_severe)

# combined to the respiratory category
res <- mutate(res, res_combine = ifelse((res_mild %in% 1 | res_moderate %in% 1 | res_severe %in% 1), 1, 0))
table(res$res_combine)

## NEW CLUSTERS

# Fatigue
fatg <- mod12_fol[,c('subjid','flw_fatigue','per_fat','eq5d_pair',
                     'flw_eq5d_ua','flw_eq5d_ua_2','flw_eq5d5l_vas','flw_eq5d5l_vas_2')]
fatg$na_count <- apply(fatg[,c('flw_fatigue','per_fat')], 1, function(x) {sum(is.na(x)) + sum(x == "NR", na.rm=TRUE)})
fatg <- fatg[fatg$na_count <=1 & fatg$eq5d_pair==T,]

fatg <- mutate(fatg, fatigue = ifelse((flw_fatigue >=3 | per_fat %in% 1) &
                                        ((flw_eq5d_ua_2 > flw_eq5d_ua) | (flw_eq5d5l_vas_2 < flw_eq5d5l_vas)),
                                      1,0))
table(fatg$fatigue)

# Joint/muscle pain/tightness
jm_pain <- mod12_fol[,c('subjid','mus_pain','flw_joint_pain','rig_mus','eq5d_pair',
                        'flw_eq5d_ua','flw_eq5d_ua_2','flw_eq5d5l_vas','flw_eq5d5l_vas_2')]

jm_pain$na_count <- apply(jm_pain[,c('mus_pain','flw_joint_pain','rig_mus')], 1, function(x) {sum(is.na(x)) + sum(x == "NR", na.rm=TRUE)})
jm_pain <- jm_pain[jm_pain$na_count <=1 & jm_pain$eq5d_pair==T,]

jm_pain <- mutate(jm_pain, joint_mus_pain = ifelse((mus_pain %in% 1 | flw_joint_pain %in% 1 | rig_mus %in% 1) &
                                                     ((flw_eq5d_ua_2 > flw_eq5d_ua) | (flw_eq5d5l_vas_2 < flw_eq5d5l_vas)),
                                                   1,0))
table(jm_pain$joint_mus_pain)

# Breathless
breath <- mod12_fol[,c('subjid','flw_breathless','flw_breathless_now','flw_breathless_pre_c19','eq5d_pair',
                       'flw_eq5d_ua','flw_eq5d_ua_2','flw_eq5d5l_vas','flw_eq5d5l_vas_2')]
breath$na_count <- apply(breath[,c('flw_breathless','flw_breathless_now','flw_breathless_pre_c19')], 1,
                         function(x) {sum(is.na(x)) + sum(x == "NR", na.rm=TRUE)})
breath <- breath[breath$na_count <=1 & breath$eq5d_pair==T,]

breath <- mutate(breath, breathless = ifelse((flw_breathless %in% 1 & (flw_breathless_now > flw_breathless_pre_c19)) &
                                               ((flw_eq5d_ua_2 > flw_eq5d_ua) | (flw_eq5d5l_vas_2 < flw_eq5d5l_vas)),
                                             1,0))
table(breath$breathless)

# Cough, chest pain, pain breathing
cough_chest_br <- mod12_fol[,c('subjid','flw_pers_cough','flw_chest_pains','flw_pain_breathing','eq5d_pair',
                               'flw_eq5d_ua','flw_eq5d_ua_2','flw_eq5d5l_vas','flw_eq5d5l_vas_2')]
cough_chest_br$na_count <- apply(cough_chest_br[,c('flw_pers_cough','flw_chest_pains','flw_pain_breathing')], 1,
                                 function(x) {sum(is.na(x)) + sum(x == "NR", na.rm=TRUE)})
cough_chest_br <- cough_chest_br[cough_chest_br$na_count <=1 & cough_chest_br$eq5d_pair==T,]

cough_chest_br <- mutate(cough_chest_br, cough_chest_breath_pain =
                           ifelse((flw_pers_cough %in% 1 | flw_chest_pains %in% 1 | flw_pain_breathing %in% 1) &
                                    ((flw_eq5d_ua_2 > flw_eq5d_ua) | (flw_eq5d5l_vas_2 < flw_eq5d5l_vas)),
                                  1,0))
table(cough_chest_br$cough_chest_breath_pain)

# Forgetfulness/lack of concentration/confusion
forg_conf <- mod12_fol[,c('subjid','for_cov','flw_confusion','flw_remember_today','flw_remember_pre_c19','eq5d_pair',
                          'flw_eq5d_ua','flw_eq5d_ua_2','flw_eq5d5l_vas','flw_eq5d5l_vas_2')]
forg_conf$na_count <- apply(forg_conf[,c('for_cov','flw_confusion','flw_remember_today','flw_remember_pre_c19')], 1,
                            function(x) {sum(is.na(x)) + sum(x == "NR", na.rm=TRUE)})
forg_conf <- forg_conf[forg_conf$na_count <=1 & forg_conf$eq5d_pair==T,]

forg_conf <- mutate(forg_conf, forget_confused = ifelse((for_cov %in% 1 | flw_confusion %in% 1 | flw_remember_today>flw_remember_pre_c19) &
                                                          ((flw_eq5d_ua_2 > flw_eq5d_ua) | (flw_eq5d5l_vas_2 < flw_eq5d5l_vas)),
                                                        1,0))
table(forg_conf$forget_confused)

# Anxiety/depression
anx_depres <- mod12_fol[,c('subjid','anx_cov','depr_mood',
                           'flw_eq5d_ad','flw_eq5d_ad_2','flw_eq5d5l_vas','flw_eq5d5l_vas_2')]
anx_depres$na_count <- apply(anx_depres[,c('anx_cov','depr_mood','flw_eq5d_ad','flw_eq5d_ad_2')], 1,
                             function(x) {sum(is.na(x)) + sum(x == "NR", na.rm=TRUE)})
anx_depres$na_count2 <- apply(anx_depres[,c('flw_eq5d5l_vas','flw_eq5d5l_vas_2')], 1,
                              function(x) {sum(is.na(x)) + sum(x == "NR", na.rm=TRUE)})
anx_depres <- anx_depres[anx_depres$na_count <=1 & anx_depres$na_count2 ==0,]

anx_depres <- mutate(anx_depres, anxiety_depressed = ifelse((flw_eq5d_ad_2 > flw_eq5d_ad | anx_cov %in% 1 | depr_mood %in% 1) &
                                                              (flw_eq5d5l_vas_2 < flw_eq5d5l_vas),
                                                            1,0))
table(anx_depres$anxiety_depressed)


# drop the res severity because not going to use it, the phd stress and anxiety are missing
a1 <- merge(age_mod12, post[,c('subjid','post_acute')])
a2 <- merge(a1, res[, c('subjid',"res_combine")])
a3 <- merge (a2,cog[, c('subjid','cog_mild','cog_moderate','cognitive')])

a3 <- merge(a3, fatg[, c('subjid','fatigue')], all.x=T)
a3 <- merge(a3, jm_pain[, c('subjid','joint_mus_pain')], all.x=T)
a3 <- merge(a3, breath[, c('subjid','breathless')], all.x=T)
a3 <- merge(a3, cough_chest_br[, c('subjid','cough_chest_breath_pain')], all.x=T)
a3 <- merge(a3, forg_conf[, c('subjid','forget_confused')], all.x=T)
a3 <- merge(a3, anx_depres[, c('subjid','anxiety_depressed')], all.x=T)

# combine the resp, cognitive and fatigue categories
a3 <- mutate(a3, Cog_Res = ifelse((cognitive %in% 1 & res_combine %in% 1), 1, 0 ))
a3 <- mutate(a3, Cog_Fat = ifelse((cognitive %in% 1 & post_acute %in% 1), 1, 0 ))
a3 <- mutate(a3, Res_Fat = ifelse((res_combine %in% 1 & post_acute %in% 1), 1, 0 ))
a3 <- mutate(a3, Cog_Res_Fat = ifelse((res_combine %in% 1 & post_acute %in% 1
                                       & cognitive %in% 1), 1, 0 ))
# all long term category
a3 <- mutate(a3, long_term = ifelse((cognitive %in% 1 | res_combine %in% 1 | post_acute %in% 1), 1, 0 ))
table(a3$long_term)

# Long term + new clusters
a3 <- mutate(a3, long_term_extended = ifelse(long_term %in% 1 | fatigue %in% 1 | joint_mus_pain %in% 1 |
                                               breathless %in% 1 | cough_chest_breath_pain %in% 1 |
                                               forget_confused %in% 1 | anxiety_depressed %in% 1, 1, 0))
table(a3$long_term_extended)

# Rename duplicated col names between 6m vs 12m
# remove long covid assignment at 12 months if didn't have the symptom cluster at 6 months
a3_cols <- c(colnames(a3),"overlap","res_mild", "res_moderate", "res_severe")
a3_dup_names <- a3_cols[!a3_cols %in% c("subjid","age_group_name","age_group_id","Age","Sex","COVID PCR")]
setnames(clusters_6m, a3_dup_names, paste0(a3_dup_names,"_6mo"))

# Merge with symptom cluster assignments at 6 months.  this merge also helpfully restricts the data to PCR+ so that we don't have to merge with acute data again
a3 <- data.table(merge(a3, clusters_6m))

names(a3)
table(a3$post_acute, a3$post_acute_6mo)
table(a3$cognitive, a3$cognitive_6mo)
table(a3$res_combine, a3$res_combine_6mo)

a3[post_acute_6mo==0, post_acute := 0]
a3[cognitive_6mo==0, cognitive := 0]
a3[res_combine_6mo==0, res_combine := 0]

# Select names that do not match any of the specified patterns
a3_chosen <- names(a3)[!Reduce(`|`, lapply(c("PCR","_6mo","_mild","_moderate","_severe"), function(pattern) {
  grepl(pattern, names(a3))}))]

# Remove redundant cols, NA values and save output
DT <- as.data.table(a3[, ..a3_chosen])
DT$follow_up <- "12_months"
DT$X <- NULL
DT[is.na(DT)] <- 0
# DT[(overlap >1 & res_mild==res_moderate), res_mild := 0]
# DT[(overlap >1 & res_moderate== res_severe), res_moderate := 0]

outpath<-"FILEPATH"
write.csv(DT, paste0(outpath,"wv1_12m_ongoing_all_data_marked_v_", version, ".csv"))

# merge with age
inter <- merge(ages, DT)[,!c('subjid','Age')]

# tabulate
# cols <- c("age_group_id","age_group_name", "sex","icu_hoterm","COVID_PCR")
data_long <- gather(inter , measure, value, c(post_acute:long_term_extended))

# table by age and sex
cocurr_mu <- data_long %>%
  group_by(age_group_id,age_group_name,Sex, measure) %>%
  filter(value=='1') %>%
  dplyr::summarise(var=n()) %>%
  spread(measure, var)

cocurr_mu <- merge(x=cocurr_mu, y=sample_n, all.y=TRUE)
cocurr_mu[is.na(cocurr_mu)] <- 0
cocurr_mu$follow_up <- "12_months"
cocurr_mu <- cocurr_mu %>% arrange(age_group_id, Sex)

write.csv(cocurr_mu, paste0(outpath,"wv1_12m_ongoing_byAgeSex_v_", version, ".csv"))

# table by sex only
cocurr_sex <- data_long %>%
  group_by(Sex,measure) %>%
  filter(value=='1') %>%
  dplyr::summarise(var=n()) %>%
  spread(measure, var)

cocurr_sex <- merge(x=cocurr_sex, y=sample_n_bysex, all.y=TRUE)
cocurr_sex[is.na(cocurr_sex)] <- 0
cocurr_sex$follow_up <- "12_months"

write.csv(cocurr_sex, paste0(outpath,"wv1_12m_ongoing_bySex_v_", version, ".csv"))
write.csv(cocurr_sex, paste0("FILEPATH", version, ".csv"))

# Read in 6-month data by Sex from wave 1 and 2
outpath<-"FILEPATH"
df_6mo_wave1 <- read.csv(paste0(outpath,"wave_1_6_month___bySex_v_", version, ".csv"))
df_6mo_wave2 <- read.csv(paste0(outpath,"wave_2_6_month___bySex_v_", version, ".csv"))

df <- data.table(rbind(df_6mo_wave1, df_6mo_wave2))
df$X <- NULL

# table by sex only
df <- df[, lapply(.SD, sum, na.rm=T), by=c("Sex", "follow_up"),
         .SDcols=setdiff(names(df), c("Sex", "follow_up"))]
df_both <- copy(df)
df_both$Sex <- NULL
df_both <- df_both[, lapply(.SD, sum, na.rm=T), by=c("follow_up"),
         .SDcols=setdiff(names(df_both), c("follow_up"))]
df_both$Sex <- "Both"
df <- rbind(df, df_both)
str(df)

write.csv(df, paste0("FILEPATH", version, ".csv"))


