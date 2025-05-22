## Docstring ----------------------------------------------------------- ----
## Project: NF COVID
## Script: 
## Description: Austria data tabulated
## Contributors: NAME
## Date 3/01/2021
## --------------------------------------------------------------------- ----

## Environment Prep ---------------------------------------------------- ----
#remove the history and call the GBD functions
rm(list=ls(all.names=T))
setwd("FILEPATH")
version <- 0

if (!exists(".repo_base")) {
  .repo_base <- 'FILEPATH'
}

source(paste0(.repo_base, 'FILEPATH/utils.R'))
source(paste0(roots$k ,"FILEPATH/get_location_metadata.R"))
source(paste0(roots$k ,"FILEPATH/get_age_metadata.R"))

## --------------------------------------------------------------------- ----
#

#module 1
#selecting patients by ICD dx, rather than PCR
data <- read_excel("Data_Innsbruck_GBD TV.xlsx", sheet="Data")[,c("pat_id","sex","age","Item_1","Item_2","Item_7","Item_8",
                                                                  "Item_9b","Item_9c","Item_9e","Item_9f","Item_9g","Item_9i","Fatigue",
                                                                  "hads_a_score","hads_D_score","MOCA_sum")]
data <- data[(!is.na(data$Item_1)& !is.na(data$Item_2)& !is.na(data$MOCA_sum)),]
hist(data$age)
#creating age group by GBD
# ages <- get_age_metadata(age_group_set_id = 19, 
#                          gbd_round_id = roots$gbd_round)[, !c('most_detailed')][,c('age_group_id','age_group_name')]


data <- mutate(data, age_group_name = ifelse(age %in% 15:19, "15 to 19",
                  ifelse(age %in% 20:24, "20 to 24",
                  ifelse(age %in% 25:29, "25 to 29",
                  ifelse(age %in% 30:34, "30 to 34",
                  ifelse(age %in% 35:39, "35 to 39",
                  ifelse(age %in% 40:44, "40 to 44",
                  ifelse(age %in% 45:49, "45 to 49",
                  ifelse(age %in% 50:54, "50 to 54", 
                  ifelse(age %in% 55:59, "55 to 59",
                  ifelse(age %in% 60:64, "60 to 64",
                  ifelse(age %in% 65:69, "65 to 69",
                  ifelse(age %in% 70:74, "70 to 74",
                  ifelse(age %in% 75:79, "75 to 79",
                  ifelse(age %in% 80:84, "80 to 84",
                  ifelse(age %in% 85:89, "85 to 89",
                  ifelse(age %in% 90:94, "90 to 94", "95_plus")))))))))))))))))


#getting the number of patients by age and by sex 
sample_n <-data %>% 
  group_by(age_group_name,sex ) %>%
  summarise(N=n()) 

#getting the number of patients by sex 
sample_n_bysex <-data %>% 
  group_by(sex) %>%
  summarise(N=n()) 

#Post-acute consequences of infectious disease (fatigue, emotional lability, insomnia); 
#lay description: “is always tired and easily upset. The person feels pain all over the body and is depressed”
#rule 1:select those reporting ‘fair’ or ‘poor’ on SF-36 Q1 
#and reporting their health as ‘somewhat or much worse’ than a year ago (SF-36 Q2)
data <- mutate(data, fatigue_r1 = ifelse((Item_1 %in% c(4,5)& Item_2 %in% c(4,5)), 1,0))

#rule 2: 	rule 2: yes on self-report fatigue question or 
#at least one of SF-36 Qs 9e, 9g and 9i <4 and [(SF-36 Q7 or 
#SF-36 Q8 about pain  answered as ‘moderate’, ‘severe’, or ‘very severe’) 
#or (HADS-a > 7 or HADS-d > 7 or SF-36 Qs 9b, 9c 
#or 9f answered as ‘often’, ‘most of the time’ or ‘continuous’)]
data <- mutate(data, fatigue_r2 = ifelse(((Fatigue %in% 1 |(Item_9e >=3 | Item_9g <4 | Item_9i <4)) & 
                                           (Item_7 %in% c(4,5,6) | Item_8 %in% c(3,4,5))|
                                           (hads_a_score>7 | hads_D_score>7 |Item_9b %in% c(1,2,3)|Item_9c %in% c(1,2,3) |Item_9f %in% c(1,2,3))), 1,0))

#rule 1 and rule 2
data <- mutate(data, post_acute = ifelse((fatigue_r1 %in% 1 & fatigue_r2 %in% 1), 1, 0 ))
table(data$post_acute)
summary(data$post_acute)

#cognition problems 
#mild cognitive problem
# o	lay description for mild dementia: “has some trouble remembering recent events, 
#and finds it hard to concentrate and make decisions and plans”
data <- mutate(data, cog_mild = ifelse((Item_1 %in% c(4,5)& Item_2 %in% c(4,5) & (MOCA_sum>18 & MOCA_sum<=25) ), 1,0))
summary(data$cog_mild)
table(data$cog_mild)

#moderate cognitive problem
# o	lay description for moderate dementia: “has memory problems and confusion, 
#feels disoriented, at times hears voices that are not real, and needs help with some daily activities”
data <- mutate(data, cog_moderate = ifelse((Item_1 %in% c(4,5)& Item_2 %in% c(4,5) & (MOCA_sum<=18)), 1,0))
summary(data$cog_moderate)
#nobody had MOCA <=18
table(data$cog_moderate)

#combined cognitive category(if cog_mild =1 | cog_moderate =1, then cognitive =1 )
data <- mutate(data, cog = ifelse((cog_mild %in% 1 | cog_moderate %in% 1), 1, 0))
table(data$cog)
summary(data$cog)

a3<-data
#combine the cognitive and fatigue categories 
a3 <- mutate(a3, Cog_Fat = ifelse((cog %in% 1 & post_acute %in% 1), 1, 0 ))

#all long term category
a3 <- mutate(a3, long_term = ifelse((cog %in% 1 | post_acute %in% 1), 1, 0 ))
table(a3$long_term)
summary(a3$long_term)

write.csv(a3, paste0("Austria_all_data_marked_v", version, ".csv"))

#merge with age 
inter <- a3[,c("age_group_name","sex","post_acute","cog","long_term","Cog_Fat")]

#tabulate 
#cols <- c("age_group_id","age_group_name", "sex","icu_hoterm","COVID_PCR")
data_long <- gather(inter , measure, value, c(post_acute:Cog_Fat) )

cocurr_mu <-data_long %>%
  group_by(age_group_name,sex, measure) %>%
  filter(value=='1') %>%
  summarise(var=n()) %>%
  spread(measure, var)

cocurr_mu <- merge(x=cocurr_mu, y=sample_n, all.y=TRUE)
cocurr_mu[is.na(cocurr_mu)] <- 0

write.csv(cocurr_mu, paste0("Austria_byAgeSex_", version, ".csv"))

#table by sex only 
cocurr_sex <-data_long %>% 
  group_by(sex,measure) %>%
  filter(value=='1') %>%
  summarise(var=n()) %>%
  spread(measure, var) 

cocurr_sex <- merge(x=cocurr_sex, y=sample_n_bysex, all.y=TRUE)
cocurr_sex[is.na(cocurr_sex)] <- 0
write.csv(cocurr_sex, paste0("Austria_bySex_v", version, ".csv"))


