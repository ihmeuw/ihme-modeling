## Docstring ----------------------------------------------------------- ----
## Project: NF COVID
## Script: 
## Description: Clean Italian ISRAIC data
## Contributors: NAME
## Date 2/9/2021
## Time stamp: 2/17/2020, we took out all the unknown sex because they are missing all other information. 
##             2/25/2020, Add followup date into the all_mark_dataset
## --------------------------------------------------------------------- ----

## Environment Prep ---------------------------------------------------- ----
rm(list=ls(all.names=T))

if (!exists(".repo_base")) {
  .repo_base <- 'FILEPATH'
}
version <- 5

source(paste0(.repo_base, 'FILEPATH/utils.R'))
source(paste0(roots$k, 'FILEPATH/get_ids.R'))
source(paste0(roots$k, 'FILEPATH/get_location_metadata.R'))
source(paste0(roots$k, 'FILEPATH/get_population.R'))

setwd("FILEPATH")
## --------------------------------------------------------------------- ----
# library(dplyr, lib.loc = "FILEPATH")
# library(data.table)
# library(readxl)
# library(ggplot2)

#Import adult survey
age_pattern<-setDT(read.csv("Italy_age_pattern_v1.csv")[,c("patient_id","covid19_test","new_dob","age","sex")])

age_pattern[age<= 1, age_group_name := "Early Neonatal to 1"]
age_pattern[age > 1 & age <=4, age_group_name := "1 to 4"]
age_pattern[age>4 & age<=9, age_group_name := "5 to 9" ]
age_pattern[age>9 & age<=14, age_group_name := "10 to 14" ]
#including 19.3 in the 15 to 19 
age_pattern[age>14 & age<=20, age_group_name := "15 to 19" ]

#Import children survey
child <- read_excel('Buonsenso_LongCovid-GBD_formatted_v2.xlsx',
                    sheet="Pediatric")[,c("patient_id","follow_up_days","general_health_rating_today",
                                          "general_health_rating_before_covid19","fully_recovered_from_covid19",
                                          "fatigue_compared_to_before_covid19", "fatigue_past_7days", "emotions_compared_to_before_covid19",
                                          "joint_pain_or_swelling_past_7days","persistent_muscle_pain_past_7days",
                                          "confusion_or_lack_of_concentration_past_7days", "classroom_learning_interference_due_to_difficulties_since_covid19",
                                          "difficulty_breathing _chest_tightness_past_7days","pain_on_breathing_past_7days","chest_pain_past_7days",
                                          "persistent_cough_past_7days")]
ped <- merge(age_pattern, child, by='patient_id')
#apply to pediatric patients only
#two patients are actually >19 at the time of the analysis. Confirmed with NAME, these two patients would be included
#in the cohort. Because at the time of survey, they were still 18. 
ped <- ped[(ped$age<=20)&(!is.na(ped$follow_up_days)), ]
#median 64.5 days
#summary(ped$follow_up_days)

#getting the number of patients by age and by sex 
sample_n <-ped %>% 
  group_by(age_group_name,sex ) %>%
  summarise(N=n()) 

#getting the number of patients by sex 
sample_n_bysex <-ped %>% 
  group_by(sex) %>%
  summarise(N=n()) 

#apply to fully recovered from covid <10
#ped <- ped[ped$fully_recovered_from_covid19<10, ]


#fatigue
ped <- mutate(ped, post_acute = ifelse((fatigue_compared_to_before_covid19 %in% c(4,5) | fatigue_past_7days %in% 'yes') & (fully_recovered_from_covid19<10 )& 
                                         ((general_health_rating_before_covid19 > general_health_rating_today) | (emotions_compared_to_before_covid19>2) | 
                                            joint_pain_or_swelling_past_7days %in% 'yes'| persistent_muscle_pain_past_7days %in% 'yes'), 1,0))

table(ped$post_acute)
summary(ped$post_acute)

#cognitive
ped <- mutate(ped, cog = ifelse((confusion_or_lack_of_concentration_past_7days %in% 'si') & (fully_recovered_from_covid19<10 ) &
                                         (general_health_rating_before_covid19 > general_health_rating_today |classroom_learning_interference_due_to_difficulties_since_covid19>2 ), 1,0))
table(ped$cog)
summary(ped$cog)


#respiratory
ped <- mutate(ped, res = ifelse((`difficulty_breathing _chest_tightness_past_7days` %in% 'yes') & (fully_recovered_from_covid19<10 ) &
                                  (general_health_rating_before_covid19 > general_health_rating_today |pain_on_breathing_past_7days %in% 'yes' |
                                     chest_pain_past_7days %in% 'yes' | persistent_cough_past_7days %in% 'yes' ), 1,0))
table(ped$res)
summary(ped$res)

#long-term
ped <- mutate(ped, long_term = ifelse((cog %in% 1 | res %in% 1 | post_acute %in% 1), 1, 0 ))
table(ped$long_term)
summary(ped$long_term)

a3<-ped

#overlaps
a3 <- mutate(a3, Cog_Res = ifelse((cog %in% 1 & res %in% 1), 1, 0 ))
a3 <- mutate(a3, Cog_Fat = ifelse((cog %in% 1 & post_acute %in% 1), 1, 0 ))
a3 <- mutate(a3, Res_Fat = ifelse((res %in% 1 & post_acute %in% 1), 1, 0 ))
a3 <- mutate(a3, Cog_Res_Fat = ifelse((res %in% 1 & post_acute %in% 1
                                       & cog %in% 1), 1, 0 ))

write.csv(a3, paste0("Italy_pediatric_data_marked_v", version, ".csv"))


inter <- a3[,c("age_group_name","sex","post_acute","cog","res","long_term","Cog_Res","Cog_Fat","Res_Fat","Cog_Res_Fat")]
data_long <- gather(inter , measure, value, c(post_acute:Cog_Res_Fat) )
cocurr_mu <-data_long %>%
  group_by(age_group_name,sex, measure) %>%
  filter(value=='1') %>%
  summarise(var=n()) %>%
  spread(measure, var)

cocurr_mu <- merge(x=cocurr_mu, y=sample_n, all.y=TRUE)
cocurr_mu[is.na(cocurr_mu)] <- 0
cocurr_mu <- cocurr_mu[cocurr_mu$sex!=0, ]
cocurr_mu$follow_up_time <- median(a3$follow_up_days)

write.csv(cocurr_mu, paste0("Italy_ped_byAgeSex_", version, ".csv"))

#table by sex only 
cocurr_sex <-data_long %>% 
  group_by(sex,measure) %>%
  filter(value=='1') %>%
  summarise(var=n()) %>%
  spread(measure, var) 

cocurr_sex <- merge(x=cocurr_sex, y=sample_n_bysex, all.y=TRUE)
cocurr_sex[is.na(cocurr_sex)] <- 0
cocurr_sex <- cocurr_sex[cocurr_sex$sex!=0, ]
cocurr_sex $follow_up_time <- median(a3$follow_up_days)
write.csv(cocurr_sex, paste0("Italy_ped_bySex_v", version, ".csv"))

#create plots
pdf(file= paste0("Italy_pediatric_plot_", version, ".pdf"), height=11, width=8.5)

cocurr_mu <- cocurr_mu[cocurr_mu$sex!= 0, ]
age_order <- c("Early Neonatal to 1", "1 to 4","5 to 9", "10 to 14", "15 to 19" )
#combined categories
Post <- ggplot(data =cocurr_mu) +
  geom_col(mapping = aes(x= factor(age_group_name,level= age_order), y=post_acute)) +
  facet_wrap('sex') +
  labs(x='', y='Post Fatigue Syndrome', fill='Severity') +
  theme(axis.text.x = element_text(hjust=1, angle=45))

Cog <-ggplot(data = cocurr_mu) +
  geom_col(mapping = aes(x= factor(age_group_name,level= age_order), y=cog)) +
  facet_wrap('sex') +
  labs(x='', y='Cognition', fill='Severity') +
  theme(axis.text.x = element_text(hjust=1, angle=45))

Res <- ggplot(data = cocurr_mu) +
  geom_col(mapping = aes(x= factor(age_group_name,level= age_order), y=res)) +
  facet_wrap('sex') +
  labs(x='', y='Respiratory', fill='Severity') +
  theme(axis.text.x = element_text(hjust=1, angle=45))

grid.arrange(Post, Cog, Res, ncol=1)

long <- ggplot(data = cocurr_mu) +
  geom_col(mapping = aes(x= factor(age_group_name,level= age_order), y=long_term)) +
  facet_wrap('sex') +
  labs(x='', y='Long_term_effect', fill='Severity') +
  theme(axis.text.x = element_text(hjust=1, angle=45))

cog_fat <- ggplot(data = cocurr_mu) +
  geom_col(mapping = aes(x= factor(age_group_name,level= age_order), y=Cog_Fat)) +
  facet_wrap('sex') +
  labs(x='', y='Cognition_fatigue', fill='Severity') +
  theme(axis.text.x = element_text(hjust=1, angle=45))

Res_fat <- ggplot(data = cocurr_mu) +
  geom_col(mapping = aes(x= factor(age_group_name,level= age_order), y=Res_Fat)) +
  facet_wrap('sex') +
  labs(x='', y='Respiratory_fatigue', fill='Severity') +
  theme(axis.text.x = element_text(hjust=1, angle=45))

grid.arrange(long, cog_fat, Res_fat, ncol=1)

#stack bar, and three bars 
plot <- cocurr_mu[,c("age_group_name","sex","cog","post_acute","res")]
age_order <- c("Early Neonatal to 1", "1 to 4","5 to 9", "10 to 14", "15 to 19" )
dt_long <- gather(plot, condition, measurement, cog:res)

Stack_bar_all_count <- ggplot(data = dt_long) +
  geom_col(mapping = aes(x=factor(age_group_name,level=age_order), y=measurement, fill=condition)) +
  facet_wrap('sex') +
  theme(axis.text.x = element_text(hjust=1, angle=45))

three_bars_discret <- ggplot(data= dt_long, aes(x=factor(age_group_name,level=age_order), y=measurement, fill= condition))+
  geom_bar(stat="identity", position=position_dodge()) +
  facet_wrap('sex') +
  theme(axis.text.x = element_text(hjust=1, angle=45))

grid.arrange(Stack_bar_all_count, three_bars_discret, ncol=1)

dev.off()

