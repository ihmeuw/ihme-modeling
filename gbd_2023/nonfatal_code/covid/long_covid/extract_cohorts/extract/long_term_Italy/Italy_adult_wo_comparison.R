## Docstring ----------------------------------------------------------- ----
## Project: NF COVID
## Script: 
## Description: Italian ISRAIC Adult data without comparison
## Contributors: NAME
## Date 2/26/2021
## --------------------------------------------------------------------- ----

## Environment Prep ---------------------------------------------------- ----
rm(list=ls(all.names=T))

if (!exists(".repo_base")) {
  .repo_base <- 'FILEPATH'
}
version <- 1

source(paste0(.repo_base, 'FILEPATH/utils.R'))
source(paste0(roots$k, 'FILEPATH/get_ids.R'))
source(paste0(roots$k, 'FILEPATH/get_location_metadata.R'))
source(paste0(roots$k, 'FILEPATH/get_population.R'))

setwd("FILEPATH")

age_pattern<-setDT(read.csv("Italy_age_pattern_v1.csv")[,c("patient_id","covid19_test","new_dob","age","sex")])

age_pattern[age<= 1, age_group_name := "Early Neonatal to 1"]
age_pattern[age > 1 & age <=4, age_group_name := "1 to 4"]
age_pattern[age>4 & age<=9, age_group_name := "5 to 9" ]
age_pattern[age>9 & age<=14, age_group_name := "10 to 14" ]
#including 19.3 in the 15 to 19 
age_pattern[age>14 & age<=20, age_group_name := "15 to 19" ]
age_pattern[age>20 & age<=24, age_group_name := "20 to 24"]
age_pattern[age>24 & age<=29, age_group_name := "25 to 29" ]
age_pattern[age>29 & age<=34, age_group_name := "30 to 34" ]
age_pattern[age>34 & age<=39, age_group_name := "35 to 39" ]
age_pattern[age>39 & age<=44, age_group_name := "40 to 44"]
age_pattern[age>44 & age<=49, age_group_name := "45 to 49" ]
age_pattern[age>49 & age<=54, age_group_name := "50 to 54" ]
age_pattern[age>54 & age<=59, age_group_name := "55 to 59" ]
age_pattern[age>59 & age<=64, age_group_name := "60 to 64"]
age_pattern[age>64 & age<=69, age_group_name := "65 to 69" ]
age_pattern[age>69 & age<=74, age_group_name := "70 to 74" ]
age_pattern[age>74 & age<=79, age_group_name := "75 to 79" ]
age_pattern[age>79 & age<=84, age_group_name := "80 to 84" ]
age_pattern[age>84 & age<=89, age_group_name := "85 to 89" ]
age_pattern[age>89 & age<=94, age_group_name := "90 to 94" ]
age_pattern[age>95, age_group_name := "95_plus" ]

#Import adult survey
adult <- read_excel('Buonsenso_LongCovid-GBD_formatted_v3.xlsx',
                    sheet="Long-COVID")[,c("patient_id","follow_up_days",
                                           "anxiety_today",
                                           "daily_activities_today",
                                           "breathlessness_today",
                                           "muscle_weakness_past_7days",
                                           "cConfusion_past_7days","cough_past_7days","problems_speaking_or_communicating_past_7days")]
dt <- merge(age_pattern[,c('patient_id','age','sex','age_group_name')], adult, by='patient_id')
#apply to pediatric patients only
#two patients are actually >19 at the time of the analysis. Confirmed with NAME, these two patients would be included
#in the cohort. Because at the time of survey, they were still 18. 

#after app
dt <- dt[dt$age>=20 & !is.na(dt$sex) & !is.na(dt$follow_up_days), ]
#getting the number of patients by age and by sex 
sample_n <-dt %>% 
  group_by(age_group_name,sex ) %>%
  summarise(N=n()) 

#getting the number of patients by sex 
sample_n_bysex <-dt %>% 
  group_by(sex) %>%
  summarise(N=n()) 

#for the italian data, we don't have fatigue category because fatigue is not collected in the adult survey
#cogntion
dt <- mutate(dt, cog = ifelse((problems_speaking_or_communicating_past_7days %in% 'si' ) |
                                  (cConfusion_past_7days %in% 'si' ), 1,0))
table(dt$cog)
summary(dt$cog)

#respiratory
dt <- mutate(dt, res = ifelse((breathlessness_today> 1 & cough_past_7days %in% "si" ), 1,0))
table(dt$res)
summary(dt$res)

dt <- mutate(dt, long_term = ifelse((cog %in% 1 | res %in% 1), 1, 0 ))
#without post acute(fatigue category)
table(dt$long_term)
summary(dt$long_term)

dt <- mutate(dt, Cog_Res = ifelse((cog %in% 1 & res %in% 1), 1, 0 ))
table(dt$Cog_Res)

write.csv(dt, paste0("Italy_adult_data_marked_wo_comparison_v", version, ".csv"))

#summarize
inter <- dt[,c("age_group_name","sex","cog","res","long_term","Cog_Res")]
data_long <- gather(inter , measure, value, c(cog:Cog_Res) )
cocurr_mu <-data_long %>%
  group_by(age_group_name,sex, measure) %>%
  filter(value=='1') %>%
  summarise(var=n()) %>%
  spread(measure, var)

cocurr_mu <- merge(x=cocurr_mu, y=sample_n, all.y=TRUE)
cocurr_mu[is.na(cocurr_mu)] <- 0
cocurr_mu <- cocurr_mu[cocurr_mu$sex!=0, ]
cocurr_mu$follow_up_time<- median(dt$follow_up_days)

write.csv(cocurr_mu, paste0("Italy_audult_byAgeSex_wo_comparison_", version, ".csv"))

#table by sex only 
cocurr_sex <-data_long %>% 
  group_by(sex,measure) %>%
  filter(value=='1') %>%
  summarise(var=n()) %>%
  spread(measure, var) 

cocurr_sex <- merge(x=cocurr_sex, y=sample_n_bysex, all.y=TRUE)
cocurr_sex[is.na(cocurr_sex)] <- 0
cocurr_sex <- cocurr_sex[cocurr_sex$sex!=0, ]
cocurr_sex $follow_up_time<- median(dt$follow_up_days)

write.csv(cocurr_sex, paste0("Italy_adult_bySex_wo_comparison_v", version, ".csv"))

#create plots
pdf(file= paste0("Italy_adult_plot_wo_comparison_", version, ".pdf"), height=11, width=8.5)

age_order <- c("Early Neonatal to 1", "1 to 4","5 to 9", "10 to 14", "15 to 19","20 to 24","25 to 29", "30 to 34","35 to 39",
               "40 to 44","45 to 49","50 to 54","55 to 59","60 to 64","65 to 69","70 to 74","75 to 79","80 to 84","85 to 89",
               "90 to 94","95_plus")

Cog <- ggplot(data =cocurr_mu) +
  geom_col(mapping = aes(x= factor(age_group_name,level= age_order), y=cog)) +
  facet_wrap('sex') +
  labs(x='', y='Cognition', fill='Severity') +
  theme(axis.text.x = element_text(hjust=1, angle=45))

Res <- ggplot(data =cocurr_mu) +
  geom_col(mapping = aes(x= factor(age_group_name,level= age_order), y=res)) +
  facet_wrap('sex') +
  labs(x='', y='Respiratory', fill='Severity') +
  theme(axis.text.x = element_text(hjust=1, angle=45))

Long_term <- ggplot(data =cocurr_mu) +
  geom_col(mapping = aes(x= factor(age_group_name,level= age_order), y=long_term)) +
  facet_wrap('sex') +
  labs(x='', y='Long Term covid', fill='Severity') +
  theme(axis.text.x = element_text(hjust=1, angle=45))

# Res_cog <- ggplot(data = cocurr_mu) +
#   geom_col(mapping = aes(x= factor(age_group_name,level= age_order), y=Cog_Res)) +
#   facet_wrap('sex') +
#   labs(x='', y='Respiratory_Cognition', fill='Severity') +
#   theme(axis.text.x = element_text(hjust=1, angle=45))

grid.arrange(Cog, Res, Long_term, ncol=1)

Cog_percent <- ggplot(data =cocurr_mu) +
  geom_col(mapping = aes(x= factor(age_group_name,level= age_order), y=cog/N)) +
  facet_wrap('sex') +
  labs(x='', y='Cognition Percentage', fill='Severity') +
  theme(axis.text.x = element_text(hjust=1, angle=45))+
  scale_y_continuous(labels = scales::percent_format())

Res_percent <- ggplot(data =cocurr_mu) +
  geom_col(mapping = aes(x= factor(age_group_name,level= age_order), y=res/N)) +
  facet_wrap('sex') +
  labs(x='', y='Respiratory Percentage', fill='Severity') +
  theme(axis.text.x = element_text(hjust=1, angle=45))+
  scale_y_continuous(labels = scales::percent_format())

Long_term_percent <- ggplot(data =cocurr_mu) +
  geom_col(mapping = aes(x= factor(age_group_name,level= age_order), y=long_term/N)) +
  facet_wrap('sex') +
  labs(x='', y='Long Term covid Percentage', fill='Severity') +
  theme(axis.text.x = element_text(hjust=1, angle=45))+
  scale_y_continuous(labels = scales::percent_format())

# Res_cog_percent <- ggplot(data = cocurr_mu) +
#   geom_col(mapping = aes(x= factor(age_group_name,level= age_order), y=Cog_Res/N)) +
#   facet_wrap('sex') +
#   labs(x='', y='Respiratory_Cognition Percentage', fill='Severity') +
#   theme(axis.text.x = element_text(hjust=1, angle=45))+
#   scale_y_continuous(labels = scales::percent_format())

grid.arrange( Cog_percent, Res_percent, Long_term_percent,ncol=1)

dev.off()
