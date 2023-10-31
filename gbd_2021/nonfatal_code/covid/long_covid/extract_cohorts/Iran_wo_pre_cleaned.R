## Docstring ----------------------------------------------------------- ----
## Project: NF COVID
## Script: 
## Description: Remove the pre information from the Iran data
## Date 2/25/2021
## Details: removed all the pre information, Hp19aX(Hp19a4, Hp19a6, Hpa8, Hp19a9, Hp19a10)
## Without Pre, the fatigue Rule 1 is gone. 

## --------------------------------------------------------------------- ----

## Environment Prep ---------------------------------------------------- ----
rm(list=ls(all.names=T))
if (!exists(".repo_base")) {
  .repo_base <- 'FILEPATH'
}
version <- 0
source(paste0(.repo_base, 'FILEPATH/utils.R'))
source(paste0("ROOT" ,"FILEPATH/get_location_metadata.R"))
source(paste0("ROOT" ,"FILEPATH/get_age_metadata.R"))

#special package to import spss file
library(foreign)
data <- read.spss("Iran COVID Cohort Study_Q1 & Q3_selected variaqble.sav", to.data.frame=TRUE)

#split the age into 5-year group
age_pattern<-setDT(data)

#male 1059, female 879
age_pattern[Dm5<= 1, age_group_name := "Early Neonatal to 1"]
age_pattern[Dm5>1 & Dm5 <=4, age_group_name := "1 to 4"]
age_pattern[Dm5>4 & Dm5<=9, age_group_name := "5 to 9" ]
age_pattern[Dm5>9 & Dm5<=14, age_group_name := "10 to 14" ]
age_pattern[Dm5>14 & Dm5<=20, age_group_name := "15 to 19" ]
age_pattern[Dm5>20 & Dm5<=24, age_group_name := "20 to 24"]
age_pattern[Dm5>24 & Dm5<=29, age_group_name := "25 to 29" ]
age_pattern[Dm5>29 & Dm5<=34, age_group_name := "30 to 34" ]
age_pattern[Dm5>34 & Dm5<=39, age_group_name := "35 to 39" ]
age_pattern[Dm5>39 & Dm5<=44, age_group_name := "40 to 44"]
age_pattern[Dm5>44 & Dm5<=49, age_group_name := "45 to 49" ]
age_pattern[Dm5>49 & Dm5<=54, age_group_name := "50 to 54" ]
age_pattern[Dm5>54 & Dm5<=59, age_group_name := "55 to 59" ]
age_pattern[Dm5>59 & Dm5<=64, age_group_name := "60 to 64"]
age_pattern[Dm5>64 & Dm5<=69, age_group_name := "65 to 69" ]
age_pattern[Dm5>69 & Dm5<=74, age_group_name := "70 to 74" ]
age_pattern[Dm5>74 & Dm5<=79, age_group_name := "75 to 79" ]
age_pattern[Dm5>79 & Dm5<=84, age_group_name := "80 to 84" ]
age_pattern[Dm5>84 & Dm5<=89, age_group_name := "85 to 89" ]
age_pattern[Dm5>89 & Dm5<=94, age_group_name := "90 to 94" ]
age_pattern[Dm5>95, age_group_name := "95_plus" ]

#getting the number of patients by age and by sex 
sample_n <-age_pattern %>% 
  group_by(age_group_name,Dm3 ) %>%
  summarise(N=n()) 

#getting the number of patients by sex 
sample_n_bysex <-age_pattern %>% 
  group_by(Dm3) %>%
  summarise(N=n()) 

dt <- age_pattern[,c('id.2',"Dm3","age_group_name","Dm5","mha12","mha25",
                     "mha92","mha91","mha94","mha111","mha112","mha113","Hp19b4","mha1021",
                     "mha103")]

#Fatigue/pain/emotional problem cluster:
#Rule 2: (MHA1.2 (general weakness) = 1 or MHA2.5 (fatigue during normal activity) = 1 or MHA9.2 (muscle weakness) = 1) and ((MHA9.1 (joint pain) = 1 or MHA9.4 (muscle pain) = 1) or (MHA11.1 (depression) = 1 or MHA11.2 (anxiety) = 1))
dt <- mutate(dt, post_acute = ifelse((mha12 %in% "yes" | mha25 %in% "yes" | mha92 %in% "yes") & ((mha91 %in% "yes" | mha94 %in% "yes") | (mha111 %in% "yes" | mha112 %in% "yes")), 1,0))
table(dt$post_acute)
summary(dt$post_acute)

# Cognition cluster: 
# MHA 11.3 (memory loss) = 1 and Hp19a.4 (reduced concentration and ability for decision making before disease) = 2  and Hp19b.4 (reduced concentration and ability for decision making after disease) = 1
# There is not enough information to grade by severity  
dt <- mutate(dt, cognitive = ifelse((mha113 %in% "yes" & Hp19b4 %in% "yes"), 1,0))
table(dt$cognitive)
summary(dt$cognitive)

# Respiratory cluster:
# Rule 1: mhb25 (history at admission of dyspnea) = 2 (no)  and mb261=0 (no history of use of oxygen prior to admission = no) or mhb25 = 1 
#and mhb251=2 (dyspnea ‘during climbing’) and dyspnea post COVID (mha1021) is 1 (at rest) or 2 (during normal activities)

#res mild
dt <- mutate(dt, res_mild = ifelse(((mha1021 %in% "intense activity" & mha103 %in% 'no' )), 1,0))
table(dt$res_mild)
summary(dt$res_mild)

#res_moderate
dt <- mutate(dt, res_moderate = ifelse(((mha1021 %in% "normal activity" & mha103 %in% 'no')), 1,0))
table(dt$res_moderate)
summary(dt$res_moderate)

#res_severe
dt <- mutate(dt, res_severe = ifelse((mha103 %in% "yes"| mha1021 %in% "rest"), 1,0))
table(dt$res_severe)
summary(dt$res_severe)

#calculate the combined respiratory category
dt <- mutate(dt, res_combine = ifelse((res_mild %in% 1 | res_moderate %in% 1 | res_severe %in% 1), 1, 0))

table(dt$res_combine)
summary(dt$res_combine)

a3 <- dt
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

#Dealing with overlaps in the respiratory category. need to round them one level up
a3$overlap <- rowSums(a3[,c("res_mild","res_moderate","res_severe")])
DT = as.data.table(a3)
DT[(overlap >1 & res_mild==res_moderate), res_mild := 0]
DT[(overlap >1 & res_moderate== res_severe), res_moderate := 0]
DT <- subset(DT, select= -c(overlap))
write.csv(DT, paste0("Iran_without_comparison_v", version, ".csv"))

#only keep the derived variables
dt <- DT[,c("id.2","Dm3","age_group_name","post_acute","cognitive","res_mild","res_moderate","res_severe","res_combine","Cog_Res","Cog_Fat","Res_Fat","Cog_Res_Fat","long_term")]
data_long <- gather(dt , measure, value, c(post_acute:long_term) )

#by age and sex only
cocurr_mu <-data_long %>%
  group_by(age_group_name,Dm3, measure) %>%
  filter(value=='1') %>%
  summarise(var=n()) %>%
  spread(measure, var)

cocurr_mu <- merge(x=cocurr_mu, y=sample_n, all.y=TRUE)
cocurr_mu[is.na(cocurr_mu)] <- 0


write.csv(cocurr_mu, paste0("Iran_without_comparison_byAgeSex_", version, ".csv"))

#table by sex only 
cocurr_sex <-data_long %>% 
  group_by(Dm3,measure) %>%
  filter(value=='1') %>%
  summarise(var=n()) %>%
  spread(measure, var) 


cocurr_sex <- merge(x=cocurr_sex, y=sample_n_bysex, all.y=TRUE)
cocurr_sex[is.na(cocurr_sex)] <- 0
write.csv(cocurr_sex, paste0("Iran_without_comparison_bySex_v", version, ".csv"))


###################################
###################################
#create plots


pdf(file= 'Iran_data_without_comparison_plots.pdf', height=11, width=8.5)
#combined categories
age_order <- c("Early Neonatal to 1", "1 to 4","5 to 9", "10 to 14", "15 to 19","20 to 24","25 to 29", "30 to 34","35 to 39",
               "40 to 44","45 to 49","50 to 54","55 to 59","60 to 64","65 to 69","70 to 74","75 to 79","80 to 84","85 to 89",
               "90 to 94","95_plus")
#combined categories discrete
Post <- ggplot(data =cocurr_mu) +
  geom_col(mapping = aes(x= factor(age_group_name,level= age_order), y=post_acute)) +
  facet_wrap('Dm3') +
  labs(x='', y='Post Fatigue Syndrome', fill='Severity') +
  theme(axis.text.x = element_text(hjust=1, angle=45))

Cog <- ggplot(data =cocurr_mu) +
  geom_col(mapping = aes(x= factor(age_group_name,level= age_order), y=cognitive)) +
  facet_wrap('Dm3') +
  labs(x='', y='Cognition', fill='Severity') +
  theme(axis.text.x = element_text(hjust=1, angle=45))

Res <- ggplot(data =cocurr_mu) +
  geom_col(mapping = aes(x= factor(age_group_name,level= age_order), y=res_combine)) +
  facet_wrap('Dm3') +
  labs(x='', y='Respiratory', fill='Severity') +
  theme(axis.text.x = element_text(hjust=1, angle=45))

Long_term <- ggplot(data =cocurr_mu) +
  geom_col(mapping = aes(x= factor(age_group_name,level= age_order), y=long_term)) +
  facet_wrap('Dm3') +
  labs(x='', y='Long Term covid', fill='Severity') +
  theme(axis.text.x = element_text(hjust=1, angle=45))

grid.arrange(Post, Cog, Res, Long_term, ncol=1)

#combined categories percentage
Post_percent <- ggplot(data =cocurr_mu) +
  geom_col(mapping = aes(x= factor(age_group_name,level= age_order), y=post_acute/N)) +
  facet_wrap('Dm3') +
  labs(x='', y='Post Fatigue Syndrome Percentage', fill='Severity') +
  theme(axis.text.x = element_text(hjust=1, angle=45))+
  scale_y_continuous(labels = scales::percent_format())

Cog_percent <- ggplot(data =cocurr_mu) +
  geom_col(mapping = aes(x= factor(age_group_name,level= age_order), y=cognitive/N)) +
  facet_wrap('Dm3') +
  labs(x='', y='Cognition Percentage', fill='Severity') +
  theme(axis.text.x = element_text(hjust=1, angle=45))+
  scale_y_continuous(labels = scales::percent_format())

Res_percent <- ggplot(data =cocurr_mu) +
  geom_col(mapping = aes(x= factor(age_group_name,level= age_order), y=res_combine/N)) +
  facet_wrap('Dm3') +
  labs(x='', y='Respiratory Percentage', fill='Severity') +
  theme(axis.text.x = element_text(hjust=1, angle=45))+
  scale_y_continuous(labels = scales::percent_format())

Long_term_percent <- ggplot(data =cocurr_mu) +
  geom_col(mapping = aes(x= factor(age_group_name,level= age_order), y=long_term/N)) +
  facet_wrap('Dm3') +
  labs(x='', y='Long Term covid Percentage', fill='Severity') +
  theme(axis.text.x = element_text(hjust=1, angle=45))+
  scale_y_continuous(labels = scales::percent_format())

grid.arrange(Post_percent, Cog_percent, Res_percent, Long_term_percent, ncol=1)


#combined categories discrete
cog_fat <- ggplot(data = cocurr_mu) +
  geom_col(mapping = aes(x= factor(age_group_name,level= age_order), y=Cog_Fat)) +
  facet_wrap('Dm3') +
  labs(x='', y='Cognition_fatigue', fill='Severity') +
  theme(axis.text.x = element_text(hjust=1, angle=45))

Res_fat <- ggplot(data = cocurr_mu) +
  geom_col(mapping = aes(x= factor(age_group_name,level= age_order), y=Res_Fat)) +
  facet_wrap('Dm3') +
  labs(x='', y='Respiratory_fatigue', fill='Severity') +
  theme(axis.text.x = element_text(hjust=1, angle=45))

Res_cog <- ggplot(data = cocurr_mu) +
  geom_col(mapping = aes(x= factor(age_group_name,level= age_order), y=Cog_Res)) +
  facet_wrap('Dm3') +
  labs(x='', y='Respiratory_Cognition', fill='Severity') +
  theme(axis.text.x = element_text(hjust=1, angle=45))

Fat_cog_res <- ggplot(data = cocurr_mu) +
  geom_col(mapping = aes(x= factor(age_group_name,level= age_order), y=Cog_Res_Fat)) +
  facet_wrap('Dm3') +
  labs(x='', y='Respiratory_Cognition_Fatigue', fill='Severity') +
  theme(axis.text.x = element_text(hjust=1, angle=45))

grid.arrange(cog_fat, Res_fat, Res_cog, Fat_cog_res , ncol=1)

#combined categories percentage
cog_fat_percent <- ggplot(data = cocurr_mu) +
  geom_col(mapping = aes(x= factor(age_group_name,level= age_order), y=Cog_Fat/N)) +
  facet_wrap('Dm3') +
  labs(x='', y='Cognition_fatigue Percentage', fill='Severity') +
  theme(axis.text.x = element_text(hjust=1, angle=45))+
  scale_y_continuous(labels = scales::percent_format())

Res_fat_percent <- ggplot(data = cocurr_mu) +
  geom_col(mapping = aes(x= factor(age_group_name,level= age_order), y=Res_Fat/N)) +
  facet_wrap('Dm3') +
  labs(x='', y='Respiratory_fatigue Percentage', fill='Severity') +
  theme(axis.text.x = element_text(hjust=1, angle=45))+
  scale_y_continuous(labels = scales::percent_format())

Res_cog_percent <- ggplot(data = cocurr_mu) +
  geom_col(mapping = aes(x= factor(age_group_name,level= age_order), y=Cog_Res/N)) +
  facet_wrap('Dm3') +
  labs(x='', y='Respiratory_Cognition Percentage', fill='Severity') +
  theme(axis.text.x = element_text(hjust=1, angle=45))+
  scale_y_continuous(labels = scales::percent_format())

Fat_cog_res_percent <- ggplot(data = cocurr_mu) +
  geom_col(mapping = aes(x= factor(age_group_name,level= age_order), y=Cog_Res_Fat/N)) +
  facet_wrap('Dm3') +
  labs(x='', y='Respiratory_Cognition_Fatigue Percentage', fill='Severity') +
  theme(axis.text.x = element_text(hjust=1, angle=45))+
  scale_y_continuous(labels = scales::percent_format())

grid.arrange(cog_fat_percent, Res_fat_percent, Res_cog_percent, Fat_cog_res_percent , ncol=1)

#stack bar, and three bars 
plot <- cocurr_mu[,c("age_group_name","Dm3","cognitive","post_acute","res_combine","N")]
age_order <- c("Early Neonatal to 1", "1 to 4","5 to 9", "10 to 14", "15 to 19","20 to 24","25 to 29", "30 to 34","35 to 39",
               "40 to 44","45 to 49","50 to 54","55 to 59","60 to 64","65 to 69","70 to 74","75 to 79","80 to 84","85 to 89",
               "90 to 94","95_plus")
dt_long <- gather(plot, condition, measurement, cognitive:res_combine)

a3$respiratory <- factor(ifelse(a3$res_mild %in% 1, 'Res_mild',
                                        ifelse(a3$res_moderate%in% 1, 'Res_moderate',
                                               ifelse(a3$res_severe %in% 1, 'Res_severe', 'None'))),
                                 levels=c('Res_mild','Res_moderate','Res_severe'))

#respiratory category discrete and all categories
respiratory_split <- ggplot(data = a3) +
  geom_col(mapping = aes(x=factor(age_group_name,level=age_order), y=res_combine, fill=respiratory)) +
  facet_wrap('Dm3') +
  theme(axis.text.x = element_text(hjust=1, angle=45))


Stack_bar_all_count <- ggplot(data = dt_long) +
  geom_col(mapping = aes(x=factor(age_group_name,level=age_order), y=measurement, fill=condition)) +
  facet_wrap('Dm3') +
  theme(axis.text.x = element_text(hjust=1, angle=45))

three_bars_discret <- ggplot(data= dt_long, aes(x=factor(age_group_name,level=age_order), y=measurement, fill= condition))+
  geom_bar(stat="identity", position=position_dodge()) +
  facet_wrap('Dm3') +
  theme(axis.text.x = element_text(hjust=1, angle=45))

#respiratory category percentage and all categories
a4 <- merge(a3, sample_n[,c('Dm3','age_group_name','N')])
respiratory_split_percentage <- ggplot(data = a4) +
  geom_col(mapping = aes(x=factor(age_group_name,level=age_order), y=res_combine/N, fill=respiratory)) +
  facet_wrap('Dm3') +
  theme(axis.text.x = element_text(hjust=1, angle=45))+
  labs(x='', y='res_combine percentage')+
  scale_y_continuous(labels = scales::percent_format())

grid.arrange(respiratory_split , Stack_bar_all_count, three_bars_discret,respiratory_split_percentage,  ncol=1)

dev.off()
