## Docstring ----------------------------------------------------------- ----
## Project: NF COVID
## Script: 
## Description: Extract German(PA_covid) Data with pre and post
## Contributors: NAME
## Date:3/18/2021
## --------------------------------------------------------------------- ----

## Environment Prep ---------------------------------------------------- ----
rm(list=ls(all.names=T))

if (!exists(".repo_base")) {
  .repo_base <- 'FILEPATH'
}
version <- 0

source(paste0(.repo_base, 'FILEPATH/utils.R'))
source(paste0(roots$k, 'FILEPATH/get_ids.R'))
source(paste0(roots$k, 'FILEPATH/get_location_metadata.R'))
source(paste0(roots$k, 'FILEPATH/get_population.R'))

#FILEPATH
setwd("FILEPATH")
germany <- read_excel('20210316_matched_survey_cluster_FU_clinical_adapted_algo(002).xlsx', sheet="summary")[,c("Patient identifier", "FU","fatigue_cluster","cognitive_cluster","mild_resp_cluster","moderate_resp_cluster","severe_resp_cluster","Age","Gender","Inpatient","Intensive care unit")]
germany <- germany[!germany$FU %in% ('12M-FU-month'), ]

#imputed age using the median value
germany$Age[is.na(germany$Age)]<-median(germany$Age,na.rm=TRUE)
summary(germany$Age)

#recode the hospitalized and icu variable as character 
germany <- mutate(germany, hospitalized = ifelse((Inpatient %in% 1), "Hospitalized", "Non-hospitalized" ))
germany <- mutate(germany, ICU = ifelse((`Intensive care unit` %in% 1), "ICU", "Non-ICU" ))
#there were some non-integer age
germany$Age <- ceiling(germany$Age)

#coding age
germany <- mutate(germany, age_group_name = ifelse(Age %in% 15:19, "15 to 19",
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


#caclulate sample size by
sample_n_agesex <-germany %>% 
  group_by(age_group_name,`hospitalized`,ICU, Gender, FU) %>%
  summarise(N=n()) 

sample_n_sex <-germany %>% 
  group_by(`hospitalized`,ICU, Gender, FU) %>%
  summarise(N=n()) 

sample_n <- germany %>%
  group_by(`hospitalized`,ICU, FU) %>%
  summarise(N=n())

#rename variables
names(germany)[names(germany) == "fatigue_cluster"] <- "post_acute"
names(germany)[names(germany) == "cognitive_cluster"] <- "cognitive"
names(germany)[names(germany) == "mild_resp_cluster"] <- "res_mild"
names(germany)[names(germany) == "moderate_resp_cluster"] <- "res_moderate"
names(germany)[names(germany) == "severe_resp_cluster"] <- "res_severe"

#create the new group variable 
germany <- mutate(germany, res_combine = ifelse((res_mild %in% 1 | res_moderate %in% 1
                                               | res_severe %in% 1), 1, 0 ))

germany <- mutate(germany, long_term = ifelse((post_acute %in% 1 | cognitive %in% 1
                                       | res_combine %in% 1), 1, 0 ))

germany <- mutate(germany, Cog_Res = ifelse((cognitive %in% 1 & res_combine %in% 1), 1, 0 ))
germany <- mutate(germany, Cog_Fat = ifelse((cognitive %in% 1 & post_acute %in% 1), 1, 0 ))
germany <- mutate(germany, Res_Fat = ifelse((res_combine %in% 1 & post_acute %in% 1), 1, 0 ))
germany <- mutate(germany, Cog_Res_Fat = ifelse((res_combine %in% 1 & post_acute %in% 1
                                       & cognitive %in% 1), 1, 0 ))

#check overlaps
germany$overlap <- rowSums(germany[,c("res_mild","res_moderate","res_severe")])
#Handling the cases that the patients falling in more than one(mild, moderate, severe) category
#they always move to the more severe state
DT = as.data.table(germany)
DT[(overlap >1 & res_mild==res_moderate), res_mild := 0]
DT[(overlap >1 & res_moderate== res_severe), res_moderate := 0]
DT[(overlap >1 & res_mild== res_severe), res_mild := 0]
inter<-subset(DT, select=-c(Inpatient, `Intensive care unit`,overlap))

#summary(DT$Age)
summary(DT$post_acute)
summary(DT$cognitive)
summary(DT$res_combine)
summary(DT$long_term)

#write all_data
write.csv(inter, paste0("FILEPATH", version, ".csv"))

#tabulate, order the columns
data_long <- gather(inter , measure, value, c(post_acute:long_term) )
inter <- inter[, c(2,9,10,11,12,3,4,5,6,7,13,14,15,16,17,18)]

data_long <- gather(inter , measure, value, c(post_acute:Cog_Res_Fat))

#tabulate by age group, followup time, gender, hospitalized, ICU
cocurr_mu <-data_long %>%
  group_by(age_group_name,FU,Gender,hospitalized,ICU, measure) %>%
  filter(value=='1') %>%
  summarise(var=n()) %>%
  spread(measure, var)

cocurr_mu <- merge(x=cocurr_mu, y=sample_n_agesex, all.y=TRUE)
cocurr_mu[is.na(cocurr_mu)] <- 0

write.csv(cocurr_mu, paste0("FILEPATH", version, ".csv"))

#tabulate by followup time, gender, hospitalized, ICU
cocurr_sex <-data_long %>%
  group_by(FU,Gender,hospitalized,ICU, measure) %>%
  filter(value=='1') %>%
  summarise(var=n()) %>%
  spread(measure, var)

cocurr_sex <- merge(x=cocurr_sex, y=sample_n_sex, all.y=TRUE)
cocurr_sex[is.na(cocurr_sex)] <- 0

write.csv(cocurr_sex, paste0("FILEPATH", version, ".csv"))

#tabulate by followup time, hospitalized, ICU
cocurr <-data_long %>%
  group_by(FU,hospitalized,ICU, measure) %>%
  filter(value=='1') %>%
  summarise(var=n()) %>%
  spread(measure, var)

cocurr <- merge(x=cocurr, y=sample_n, all.y=TRUE)
cocurr[is.na(cocurr)] <- 0

write.csv(cocurr, paste0("FILEPATH", version, ".csv"))

