## Docstring ----------------------------------------------------------- ----
## Project: NF COVID
## Script: 
## Description: Extract UW covid data(HAARVI)
## Date:3/5/2021
## --------------------------------------------------------------------- ----

## Environment Prep ---------------------------------------------------- ----
rm(list=ls(all.names=T))

if (!exists(".repo_base")) {
  .repo_base <- 'FILEPATH'
}
version <- 2

source(paste0(.repo_base, 'FILEPATH/utils.R'))
source(paste0("ROOT", 'FILEPATH/get_ids.R'))
source(paste0("ROOT", 'FILEPATH/get_location_metadata.R'))
source(paste0("ROOT", 'FILEPATH/get_population.R'))
  
#the UW HAARVI data, extract the flags directly
UW <- read_excel('HAARVI follow-up.xlsx', sheet="HAARVI follow-up raw data")[,c('Participant Study ID',"Hospitalized/Ambulatory",'Age','Fat','Resp',"Cogn","Any'")]
followup<-read_excel("HAARVI_Dataset_hosp_ambul_with_fu_time.xlsx")[,c('Participant Study ID','follow_up_days')]

#merge sex data with the followup folder
sex <- read.csv('HAARVI_Dataset_sex.csv')[,c('Participant.Study.ID','Sex')]
followup <-merge(followup, sex, by.x="Participant Study ID",by.y = "Participant.Study.ID")

UW <- merge(UW, followup)
#code the age group
UW <- mutate(UW, age_group_name = ifelse(Age %in% 15:19, "15 to 19",
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




UW <-UW[!is.na(UW$`Participant Study ID`),]

#caclulate sample size by
sample_n <-UW %>% 
  group_by(age_group_name,`Hospitalized/Ambulatory`,Sex ) %>%
  summarise(N=n()) 

sample_n_sex <-UW %>% 
  group_by(`Hospitalized/Ambulatory`,Sex ) %>%
  summarise(N=n()) 

#rename columns
names(UW)[names(UW) == "Fat"] <- "post_acute"
names(UW)[names(UW) == "Cogn"] <- "cognitive"
names(UW)[names(UW) == "Resp"] <- "res_combine"
names(UW)[names(UW) == "Any'"] <- "long_term"

summary(UW$Age)
summary(UW$post_acute)
summary(UW$cognitive)
summary(UW$res_combine)
summary(UW$long_term)

#adding the overlap categories
UW <- mutate(UW, Cog_Res = ifelse((cognitive %in% 1 & res_combine %in% 1), 1, 0 ))
UW <- mutate(UW, Cog_Fat = ifelse((cognitive %in% 1 & post_acute %in% 1), 1, 0 ))
UW <- mutate(UW, Res_Fat = ifelse((res_combine %in% 1 & post_acute %in% 1), 1, 0 ))
UW <- mutate(UW, Cog_Res_Fat = ifelse((res_combine %in% 1 & post_acute %in% 1
                                       & cognitive %in% 1), 1, 0 ))

summary(UW$Cog_Res)
summary(UW$Cog_Fat)
summary(UW$Res_Fat)
summary(UW$Cog_Res_Fat)

UW$follow_up_index <- "symptom onset"
#save all marked data
write.csv(UW, paste0("UW_HAARVI_all_data_marked_v", version, ".csv"))

inter<-subset(UW, select=-c(`Participant Study ID`,follow_up_days,Age,follow_up_index))
inter <- inter[, c(6,7, 1, 2, 3, 4,5,8,9,10,11)]
#tabulate by gender and sex
data_long <- gather(inter , measure, value, c(post_acute:Cog_Res_Fat))

#by age_group, index, ambulatory/hospitalized
cocurr_mu <-data_long %>%
  group_by(age_group_name,`Hospitalized/Ambulatory`,Sex, measure) %>%
  filter(value=='1') %>%
  summarise(var=n()) %>%
  spread(measure, var)

cocurr_mu <- merge(x=cocurr_mu, y=sample_n, all.y=TRUE)
cocurr_mu[is.na(cocurr_mu)] <- 0
cocurr_mu$follow_up_index <-"symptom onset"
cocurr_mu$follow_up_time <- median(UW$follow_up_days, na.rm=TRUE)

write.csv(cocurr_mu, paste0("UW_byAgeSex_", version, ".csv"))

#sex information is still missing 

cocurr_sex <-data_long %>% 
  group_by(`Hospitalized/Ambulatory`,Sex,measure) %>%
  filter(value=='1') %>%
  summarise(var=n()) %>%
  spread(measure, var) 

cocurr_sex <- merge(x=cocurr_sex, y=sample_n_sex, all.y=TRUE)
cocurr_sex[is.na(cocurr_sex)] <- 0
cocurr_sex$follow_up_index <-"symptom onset"
cocurr_sex$follow_up_time <- median(UW$follow_up_days, na.rm=TRUE)
write.csv(cocurr_sex, paste0("UW_bySex_v", version, ".csv"))
