#This script calculates proportion of time in ictal state (pTIS) for the various headache diagnoses in the HARDSHIP (micro)dataset.
#In the end two data frames are created, one where pTIS is stratified by diagnosis, sex and age, and one where pTIS is stratified by diagnosis and sex.

####################
#### PREPARATION ###
####################

#Loading libraries and diagnostic algorithms.
library(readr)
library(data.table)
library(xlsx)
library(plotrix)
source('FILEPATH')

#Creating a dataframe containing the HARDSHIP-data and turning it into a data table.
df <- read_csv('FILEPATH')
df <- as.data.table(df)

df$R_Diagnosis <- HARDSHIP_Diagnostic_Algorithm_Adults(df)

#####################################################
### CALCULATING PROPORTION OF TIME IN ICTAL STATE ###
#####################################################

#Computing frequency as a proportion of total days. 
#Frequency of most bothersome headache (days/year) takes precedence if migraine/TTH, whereas frequency of any headache (days/month) takes precedence if MOH.
df$R_Frequency_pTIS <-   ifelse(df$R_Diagnosis %in% c(3, 4, 5, 6, 7, 9),
                           ifelse(!is.na(df$D3.02_How_Often_MBH_Days_Per_Year_H20), df$D3.02_How_Often_MBH_Days_Per_Year_H20/365,
                                  ifelse(!is.na(df$D1.03_Headache_Days_Last_Month_H14), df$D1.03_Headache_Days_Last_Month_H14/30, NA)),
                           ifelse(df$R_Diagnosis %in% c(1),
                                  ifelse(!is.na(df$D1.03_Headache_Days_Last_Month_H14), df$D1.03_Headache_Days_Last_Month_H14/30,
                                         ifelse(!is.na(df$D3.02_How_Often_MBH_Days_Per_Year_H20), df$D3.02_How_Often_MBH_Days_Per_Year_H20/365, NA)), 
                                  NA)
)

#Computing duration. 
#Duration of chronic headache (pertains only MOH) takes precedence over usual headache duration (presumably with medication), which takes precedence over duration without medication.
df$R_Duration <- ifelse(df$R_Diagnosis %in% c(1, 3, 4, 5, 6, 7, 9),
                   ifelse(!is.na(df$D2.01_How_Long_Headaches_15_Days_Per_Month_Hours_H15), df$D2.01_How_Long_Headaches_15_Days_Per_Month_Hours_H15,
                          ifelse(!is.na(df$D3.03_How_Long_MBH_Hours_H21), df$D3.03_How_Long_MBH_Hours_H21, 
                                 ifelse(!is.na(df$D3.05_How_Long_Without_Med_MBH_Hours_H23), df$D3.05_How_Long_Without_Med_MBH_Hours_H23, NA))), 
                   NA)

#Computing pTIS by multiplying frequency and duration. 
#Duration is capped at 24 hours since frequency is reported in days per time unit (as opposed to attacks per time unit).
df$R_pTIS <- ifelse(df$R_Duration>=24, df$R_Frequency_pTIS, df$R_Frequency_pTIS*df$R_Duration/24)

############################
#### EXTRACTING RESULTS ####
############################

#Creating age variable for stratifying into 3 age bins (<35; 35-49, >50)
df$R_Age_Group <- ifelse(df$C1.02_Age_H02<35, 1,
                         ifelse(df$C1.02_Age_H02>=35 & df$C1.02_Age_H02<50, 2,
                                ifelse(df$C1.02_Age_H02>=50, 3, NA
                                       )
                                )
                         )

#Drawing results
pTIS_by_sex_and_age <- df[B1.04_Country_Name!="United Kingdom" &
                            B1.04_Country_Name!="Spain_Patient" &
                            B1.04_Country_Name!="Netherland Patient" &
                            B1.04_Country_Name!="Italy" &
                            B1.04_Country_Name!="Ireland" &
                            B1.04_Country_Name!="Germany" &
                            B1.04_Country_Name!="France" &
                            B1.04_Country_Name!="Austria" &
                            B1.04_Country_Name!="Morocco_Fes" &
                            !is.na(C1.01_Gender_H03) &
                            !is.na(R_Age_Group) &
                            R_Diagnosis %in% c(1,3,4,5,6), .(time_ictal_mean=mean(R_pTIS, na.rm=T), time_ictal_se=std.error(R_pTIS, na.rm=T)), by=.(C1.01_Gender_H03, R_Age_Group, R_Diagnosis)]
pTIS_by_sex_and_age <- pTIS_by_sex_and_age[order(R_Age_Group)]
pTIS_by_sex_and_age <- pTIS_by_sex_and_age[order(C1.01_Gender_H03)]
pTIS_by_sex_and_age <- pTIS_by_sex_and_age[order(R_Diagnosis)]

#Relabeling stuff to make it understandable for outsiders
pTIS_by_sex_and_age$C1.01_Gender_H03[pTIS_by_sex_and_age$C1.01_Gender_H03==1] <- "male"
pTIS_by_sex_and_age$C1.01_Gender_H03[pTIS_by_sex_and_age$C1.01_Gender_H03==2] <- "female"
pTIS_by_sex_and_age$R_Age_Group[pTIS_by_sex_and_age$R_Age_Group==1] <- "<35"
pTIS_by_sex_and_age$R_Age_Group[pTIS_by_sex_and_age$R_Age_Group==2] <- "35-49"
pTIS_by_sex_and_age$R_Age_Group[pTIS_by_sex_and_age$R_Age_Group==3] <- ">50"
pTIS_by_sex_and_age$R_Diagnosis[pTIS_by_sex_and_age$R_Diagnosis==1] <- "moh"
pTIS_by_sex_and_age$R_Diagnosis[pTIS_by_sex_and_age$R_Diagnosis==3] <- "definite migraine"
pTIS_by_sex_and_age$R_Diagnosis[pTIS_by_sex_and_age$R_Diagnosis==4] <- "definite tth"
pTIS_by_sex_and_age$R_Diagnosis[pTIS_by_sex_and_age$R_Diagnosis==5] <- "probable migraine"
pTIS_by_sex_and_age$R_Diagnosis[pTIS_by_sex_and_age$R_Diagnosis==6] <- "probable tth"

pTIS_by_sex_and_age <- pTIS_by_sex_and_age %>% rename("diagnosis" = "R_Diagnosis",
                                      "sex" = "C1.01_Gender_H03",
                                      "age" = "R_Age_Group")

#Drawing results (this time not stratified by age)
pTIS_by_sex <- df[B1.04_Country_Name!="United Kingdom" &
                            B1.04_Country_Name!="Spain_Patient" &
                            B1.04_Country_Name!="Netherland Patient" &
                            B1.04_Country_Name!="Italy" &
                            B1.04_Country_Name!="Ireland" &
                            B1.04_Country_Name!="Germany" &
                            B1.04_Country_Name!="France" &
                            B1.04_Country_Name!="Austria" &
                            B1.04_Country_Name!="Morocco_Fes" &
                            !is.na(C1.01_Gender_H03) &
                            !is.na(R_Age_Group) &
                            R_Diagnosis %in% c(1,3,4,5,6), .(time_ictal_mean=mean(R_pTIS, na.rm=T), time_ictal_se=std.error(R_pTIS, na.rm=T)), by=.(C1.01_Gender_H03, R_Diagnosis)]
pTIS_by_sex <- pTIS_by_sex[order(C1.01_Gender_H03)]
pTIS_by_sex <- pTIS_by_sex[order(R_Diagnosis)]

#Relabeling stuff to make it understandable for outsiders
pTIS_by_sex$C1.01_Gender_H03[pTIS_by_sex$C1.01_Gender_H03==1] <- "male"
pTIS_by_sex$C1.01_Gender_H03[pTIS_by_sex$C1.01_Gender_H03==2] <- "female"
pTIS_by_sex$R_Diagnosis[pTIS_by_sex$R_Diagnosis==1] <- "moh"
pTIS_by_sex$R_Diagnosis[pTIS_by_sex$R_Diagnosis==3] <- "definite migraine"
pTIS_by_sex$R_Diagnosis[pTIS_by_sex$R_Diagnosis==4] <- "definite tth"
pTIS_by_sex$R_Diagnosis[pTIS_by_sex$R_Diagnosis==5] <- "probable migraine"
pTIS_by_sex$R_Diagnosis[pTIS_by_sex$R_Diagnosis==6] <- "probable tth"

pTIS_by_sex <- pTIS_by_sex %>% rename("diagnosis" = "R_Diagnosis",
                                      "sex" = "C1.01_Gender_H03")

#########################################
#### EXPORTING THE NEWLY CREATED DFs ####
#########################################

write.xlsx(pTIS_by_sex_and_age, 'FILEPATH')
write.xlsx(pTIS_by_sex, 'FILEPATH')
