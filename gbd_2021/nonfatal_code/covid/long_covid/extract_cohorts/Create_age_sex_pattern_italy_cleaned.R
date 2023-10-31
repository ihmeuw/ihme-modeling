## Docstring ----------------------------------------------------------- ----
## Project: NF COVID
## Script: 
## Description: Create age_sex pattern for Italian data
## The code was used to clean the messy input file
## Date 2/8/2021
## --------------------------------------------------------------------- ----

## Environment Prep ---------------------------------------------------- ----
rm(list=ls(all.names=T))

if (!exists(".repo_base")) {
  .repo_base <- 'FILEPATH'
}

source(paste0(.repo_base, 'FILEPATH/utils.R'))
source(paste0("ROOT", 'FILEPATH/get_ids.R'))
source(paste0("ROOT", 'FILEPATH/get_location_metadata.R'))
source(paste0("ROOT", 'FILEPATH/get_population.R'))

## --------------------------------------------------------------------- ----
library(dplyr)
library(data.table)
library(readxl)

#Import adult survey

#clean the age pattern. 
adult <- read_excel('Buonsenso_LongCovid-GBD_formatted_v2.xlsx', 
                   sheet="Long-COVID")[0:237,c("patient_id", "covid19_test", "date_of_birth","sex")]
adult$dob <- format(as.Date(adult$date_of_birth, format="%d/%m/%Y"), "%d-%m-%Y")
adult$date_of_birth = as.Date(as.numeric(adult$date_of_birth), origin = "1899-12-30")
adult$date_of_birth <- format(adult$date_of_birth, "%d-%m-%Y")
adult$dob[158:236]<-adult$date_of_birth[158:236]

adult$new_dob <- as.Date(adult$dob, "%d-%m-%Y")
today <- as.Date("2021-02-08")

adult$age <- difftime(today, adult$new_dob, units="days")/365
output1 <- adult[,c('patient_id','covid19_test','new_dob','age','sex')]

#Import children survey
child <- read_excel('Buonsenso_LongCovid-GBD_formatted_v2.xlsx', 
                    sheet="Pediatric")[,c("patient_id","sex","covid19_test","date_of_birth")]

child<- child[!is.na(child$date_of_birth), ]
supp <- anti_join(child, adult, by='patient_id')
supp$dob <- supp$date_of_birth
supp$dob <- format(as.Date(supp$date_of_birth, format="%d/%m/%Y"), "%d-%m-%Y")
supp$date_of_birth = as.Date(as.numeric(supp$date_of_birth), origin = "1899-12-30")
supp$date_of_birth <- format(supp$date_of_birth, "%d-%m-%Y")

supp$dob <- ifelse(is.na(supp$dob), supp$date_of_birth, supp$dob)
supp$dob <- as.Date(supp$dob, "%d-%m-%Y")
supp$new_dob <- as.Date(supp$dob, "%d-%m-%Y")
supp$age <- difftime(today, supp$new_dob, units="days")/365
output2 <- supp[,c('patient_id','covid19_test','new_dob','age','sex')]

output <- rbind(output1, output2)
write.csv(output,"Italy_age_pattern_v1.csv")


