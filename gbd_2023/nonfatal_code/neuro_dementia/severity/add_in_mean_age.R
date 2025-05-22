## ---------------------------------------------------------------------------------------------------- ##
## Finish data prep / dementia severity
##
## Author: USERNAME
## ---------------------------------------------------------------------------------------------------- ##


## SET UP ENVIRONMENT --------------------------------------------------------------------------------- ##

rm(list=ls())

if (Sys.info()["sysname"] == "Linux") {
  j <- "FILEPATH"
  h <- "FILEPATH"
} else {
  j <- "FILEPATH"
  h <- "FILEPATH"
}

library(data.table)
date <- gsub("-", "_", Sys.Date())

## SET OBJECTS ---------------------------------------------------------------------------------------- ##

dem_dir <- paste0("FILEPATH")
severity_dir <- paste0("FILEPATH")
fn_dir <- "FILEPATH"

nloops_path <- paste0("FILEPATH") 

## SOURCE FUNCTIONS ----------------------------------------------------------------------------------------- ##

source(paste0("FILEPATH","get_ids.R"))
source(paste0("FILEPATH","get_age_metadata.R"))
source(paste0("FILEPATH","get_outputs.R"))

mround <- function(x,base){
  base*round(x/base)
}


## READ IN DATA  -------------------------------------------------------------------------------------- ##

data <- fread(paste0("FILEPATH"))

#determine how many files mean_age data was saved into
nloops <- fread(nloops_path)
nloops <- ceiling(nrow(nloops)/5)
print(nloops)

calc_age <- data.table()
for(i in 1:nloops){
  rows <- fread(paste0("FILEPATH"))
  calc_age <- rbind(calc_age, rows)
}

## MERGE MEAN_AGE BACK INTO MAIN DT ------------------------------------------------------------------- ##

## MERGE BACK ON ALL INFORMATION AND ATTACH TO THE REST OF THE DATA
data_no_mean_age <- data[is.na(mean_age),]
data_no_mean_age[,mean_age:=NULL]
data_no_mean_age <- merge(data_no_mean_age, calc_age, by = c("location_id",
                                                             "location_name",
                                                             "sex_id",
                                                             "year_start",
                                                             "year_end",
                                                             "age_start",
                                                             "age_end"), all.x = T)
#mark which rows are estimates
data_no_mean_age[,estimated_mean_age:=T]
data[,estimated_mean_age:=F]
data <- rbind(data[!is.na(mean_age),], data_no_mean_age)

## SAVE ----------------------------------------------------------------------------------------------- ##

write.csv(data, paste0("FILEPATH"))








