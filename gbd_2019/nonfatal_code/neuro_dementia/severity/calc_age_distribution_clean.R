## ---------------------------------------------------------------------------------------------------- ##
## Append mean_age to rows of data for which missing
##
## Calculates mean_age based on dementia prevalence estimates
## Author: USERNAME, sourcing from USERNAME
## 06/20/2019
## ---------------------------------------------------------------------------------------------------- ##


## SET UP ENVIRONMENT --------------------------------------------------------------------------------- ##

rm(list=ls())

if (Sys.info()["sysname"] == "Linux") {
  j_drive <- "FILEPATH"
  h_drive <- "FILEPATH"
} else {
  j_drive <- "FILEPATH"
  h_drive <- "FILEPATH"
}

library(data.table)
date <- gsub("-", "_", Sys.Date())


## SET OBJECTS ---------------------------------------------------------------------------------------- ##

args <- commandArgs(trailingOnly = TRUE) #this is everything fed into this qsub
loop <- as.numeric(args[1])
filename <- args[2]

dem_dir <- paste0("FILEPATH")
severity_dir <- paste0("FILEPATH")
fn_dir <- "FILEPATH"


step <- "iterative"
round <- 6
prev <- 5
dem_cause <- ID
years <- 1990:2017 # aren't currently using studies past 2016
sexes <- 1:3
dem_ages <- c(13:22, 30:32, 235)

## SOURCE FNS ----------------------------------------------------------------------------------------- ##

source(paste0("FILEPATH"))
source(paste0("FILEPATH"))
source(paste0("FILEPATH"))

mround <- function(x,base){
  base*round(x/base)
}


## READ IN DATA  -------------------------------------------------------------------------------------- ##

age_dt <- get_age_metadata(12)

calc_age <- fread(filename)

#subset data
row_end <- loop*5
row_start <- row_end-4
row_end <- min(nrow(calc_age),row_end)

calc_age <- calc_age[row_start:row_end,]

## FUNCTION TO GET AGES FOR POPULATION CALCULATION
get_age_mean <- function(n){
  row <- calc_age[n]
  age1 <- age_dt[age_group_years_start == min(row[, mround(age_start, 5)],95), age_group_id]
  print(paste0("age1: ",age1))
  lastage <- age_dt[age_group_years_end == min(row[, mround(age_end, 5)],95), age_group_id]
  print(paste0("lastage: ",lastage))
  ages <- age_dt[age_group_id >= age1 & age_group_id <= lastage, age_group_id]
  age_string <- paste("c(", ages[1], paste(", ", ages[-1], collapse = ''), ")", collapse = '')
  if(lastage<=age1){ # this happens when age_start==age_end and is thus the end of one age bracket & beginning of the next
    age_string <- paste("c(", age1, ")")
  }
  row[, age_string := age_string]
  print(paste0("age_string: ",age_string))
  
  age_group_ids <- row[,eval(parse(text = age_string))]
  loc_id <- row[,location_id]
  year_id <- row[,year]
  prevalence <- get_outputs("cause",
                            cause_id = dem_cause,
                            measure_id = prev,
                            metric_id = 1,
                            age_group_id = row[,eval(parse(text = age_string))],
                            year_id = max(row[,year],1990),
                            sex_id = row[,sex_id],
                            location_id = row[,location_id],
                            gbd_round_id = 5)
  prevalence <- copy(prevalence[,.(age_group_id, age_group_name, location_id, location_name, sex_id, year_id, val, upper, lower)])
  prevalence <- merge(prevalence, age_dt, by = "age_group_id")
  prevalence[,midage := (age_group_years_start + age_group_years_end) / 2]
  prevalence[, total := sum(val)] # make sure this is correct var
  average_age <- prevalence[,sum(val/total*midage)]
  print(paste0("average_age: ",average_age))
  row[,mean_age:=average_age]
  row <- row[,.(location_id, sex_id, location_name, year_start, year_end, age_start, age_end, mean_age)]
  return(row)
}


calc_age <- rbindlist(lapply(1:nrow(calc_age), get_age_mean))
## SAVE   ----------------------------------------------------------------------------------------- ##

write.csv(calc_age, file=paste0("FILEPATH"), row.names = F)
