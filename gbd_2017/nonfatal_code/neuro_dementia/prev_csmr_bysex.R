###########################################################
### Author: 
### Date: 12/9/2016
### Updated: 12/20/2017
### Project: GBD Nonfatal Estimation
### Purpose: CSMR and Prevalence by location and sex- Dementia
###########################################################

rm(list=ls())

if (Sys.info()["sysname"] == "Linux") {
  j_root <- "/home/j/" 
  h_root <- "/homes/USERNAME/"
} else { 
  j_root <- "J:"
  h_root <- "H:"
}

require(data.table)
require(RMySQL)
library(dbplyr, lib.loc = paste0(j_root, FILEPATH))
library(openxlsx, lib.loc = paste0(j_root, FILEPATH))

## SET OBJECTS
con <- dbConnect(MySQL(), user=USERNAME, password= PASSWORD, 
                 host = HOST, dbname=NAME)
central_function <- paste0(j_root, FILEPATH)
output_dir <- paste0(j_root, FILEPATH)
dismod_model <- 292544
codem_male <- 396329
codem_female <- 396332
date <- gsub("-", "_", Sys.Date())

## SOURCE FUNCTIONS
source(paste0(central_function, "get_location_metadata.R"))
source(paste0(central_function, "get_model_results.R"))
source(paste0(central_function, "get_population.R"))
source(paste0(central_function, "get_ids.R"))

##USER FUNCTIONS
pull_csmr <- function(s){
  name <- sex_ids[sex_id == s, sex]
  dt <- get_model_results(gbd_team = "cod", model_version_id = get(paste0("codem_", name)), measure_id = 5, 
                          year_id = 2017, age_group_id = c(13:20, 30:32, 235), location_id = loc_ids)
  setnames(dt, "mean_death_rate", "csmr")
  dt <- dt[, .(location_id, sex_id, age_group_id, csmr)]
  dt <- merge(dt, age_weights, by = c("age_group_id", "sex_id"))
  dt[, weighted := csmr * new_weight]
  dt[, as_csmr := sum(weighted), by = c("location_id")]
  dt <- unique(dt, by = "location_id")
  dt <- dt[, .(location_id, sex_id, as_csmr)]
  return(dt)
}

## GET AGE WEIGHTS
death_results <- get_model_results(gbd_team = "cod", gbd_id = 543, measure_id = 1, gbd_round_id = 4,
                                   location_id = 1, year_id = 2016, sex_id = c(1, 2), 
                                   age_group_id = c(13:20, 30:32, 235), status = "best")
death_results[, new_weight := mean_death/sum(mean_death), by = "sex_id"]
death_results <- unique(death_results, by = c("age_group_id", "sex_id"))
age_weights <- copy(death_results[, .(age_group_id, sex_id, new_weight)])

## GET LOCATIONS AND SEX INFO
locations <- get_location_metadata(location_set_id=9)
locations <- locations[level == 3]
loc_ids <- unique(locations$location_id)
loc_dt <- locations[, .(location_id, location_name)]
sex_ids <- get_ids(table = "sex")
sex_ids[, sex := tolower(sex)]

## PULL PREVALENCE
prev <- get_model_results(gbd_team = "epi", model_version_id = dismod_model, measure_id = 5,
                          sex_id = c(1, 2), age_group_id = c(13:20, 30:32, 235), year_id = 2017, location_id = loc_ids)
setnames(prev, "mean", "prev")
prev <- prev[, .(location_id, sex_id, age_group_id, prev)]
prev <- merge(prev, age_weights, by = c("age_group_id", "sex_id"))
prev[, weighted := prev * new_weight]
prev[, as_prev := sum(weighted), by = c("location_id", "sex_id")]
prev <- unique(prev, by = c("location_id", "sex_id"))
prev <- prev[, .(location_id, sex_id, as_prev)]

## PULL CSMR
csmr <- rbindlist(lapply(1:2, pull_csmr))

## COMBINE TOGETHER
total <- merge(prev, csmr, by = c("location_id", "sex_id"))
total[, ratio := as_csmr/as_prev]
total <- merge(total, loc_dt, by = c("location_id"))
total <- dcast(total, location_id + location_name ~ sex_id, value.var = c("ratio", "as_prev", "as_csmr"))
total[, sum_mean := (ratio_1 + ratio_2)/2]
total <- total[order(-sum_mean)]
write.xlsx(total, paste0(output_dir, "emr_countries", date, ".xlsx"))
