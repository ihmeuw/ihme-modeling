###########################################################
### Author: 
### Adapted from Code written by 
### Date: 12/9/2016
### Project: GBD Nonfatal Estimation
### Purpose: CSMR and Prevalence by location and sex- Alzheimer's Disease 
###########################################################

#Setup
rm(list=ls())

if (Sys.info()["sysname"] == "Linux") {
  j.root <- "/home/j/" 
  h.root <- "/homes/USERNAME/"
} else { 
  j.root <- "J:"
  h.root <- "H:"
}

#load packages, install if missing
require(data.table)
require(RMySQL)

##connection string for epi database
con <- dbConnect(MySQL(), user= USERNAME, password= PASSWORD, 
                 host = HOSTNAME, dbname='cod')

##set directories
central.function <- FILEPATH
output.dir <- FILEPATH

#get central functions
source(paste0(j.root, central.function, "get_location_metadata.R"))
source(paste0(j.root, central.function, "get_model_results.R"))
source(paste0(j.root, central.function, "get_population.R"))
source(paste0(j.root, FILEPATH, ".r"))

##Get gbd standard age-weights
age_weights <- as.data.table(get_age_weights())
age_weights <- age_weights[gbd_round_id==4 & age_group_weight_description=="IHME standard age weight",]
age_weights <- age_weights[age_group_id %in% c(13:20, 30:32, 235),]
age_weights <- age_weights[, sum := sum(age_group_weight_value)]
age_weights <- age_weights[, new_weight := age_group_weight_value/sum]

##pull locations
locations <- as.data.table(get_location_metadata(location_set_id=35))
locations <- locations[,c("location_id", "ihme_loc_id", "location_name", "is_estimate", "level"), with=F] ##keep what we need

##pull prevalence
prevalence.f <- as.data.table(get_model_results(gbd_team = "epi", model_version_id = 95694, 
                                                measure_id = 5, year_id = 2016, sex_id = 2, age_group_id = 27))
prevalence.m <- as.data.table(get_model_results(gbd_team = "epi", model_version_id = 95694, 
                                                measure_id = 5, year_id = 2016, sex_id = 1, age_group_id = 27))
setnames(prevalence.f, "mean", "as_prev")
setnames(prevalence.m,"mean","as_prev")
prevalence.m <- prevalence.m[,.(age_group_id, sex_id, location_id, prev)]
prevalence.f <- prevalence.f[,.(age_group_id, sex_id, location_id, prev)]

##pull csmr
csmr.m <- as.data.table(get_model_results(gbd_team = "cod", model_version_id = 92756, 
                                          age_group_id = c(2:20, 30:32, 235), year_id = 2016))
csmr.f <- as.data.table(get_model_results(gbd_team = "cod", model_version_id = 92753,
                                          age_group_id = c(2:20, 30:32, 235), year_id = 2016))
csmr.m[,deaths := mean_cf * mean_env] ##generate number of deaths
csmr.f[,deaths := mean_cf * mean_env] ##generate number of deaths
csmr.m <- csmr.m[,.(deaths, population, age_group_id, sex_id, location_id)]
csmr.f <- csmr.f[,.(deaths, population, age_group_id, sex_id, location_id)]

##for both csmr sets
all_data <- list(csmr.f = csmr.f, csmr.m = csmr.m)
for (x in names(all_data)){
  ##age standardized csmr
  data <- merge(all_data[[x]], age_weights, by = c("age_group_id"))
  data[,as_csmr := deaths*new_weight/population] ##age standardize
  data[,as_csmr := sum(as_csmr), by = "location_id"] ##sum over age
  data <- unique(data, by= "location_id") ##only unique
  assign(x, data)
}

male <- merge(csmr.m, prevalence.m, by = c("location_id"))
female <- merge(csmr.f, prevalence.f, by= c("location_id"))

##for each sex total manipulations
all_data <- list(male = male, female = female)
for (x in names(all_data)){
  data <- all_data[[x]]
  data[,ratio := as_csmr/as_prev] ##generate ratio
  data <- merge(data, locations, by= "location_id") ## merge to get location names
  data <- data[level==3,] ##only countries
  data <- data[,.(location_id, location_name, ihme_loc_id, as_csmr, as_prev, ratio)]
  assign(x, data)
}

write.csv(male, paste0(j.root, FILEPATH, ".csv"))
write.csv(female, paste0(j.root, FILEPATH, ".csv"))