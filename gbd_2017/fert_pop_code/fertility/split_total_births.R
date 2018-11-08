
##############################
## Purpose: Split Total Birth numbers using model into age specific information for loop 2
##############################

sessionInfo()
rm(list=ls())
library(data.table)
library(haven)
library(lme4)
library(ggplot2)
library(splines)
library(parallel)
library(magrittr)
library(mortdb, lib = FILEPATH)


if (Sys.info()[1] == 'Windows') {
  username <- USERNAME
  root <- "J:/"
  version <- "va17"
  loc <- "CHN_44533"
} else if (interactive()){
  username <- Sys.getenv("USER")
  root <- FILEPATH
  version <- 
  loc <- 
} else {
  username <- Sys.getenv("USER")
  root <- FILEPATH
  loc <- commandArgs(trailingOnly = T)[1]
  version <- commandArgs(trailingOnly = T)[2]
}

print(loc)
## set directories
birth_dir <- FILEPATH
asfr_dir <- FILEPATH
jbase <- FILEPATH
data_dir <- FILEPATH
j_data_dir <- FILEPATH

## read in data

total_births <- data.table(readRDS(FILEPATH))
pop <- fread(FILEPATH)
input_data <- readRDS(FILEPATH)
input_data <- input_data[ihme_loc_id == loc]

loop1asfr <- list()
for(age in seq(15,45, 5)){
  loop1asfr[[paste0(age)]] <- fread(paste0(asfr_dir, "age_", age, "/gpr_", loc, "_", age, ".csv"))
  loop1asfr[[paste0(age)]]$age <- age
}
loop1asfr <- rbindlist(loop1asfr)

## getting age map
age_map <- data.table(get_age_map())[,.(age = age_group_name_short, age_group_id)]
age_map[,age := as.numeric(age)]

loc_map <- data.table(get_locations(level="all", gbd_year = 2017))

## data prep/processing
total_births <- total_births[fmeasid == 4][age_group_id == 22][,.(nid, source, source_type, ihme_loc_id, year_id, birth_data = val, pvid = date_added)]
total_births <- total_births[ihme_loc_id == loc]
total_births[, year_id := as.numeric(year_id)][,birth_data := as.numeric(birth_data)]

pop[,pvid := NULL]
pop <- merge(pop, age_map, all.x = T, by = "age_group_id")
pop <- merge(pop, loc_map[,.(ihme_loc_id, location_id)], all.x = T, by = "location_id")

loop1asfr[,year_id := floor(year)][,year := NULL][,lower := NULL][,upper := NULL]

## need to split total births to maternal age specific births
calculate_asfr_data <- function(asfr, population, total_birth_data){
  data <- merge(asfr, population, all.x = T, by = c("age", "year_id", "ihme_loc_id"))
  
  ## first get ratio of predicted age specific births to predicted total births
  data[,mean := mean * population]
  
  total <- data[,.(model_total_births = sum(mean)), by = c("year_id", "ihme_loc_id")]
  data <- merge(data, total, by = c("year_id", "ihme_loc_id"), all=T)
  data[,age_prop := mean/model_total_births]
  
  ## apply ratio to total births data
  data <- merge(data, total_birth_data, by = c("ihme_loc_id", "year_id"), all.y = T)
  data[,birth_data := birth_data * age_prop]
  
  ## now get into asfr space by dividing by Charlton's pops
  data[,val := birth_data/ population]
  
  ## calculate variance
  data[,var := (val * (1-val)) / birth_data]
  
  ## format to match the data being read in to model
  data[,fmeasid := 2]
  data[,update := NA]
  data <- data[,.(nid, id = source, source_type, location_id, ihme_loc_id, year_id, age_group_id, fmeasid, val, var, update, pvid)]
  
  return(data)
}

split_asfr <- calculate_asfr_data(asfr = loop1asfr, population = pop, total_birth_data = total_births)

write.csv(split_asfr, paste0(data_dir, "total_births/", loc, "split_births.csv"), row.names = F)



