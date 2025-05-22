####
# Lagging 2020 results to 2021-2022 for China, Nigeria, Kenya and Ethiopia subnational
#
# Dismod model had a spike after 2020 for this subnational locations, so we are correcting this.

#functions
source("FILEPATH/get_location_metadata.R")
library(dplyr)

#locations
path_all_draws <- "FILEPATH"

#getting location metadata

loc_metadata <- get_location_metadata(location_set_id = 9,decomp_step = ADDRESS)
loc_metadata <- subset(loc_metadata, parent_id == 6| parent_id== 214 | parent_id == 179 | parent_id == 180 | parent_id == 44533)

#get locations ids for subnational

locs_id <- loc_metadata$location_id

#function

for(i in locs_id){

  #import draws from location_id
  loc_draws <- read.csv(paste0(path_all_draws, i, ".csv"))

  #drop years 2021 and 2022
  loc_draws <- subset(loc_draws, year_id < 2020)
  
  #create draws for 2020, 2021,20222 from 2019
  loc_draws20 <- subset(loc_draws, year_id == 2019)
  loc_draws20 <- mutate(loc_draws20, year_id = 2020)
  
  loc_draws21 <- subset(loc_draws, year_id == 2019)
  loc_draws21 <- mutate(loc_draws21, year_id = 2021)
  
  loc_draws22 <- subset(loc_draws, year_id == 2019)
  loc_draws22 <- mutate(loc_draws22, year_id = 2022)

  #rbind
  loc_draws_f <- rbind(loc_draws, loc_draws20, loc_draws21, loc_draws22)
  rm(loc_draws, loc_draws21, loc_draws22)

  #save
  write.csv(loc_draws_f, paste0(path_all_draws, i, ".csv"), row.names=FALSE)
}





