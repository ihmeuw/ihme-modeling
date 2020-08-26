## Compile the edensity output files into location-specific files

rm(list=ls())

## packages & scripts
library(data.table)
source("FUNCTION")

## versioning
output.version <- 6 # GBD 2019 resubmission (all years)

## st-gpr run_id
run_id <- 83492

## directories
input_dir <- file.path("FILEPATH")
output_dir <- file.path("FILEPATH")
if(!dir.exists(output_dir)) dir.create(output_dir, recursive = TRUE)

## parameters
years <- 1990:2019
locs <- get_location_metadata(gbd_round_id = 6, location_set_id = 22)
locations <- locs[is_estimate == 1, unique(location_id)]


## append all the years together & save
for (location in locations) {
  relevant_files <- paste0(location, "_", years, ".csv") # all the files from this location
  all_years <- rbindlist(lapply(file.path(input_dir, relevant_files), fread), use.names = TRUE) # rbind all the year-specific files into one file
  
  write.csv(all_years, paste0(file.path(output_dir, location), ".csv"))
}
