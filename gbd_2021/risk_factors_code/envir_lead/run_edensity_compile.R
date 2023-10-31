## 1/6/2020
## Compile the edensity output files into location-specific files for uploading to epi database

rm(list=ls())

## packages & scripts
library(data.table)
library(parallel)
source("FILEPATH/get_location_metadata.R")

## versioning (see run_edensity_launch.R)
output_version <- 15 # GBD 2020 final

## st-gpr run_id (see run_edensity_launch.R)
run_id <- 168590

## directories
input_dir <- file.path("FILEPATH", output_version, run_id, "loc_year")
output_dir <- file.path("FILEPATH", output_version, run_id)
if(!dir.exists(output_dir)) dir.create(output_dir, recursive = TRUE)

## parameters
years <- 1990:2022
locs <- get_location_metadata(gbd_round_id = 7, location_set_id = 22)
locations <- locs[is_estimate == 1, unique(location_id)]

## append all the years together & save
for (location in locations) {
  print(location)
  
  relevant_files <- paste0(location, "_", years, ".csv") # all the files from this location
  all_years <- rbindlist(mclapply(file.path(input_dir, relevant_files), fread, mc.cores = 5), use.names = TRUE) # rbind all the year-specific files into one file
  
  write.csv(all_years, paste0(file.path(output_dir, location), ".csv"), row.names = FALSE)
}
