# This script contains a function that checks whether all locations are present in a specified directory
# for a specific measure_name.

library(data.table)

source("/PATH/get_location_metadata.R")

check_unfinished_locs <- function(location_set_id, gbd_round_id, decomp_step, paf_dir, measure_name) {
  locations <- get_location_metadata(location_set_id=location_set_id, gbd_round_id = gbd_round_id, decomp_step = decomp_step)
  locations <- locations$location_id[locations$is_estimate==1]
  
  unfinished_locations <- c()
  
  files <- list.files(paf_dir)
  checks <- files[which(files %like% measure_name)]
  finished <- lapply(checks, function(file) substr(file, 5, nchar(file)-4))
  unfinished_locations <- c(unfinished_locations, setdiff(locations, finished))
}