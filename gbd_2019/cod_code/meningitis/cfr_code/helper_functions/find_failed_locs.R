# this is a function to find failed locations from a given step name as input

find_failed_locs <- function(step) {
  
  source("current/r/get_location_metadata.R")
  
  # pull locations from CSV created in model_custom
  # locations <- readRDS(file.path(in_dir,"locations_temp.rds"))
  
  # or generate all locations
  loc.meta <- get_location_metadata(location_set_id = 9, gbd_round_id = 6)
  locations <- loc.meta[most_detailed == 1 & is_estimate == 1, unique(location_id)]
  
  date <- gsub("-", "_", Sys.Date())
  check_dir <- # filepath
  checks <- list.files(check_dir, pattern='finished.*.txt')
  finished_locs <- c()
  for (x in checks) {
    finished_locs <- c(finished_locs, substr(x, 10, nchar(x) - 4))
  }
  finished_locs <- as.integer(finished_locs)
  failed_locs <- setdiff(locations, finished_locs)
  return(failed_locs)
}
