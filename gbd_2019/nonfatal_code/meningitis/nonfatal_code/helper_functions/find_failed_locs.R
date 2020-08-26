# this is a function to find failed locations from a given step name as input
# date <- gsub("-", "_", Sys.Date())
date <- "2020_01_26"
find_failed_locs <- function(step) {
  
  source("current/r/get_location_metadata.R")
  
  # pull locations from CSV created in model_custom
  # locations <- readRDS(file.path(in_dir,"locations_temp.rds"))
  
  # or generate all locations
  loc.meta <- get_location_metadata(location_set_id = 9, gbd_round_id = 6)
  locations <- loc.meta[most_detailed == 1 & is_estimate == 1, unique(location_id)]
  
  step_args <- fread("step_args.csv")
  
  step_name <- step_args[step_num == step, step_name]
  step_num <- step
  
  men_dir <- paste0("filepath", date)
  root_tmp_dir <- paste0(men_dir, "/draws")
  tmp_dir <- paste0(root_tmp_dir, "/",step_num, "_", step_name)
  checks <- list.files(file.path(tmp_dir, "02_temp/01_code/checks"), pattern='finished.*.txt')
  finished_locs <- c()
  for (x in checks) {
    finished_locs <- c(finished_locs, substr(x, 13, nchar(x) - 4))
  }
  finished_locs <- as.integer(finished_locs)
  failed_locs <- setdiff(locations, finished_locs)
  return(failed_locs)
}
