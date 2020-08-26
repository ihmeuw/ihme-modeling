#date <- gsub("-", "_", Sys.Date())
date <- "2020_01_27"

source(paste0(k, "current/r/get_location_metadata.R"))
loc.meta <- get_location_metadata(location_set_id = 9, gbd_round_id = 6)
loc.meta <- loc.meta[loc.meta$most_detailed == 1 & loc.meta$is_estimate == 1, ]
locations <- unique(loc.meta$location_id)

find_failed_locations <- function(step) {
  h <- # filepath
  step_args <- fread(paste0(h,"step_args.csv"))
  
  step_name <- step_args[step_num == step, step_name]
  step_num <- step
  
  enceph_dir <- paste0("filepath", date)
  root_tmp_dir <- paste0(enceph_dir, "/draws")
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

locs <- find_failed_locations("05b")
cat(locs, sep = ", ")
