#####################################INTRO#############################################
#' Purpose: Turning ST-GPR results from syphilis testing/treatment proportions into usable 
#'          proportions by location
######################################INTRO#############################################
library(data.table)
library(ini)

#ST-GPR functions 
source("FILEPATH")
library(dplyr)

# Read in data ------------------------------------------------------------
##Read in central covariates 
gpr_root <- "FILEPATH"
test_path  <- "FILEPATH"
treat_path <- "FILEPATH"

test_files  <- list.files(test_path)
treat_files <- list.files(treat_path)

# function for processing unracked GPR results
# reads in files that have multiple locs each
# transforms back from logit and saves by location
prep_gpr <- function(file, root_dir) {
  dt <- fread(paste0(root_dir, file))
  dt[, `:=`(age_group_id = NULL, sex_id = NULL)]            # drop unneeded rows
  unique_locs <- unique(dt$location_id)                     # grab for verbose-ness
  cat("Loading data for locations: "); cat(unique_locs)
  cat("\n")
  
  dt_long <- melt(dt, id.vars = c("location_id","year_id"), # melt long
                  measure.vars = patterns("draw_"),
                  variable.name = "draw_num", value.name = "draw")
  
  dt_long[, draw := boot::inv.logit(draw)]                  # transform from logit to prop
  return(dt_long)
}

# save csvs for testing and treatment data
message(paste0(Sys.time(), " Read in all testing (run_id ", test_run_id, ") and treatment (run_id ", treat_run_id, ") data"))

test  <- rbindlist(lapply(test_files,  prep_gpr, root_dir = test_path)) 
treat <- rbindlist(lapply(treat_files, prep_gpr, root_dir = treat_path))

# Parent fill for subnationals --------------------------------------------
# using the special st-gpr functions 
loc_data <- get_location_hierarchy(35) %>% 
  select(location_id, level, level_3)

# merge location data onto dts
# drops some locations that we don't need 
test_merge  <- merge(test, loc_data,  by = "location_id")
treat_merge <- merge(treat, loc_data, by = "location_id")

# drop subnationals
test_merge  <- test_merge[level <= 3, ]
treat_merge <- treat_merge[level <= 3, ]

# merge back on all locs, parent-filling subnationals
test_filled <- left_join(test_merge, loc_data, by = c("level_3")) %>% 
  select(location_id = location_id.y, year_id, draw_num, draw)

treat_filled <- left_join(treat_merge, loc_data, by = c("level_3")) %>% 
  select(location_id = location_id.y, year_id, draw_num, draw)


# Save csvs by location ---------------------------------------------------
#locations <- loc_data$location_id
test_filled  <- as.data.table(test_filled)
treat_filled <- as.data.table(treat_filled)

# specify a datatable (dt), location (vectorizable), and an out_dir. Verbose is optional
# if you want live progress on which locations have been saved
save_location_subset <- function(dt, location, out_dir, root_name, verbose = TRUE) {
  if (verbose) {
    message(paste0("Saving for location id ", location))
  }
  
  location_subset <- dt[location_id == location]
  readr::write_csv(location_subset, paste0(out_dir, root_name, location, ".csv"))
}

# save test data first
message(paste(Sys.time(), "Save syphilis testing data"))
parallel::mclapply(locations, function(loc) save_location_subset(dt = test_filled, location = loc, 
                                                               out_dir = paste0(out_dir, "FILEPATH"), 
                                                               root_name = "test_", verbose  = T))

# now its time for treatment data!
message(paste(Sys.time(), "Save syphilis treatment data"))
parallel::mclapply(forecast_locs, function(loc) save_location_subset(dt = treat, location = loc, 
                                                               out_dir = paste0(out_dir, "FILEPATH"), 
                                                               root_name = "treat_", verbose  = T))







