# this is a function to find failed locations from the Python step 04a

source("current/r/get_location_metadata.R")

# pull locations from CSV created in model_custom
# locations <- readRDS("filepath")

# or generate all locations
loc.meta <- get_location_metadata(location_set_id = 9, gbd_round_id = 6)
locations <- loc.meta[most_detailed == 1 & is_estimate == 1, unique(location_id)]

sexes <- c("1","2")
years <- c ("1990", "1995", "2000", "2005", "2010", "2015", "2017", "2019")
locations_sex_year <- list(locations, sexes, years)
lsy_grid <- expand.grid(locations_sex_year)
lsy_vector <- c()
for (i in 1:nrow(lsy_grid)) {
  lsy_vector[i] <- paste0(lsy_grid[i,1],"_",lsy_grid[i,2],"_",lsy_grid[i,3])
}
lsy_vector <- sort(lsy_vector)

# date <- gsub("-", "_", Sys.Date())
date <- "2020_01_26"

checks <- list.files(paste0("filepath", date, "/draws/04a_dismod_prep_wmort/04_ODE/rate_in/checks"))
finished_locs <- c()
for (x in checks) {
  finished_locs <- c(finished_locs, substr(x, 10, nchar(x) - 4))
}
failed_locs <- setdiff(lsy_vector, finished_locs); failed_locs
# return(failed_locs)