# this is a function to find failed locations from the Python step 04a

solver <- "prep" # prep or run
source("/filepath/get_location_metadata.R")
functional <- "meningitis"

# generate locations
loc.meta <- get_location_metadata(location_set_id = 9, gbd_round_id = 7)
locations <- loc.meta[most_detailed == 1 & is_estimate == 1, unique(location_id)]

if (solver == "prep") {
  sexes <- c("1","2")
  years <- c ("1990", "1995", "2000", "2005", "2010", "2015", "2019", "2020", "2021", "2022")
  locations_sex_year <- list(locations, sexes, years)
  lsy_grid <- expand.grid(locations_sex_year)
  lsy_vector <- c()
  for (i in 1:nrow(lsy_grid)) {
    lsy_vector[i] <- paste0(lsy_grid[i,1],"_",lsy_grid[i,2],"_",lsy_grid[i,3])
  }
  lsy_vector <- sort(lsy_vector)
  check_directory <- "filepath"
  checks <- sort(list.files(check_directory))
  checks <- checks[1:length(checks)-1]
  failed_locs <- setdiff(locations,checks)
  for (location in checks){
    year_check <- list.files(file.path(check_directory, location))
    if (!identical(year_check, years)) failed_locs <- c(failed_locs, location)
    for (year in year_check){
      sex_check <- list.files(file.path(check_directory, location, year))
      if (!identical(sex_check, sexes)) failed_locs <- c(failed_locs, location)
      for (sex in sex_check){
        file_check <- list.files(file.path(check_directory, location, year, sex))
        if (length(file_check)<2) failed_locs <- c(failed_locs, location)
      }
    }
  }

  print_failed_locs <- paste(as.numeric(unique(failed_locs)), collapse = ", ")
  
} else if (solver == "run") {
  checks <- list.files("filepath")
  outcomes <- c ("epilepsy", "long_modsev")
  locations_sex_year <- list(outcomes, locations)
  lsy_grid <- expand.grid(locations_sex_year)
  lsy_vector <- c()
  for (i in 1:nrow(lsy_grid)) {
    lsy_vector[i] <- paste0(lsy_grid[i,1],"_",lsy_grid[i,2])
  }
  lsy_vector <- sort(lsy_vector)
  finished_locs <- c()
  for (x in checks) {
    finished_locs <- c(finished_locs, substr(x, 23, nchar(x) - 4))
  }
  failed_locs <- setdiff(lsy_vector, finished_locs); failed_locs 
}
