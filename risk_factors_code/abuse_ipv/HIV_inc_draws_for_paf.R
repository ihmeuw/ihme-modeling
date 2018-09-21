###############################################################################
# Description: Prepare incidence draws for HIV PAF code
###############################################################################

### Setup
rm(list=ls())
gc()
library(data.table)
library(parallel)

## get location list with lowest GBD level locations
source("FILEPATH/get_location_metadata.R")
loc.table <- get_location_metadata(location_set_id=22)
loc.table <- loc.table[loc.table$is_estimate == 1 & loc.table$most_detailed == 1,] 

### Code
file.list <- list.files("FILEPATH")
file.list.out <- list.files("FILEPATH")
loc.list <- gsub("FILENAME_SUFFIX", "", file.list)
for (loc in loc.list) {
  loc.id <- loc.table[ihme_loc_id == loc, location_id]
  filename <- paste0(loc.id, "_hiv_incidence_draws.csv")
  if (filename %in% file.list.out){
    print(paste0(filename," already done"))
  } else {
    print(loc)
    ##loc.id <- loc.table[ihme_loc_id == loc, location_id]
    in.path <- paste0(in.dir, loc, suffix)
    data.id <- fread(in.path)
    data.id[, location_id := loc.id]
    data.id[, run_num := paste0("inc", run_num)]
    cast.data <- dcast.data.table(data.id, location_id + year_id + age_group_id + sex_id ~ run_num, value.var="new_hiv")
    output.path <- paste0(output.dir, loc.id, "_hiv_incidence_draws.csv")
    write.csv(cast.data, file="FILEPATH", row.names=F)
  }
}

### End