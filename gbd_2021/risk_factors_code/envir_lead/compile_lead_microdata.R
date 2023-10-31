############################################################################################################
## Project: RF: Lead Exposure
## Purpose: Compile lead microdata for distribution modeling
############################################################################################################

# clear memory
rm(list=ls())
library(data.table)
library(magrittr)
library(haven)
library(ggplot2)

# runtime configuration
if (Sys.info()["sysname"] == "Linux") {
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
} else {
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
}

files <- list.files(file.path(j_root,"FILEPATH/exp_microdata_gbd2020"), full.names = TRUE)

# pull location metadata
source("FILEPATH/get_location_metadata.R")
locations <- get_location_metadata(gbd_round_id = 7, location_set_id = 22)
locs <- locations[,list(location_id,ihme_loc_id,region_id,super_region_id)]

read_file <- function(filepath) {
  if (filepath %like% ".dta") {
    df <- data.table(read_dta(filepath))
  } else if (filepath %like% ".csv") {
    df <- fread(filepath)
  }
  
  if (unique(df$ihme_loc_id == "IND")) df[,ihme_loc_id:= admin_1_id]
  df[, year_id := floor((year_start + year_end)/2)]
  df <- df[!is.na(bll) & bll != 8888, .(nid, ihme_loc_id, year_id, age_year, sex_id, bll)]
  
  return(df)
}

data <- rbindlist(lapply(files, read_file), fill = TRUE)
data <- merge(locs, data, by = "ihme_loc_id")
setnames(data, "bll", "data")

write.csv(data[, .(nid, location_id, year_id, age_year, sex_id, data)],
          "FILEPATH/compiled_lead_microdata.csv", row.names = FALSE)

## END