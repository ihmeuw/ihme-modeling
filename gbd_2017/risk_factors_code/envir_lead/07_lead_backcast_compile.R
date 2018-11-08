# Project: RF: Lead Exposure
# Purpose: Compile backcasted lead exposures

#----CONFIG-------------------------------------------------------------------------------------------------------------
# clear memory
rm(list=ls())

# load packages, install if missing
#pacman::p_load(data.table, magrittr)
library(data.table)
library(magrittr)
library(parallel)

# Directories
output.version <- 1
run_id <- 43253
exp_type <- "blood" # "bone" "blood"

input_dir <- file.path("FILEPATH")
output_dir <- file.path("FILEPATH")
dir.create(output_dir,recursive = T, showWarnings = FALSE)

#**********************************************************************************************
#compile
source(file.path("FILEPATH"))
locations <- get_location_metadata(gbd_round_id = 5, location_set_id = 22)
locs <- locations[level > 2,location_id]

for (loc in locs) {
  files <- list.files(input_dir,pattern = paste0("^",loc,"_"))
  df <- rbindlist(mclapply(file.path(input_dir,files),fread,mc.cores=10))
  df <- dcast(df, measure_id + location_id + year_id + sex_id + age_group_id ~ variable)
  write.csv(df,file.path(output_dir,paste0(loc,".csv")),row.names = F)
}
