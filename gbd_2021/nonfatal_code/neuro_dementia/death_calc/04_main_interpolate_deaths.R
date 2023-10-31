###########################################################################################################################
### Author: USERNAME - modified by USERNAME for GBD 2020
### Project: GBD Nonfatal Estimation
### Purpose: Main Script for Interpolating Deaths - interpolate to 2022 from GBD 2019 best, collapse Norway subnationals
##########################################################################################################################

rm(list=ls())

if (Sys.info()["sysname"] == "Linux") {
  j <- "FILEPATH" 
  h <- "FILEPATH"
  l <- "FILEPATH"
  functions_dir <- "FILEPATH"
} else { 
  j <- "FILEPATH"
  h <- "FILEPATH"
  l <- "FILEPATH"
  functions_dir <- "FILEPATH"
}

pacman::p_load(data.table, openxlsx, ggplot2)
library(mortcore, lib = "FILEPATH")
date <- gsub("-", "_", Sys.Date())

# SET UP OBJECTS ----------------------------------------------------------

code_dir <- paste0("FILEPATH")
functions_dir <- paste0(functions_dir, "FILEPATH")
save_dir <- paste0("FILEPATH", date)
dir.create(paste0(save_dir, "FILEPATH"))
save_dir <- paste0(save_dir, "FILEPATH")

# SOURCE FUNCTIONS --------------------------------------------------------

source(paste0(functions_dir, "get_location_metadata.R"))

# GET LOCATIONS -----------------------------------------------------------

loc_dt <- get_location_metadata(location_set_id = 35, gbd_round_id=7)
loc_dt <- loc_dt[(is_estimate == 1 & most_detailed == 1)]
params <- data.table(location = loc_dt[, location_id])
params[, task_num := 1:.N]
map_path <- paste0(save_dir, "task_map.csv")
write.csv(params, map_path, row.names = F)

# SUBMIT JOBS -------------------------------------------------------------

array_qsub(jobname = "interpolate_deaths",
           shell = "FILEPATH",
           code = paste0(code_dir, "05_child_interpolate_deaths.R"),
           pass = list(date),
           proj = "NAME",
           num_tasks = nrow(params), archive_node = T,
           cores = 10, mem = 5, log = T, submit = T)


assertable::check_files(paste0(params$location, ".csv"), paste0(save_dir, "rate/"))
