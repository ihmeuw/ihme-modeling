##########################################################################
### Author: USERNAME
### Date: 07/22/2019
### Project: GBD Nonfatal Estimation
### Purpose: Master Script for Calculating Envelope Proportions and New Dementia Prevalence
##########################################################################

rm(list=ls())

if (Sys.info()["sysname"] == "Linux") {
  j_root <- "FILEPATH" 
  h_root <- "FILEPATH"
  l_root <- "FILEPATH"
  functions_dir <- "FILEPATH"
} else { 
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
  l_root <- "FILEPATH"
  functions_dir <- "FILEPATH"
}

pacman::p_load(data.table, openxlsx, ggplot2)
library(mortcore, lib = "FILEPATH")
date <- gsub("-", "_", Sys.Date())

# SET UP OBJECTS ----------------------------------------------------------

code_dir <- paste0(h_root, "FILEPATH")
functions_dir <- paste0("FILEPATH")
save_dir <- paste0("FILEPATH", date)
dir.create(save_dir)
save_dir <- paste0(save_dir, "/")
dir.create(paste0(save_dir, "FILEPATH"))
dir.create(paste0(save_dir, "FILEPATH"))
dir.create(paste0(save_dir, "FILEPATH"))
dir.create(paste0(save_dir, "FILEPATH"))
dir.create(paste0(save_dir, "FILEPATH"))

# SOURCE FUNCTIONS --------------------------------------------------------

source(paste0(functions_dir, "get_location_metadata.R"))

# GET LOCATIONS -----------------------------------------------------------

loc_dt <- get_location_metadata(location_set_id = 35)
loc_dt <- loc_dt[(is_estimate == 1 & most_detailed == 1)]
params <- data.table(location = loc_dt[, location_id])
params[, task_num := 1:.N]
map_path <- paste0("FILEPATH", "task_map.csv")
write.csv(params, "FILEPATH", row.names = F)

# SUBMIT JOBS -------------------------------------------------------------

array_qsub(jobname = "adjust_dementia_prev",
           shell = "ADDRESS",
           code = paste0(code_dir, "01_correct_prevalence.R"),
           pass = list(map_path, save_dir),
           proj = "PROJECT",
           num_tasks = nrow(params), archive_node = T,
           cores = 4, mem = 5, log = T, submit = T)



assertable::check_files(paste0(params$location, ".csv"), paste0("FILEPATH"))