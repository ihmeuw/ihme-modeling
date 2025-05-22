##########################################################################
### Author: USERNAME
### Project: GBD Nonfatal Estimation
### Purpose: Save Custom COD Results to MEID
##########################################################################

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
date <- gsub("-", "_", Sys.Date())

# SET OBJECTS -------------------------------------------------------------

functions_dir <- paste0(functions_dir, "FILEPATH")
save_dir <- paste0("FILEPATH", date, "/rate/")
meid <- ID

# SOURCE FUNCTIONS --------------------------------------------------------

functs <- c("save_results_epi.R")
invisible(lapply(functs, function(x) source(paste0(functions_dir, x))))

# RUN SAVE RESULTS --------------------------------------------------------

save_results_epi(input_dir = save_dir, input_file_pattern = "{location_id}.csv", 
                 modelable_entity_id = meid, description = "COD Results Estimation Years - after 2020 update",
                 measure_id = 6, decomp_step = "iterative", bundle_id=ID, 
                 crosswalk_version_id = ID, gbd_round_id=7, mark_best = T)

## SAVING AS MEASURE 6 SO THAT WE CAN INTERPOLATE
