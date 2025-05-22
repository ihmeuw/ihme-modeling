##########################################################################
### Author: USERNAME
### Date: 03/06/2019
### Project: GBD Nonfatal Estimation
### Purpose: Save Adjusted Prevalence
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
save_meid <- ID #pre- or post-mortality

# SET UP OBJECTS ----------------------------------------------------------

code_dir <- paste0("FILEPATH")
functions_dir <- "FILEPATH"
save_dir <- paste0("FILEPATH")

gas# SOURCE FUNCTIONS --------------------------------------------------------

source(paste0(functions_dir, "save_results_epi.R"))

# Call Function -----------------------------------------------------------

save_results_epi(input_dir = save_dir, input_file_patter = '{location_id}.csv', 
                 modelable_entity_id = save_meid, description = paste0("2019 best subtractions - outliers for non-fatal same as fatal - incidence + emr included, ", date),
                 measure_id = ID, decomp_step = 'iterative', gbd_round_id=ID, crosswalk_version_id = ID, bundle_id=ID, mark_best = T)


