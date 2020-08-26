##########################################################################
### Author: USERNAME
### Date: 03/06/2019
### Project: GBD Nonfatal Estimation
### Purpose: Save Adjusted Prevalence
##########################################################################

rm(list=ls())

if (Sys.info()["sysname"] == "Linux") {
  j_root <- "FILEPATH" 
  h_root <- "FILEPATH"
  l_root <- "FILEPATH"
} else { 
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
  l_root <- "FILEPATH"
}

pacman::p_load(data.table, openxlsx, ggplot2)
library(mortcore, lib = "FILEPATH")
date <- gsub("-", "_", Sys.Date())
save_meid <- ID

# SET UP OBJECTS ----------------------------------------------------------

code_dir <- paste0("FILEPATH")
functions_dir <- paste0("FILEPATH")
save_dir <- paste0("FILEPATH", date, "/FILEPATH/")

# SOURCE FUNCTIONS --------------------------------------------------------

source(paste0(functions_dir, "save_results_epi.R"))

# Call Function -----------------------------------------------------------

save_results_epi(input_dir = save_dir, input_file_patter = '{location_id}.csv', 
                 modelable_entity_id = save_meid, description = paste0("results without predicted EMR (Other outliers) ", date),
                 measure_id = 5:6, decomp_step = 'step4', mark_best = T)