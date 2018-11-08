##########################################################################
## Author: 
## Date: December 4th, 2017
## Purpose: Save Results Secondary Epilepsy
##########################################################################

## LOAD LIBRARIES
library(pacman, lib.loc = FILEPATH)
pacman::p_load(data.table, magrittr, readr)

## SET OBJECTS
functions_dir <- FILEPATH
epilepsy_dir <- FILEPATH
secondary_dir <- paste0(epilepsy_dir, "secondary/")
date <- gsub("-", "_", Sys.Date())
results_dir <- paste0(secondary_dir, "/", date, "/")
meid <- 3026
file_pattern <- "{location_id}.csv"
description <- paste0("Secondary epilepsy, split from envelope using regression results on ", date)

## SOURCE FUNCTIONS
source(paste0(functions_dir, "save_results_epi.R"))

## SAVE RESULTS
save_results_epi(modelable_entity_id = meid, input_dir = results_dir, input_file_pattern = file_pattern, 
                 description = description, mark_best = T)