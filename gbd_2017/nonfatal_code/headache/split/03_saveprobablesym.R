##########################################################################
## Author: 
## Date: May 8th
## Purpose: Save Results Probable Symptomatic Headache
##########################################################################

## LOAD LIBRARIES
library(pacman, lib.loc = FILEPATH)
pacman::p_load(data.table, magrittr, readr)

## SET OBJECTS
functions_dir <- FILEPATH
headache_dir <- FILEPATH
file_dir <- paste0(headache_dir, "probable_sym/")
date <- gsub("-", "_", Sys.Date())
results_dir <- paste0(file_dir, "/", date, "/")
meid <- 20183
file_pattern <- "{location_id}.csv"
description <- paste0("Probable symptomatic headache, split using global severity split on ", date)

## SOURCE FUNCTIONS
source(paste0(functions_dir, "save_results_epi.R"))

## SAVE RESULTS
save_results_epi(modelable_entity_id = meid, input_dir = results_dir, input_file_pattern = file_pattern, 
                 description = description, mark_best = T)