##########################################################################
## Author: 
## Date: December 4th, 2017
## Purpose: Save Results Treated No Fits Epilepsy
##########################################################################

## LOAD LIBRARIES
library(pacman, lib.loc = FILEPATH)
pacman::p_load(data.table, magrittr, readr)

## SET OBJECTS
functions_dir <- FILEPATH
epilepsy_dir <- FILEPATH
tnf_dir <- paste0(epilepsy_dir, "tnf/")
date <- gsub("-", "_", Sys.Date())
results_dir <- paste0(tnf_dir, "/", date, "/")
meid <- 1951
file_pattern <- "{location_id}.csv"
description <- paste0("Treated no fits epilepsy, split from envelope using regression results on ", date)

## SOURCE FUNCTIONS
source(paste0(functions_dir, "save_results_epi.R"))

## SAVE RESULTS
save_results_epi(modelable_entity_id = meid, input_dir = results_dir, input_file_pattern = file_pattern, 
                 description = description, mark_best = T)