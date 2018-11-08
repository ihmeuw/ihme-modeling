##########################################################################
## Author: 
## Date: December 4th, 2017
## Purpose: Save Results Severe Epilepsy
##########################################################################

## LOAD LIBRARIES
library(pacman, lib.loc = FILEPATH)
pacman::p_load(data.table, magrittr, readr)

## SET OBJECTS
functions_dir <- FILEPATH
epilepsy_dir <- FILEPATH
severe_dir <- paste0(epilepsy_dir, "severe/")
date <- gsub("-", "_", Sys.Date())
results_dir <- paste0(severe_dir, "/", date, "/")
meid <- 1953
file_pattern <- "{location_id}.csv"
description <- paste0("Severe epilepsy, split from envelope using regression results on ", date)

## SOURCE FUNCTIONS
source(paste0(functions_dir, "save_results_epi.R"))

## SAVE RESULTS
save_results_epi(modelable_entity_id = meid, input_dir = results_dir, input_file_pattern = file_pattern, 
                 description = description, mark_best = T)