##########################################################################
## Purpose: Save Results Definite Symptomatic Headache
##########################################################################

## LOAD LIBRARIES
library(pacman, lib.loc = "FILEPATH")
pacman::p_load(data.table, magrittr, readr)

## SET OBJECTS
file_dir <- paste0("FILEPATH", "definite_sym/")
date <- gsub("-", "_", Sys.Date())

date <- "date"

results_dir <- paste0(file_dir, "/", date, "/")
meid <- 1
file_pattern <- "{location_id}.csv"
description <- paste0("Definite symptomatic headache, split using global severity split on ", date)
step <- "step4"

## SOURCE FUNCTIONS
source(paste0("FILEPATH", "save_results_epi.R"))

## SAVE RESULTS
save_results_epi(modelable_entity_id = meid, input_dir = results_dir, input_file_pattern = file_pattern, 
                 description = description, mark_best = T, decomp_step = step)