
rm(list=ls())
os <- .Platform$OS.type

arg <- commandArgs(trailingOnly = TRUE)
cause <- arg[1]

#############################################

source(paste0("FILEPATH/save_results_cod.R"))

#############################################

save_results_cod(cause_id = cause,
                 input_dir = paste0("FILEPATH", cause),
                 input_file_pattern = "{sex_id}/{location_id}_{year_id}_{sex_id}.csv",
                 description = ("Final CODCorrect Upload"),
                 gbd_round_id = 5)
                     