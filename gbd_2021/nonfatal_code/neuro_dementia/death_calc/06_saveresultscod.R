##########################################################################
### Author: USERNAME
### Project: GBD Nonfatal Estimation
### Purpose: Save Custom COD Results to COD
##########################################################################

rm(list=ls())

if(Sys.info()["sysname"] == "Linux") {
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
date <- "2020_05_17" #final step2 2020

# SET OBJECTS -------------------------------------------------------------

functions_dir <- paste0(functions_dir, "FILEPATH")
save_dir <- paste0("FILEPATH")
cid <- ID

# SOURCE FUNCTIONS --------------------------------------------------------

functs <- c("save_results_cod.R")
invisible(lapply(functs, function(x) source(paste0(functions_dir, x))))

# RUN SAVE RESULTS --------------------------------------------------------

save_results_cod(input_dir = save_dir, input_file_pattern = "{location_id}.csv", 
                 cause_id = cid, description = "Custom Step3 Results Dementia Mortality - same as Step 2 2020 - updated for envelope",
                 metric_id = 3, decomp_step = "step3", gbd_round_id=7, mark_best = T)

