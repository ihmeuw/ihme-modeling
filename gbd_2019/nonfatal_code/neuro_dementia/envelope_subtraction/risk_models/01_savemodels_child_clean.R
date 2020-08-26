##########################################################################
### Author: FILEPATH
### Date: 07/23/2019
### Project: GBD Nonfatal Estimation
### Purpose: SAVE ENVELOPE MODELS CHILD
##########################################################################

rm(list=ls())

if (Sys.info()["sysname"] == "Linux") {
  j_root <- "FILEPATH" 
  h_root <- "FILEPATH"
  l_root <- "FILEPATH"
  functions_dir <- "FILEPATH"
} else { 
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
  l_root <- "FILEPATH"
  functions_dir <- "FILEPATH"
}

pacman::p_load(data.table, openxlsx, ggplot2)
library(mortcore, lib = "FILEPATH")
date <- gsub("-", "_", Sys.Date())

# SET OBJECTS -------------------------------------------------------------

functions_dir <- paste0(functions_dir, "FILEPATH")
model_dt <- data.table(cause = c("ds", "stroke", "tbi", "pd"),
                       date = c("2019_07_02", "2019_06_26", "2019_07_03", "2019_07_24"),
                       me_id = c(ID, ID, ID, ID),
                       dir = c("FILEPATH", "FILEPATH", "FILEPATH", "FILEPATH"))

# GET ARGS ----------------------------------------------------------------

args<-commandArgs(trailingOnly = TRUE)
map_path <-args[1]
save_dir <-args[2]
params <- fread(map_path)
task_id <- Sys.getenv("SGE_TASK_ID")
c <- params[task_num == task_id, cause]

# SOURCE FUNCTIONS --------------------------------------------------------

functs <- c("save_results_epi")
invisible(lapply(functs, function(x) source(paste0(functions_dir, x, ".R"))))

# SAVE RESULTS ------------------------------------------------------------

save_results_epi(input_dir = paste0('FILEPATH'), input_file_patter = "{location_id}.csv",
                 modelable_entity_id = model_dt[cause == c, me_id], description = paste0("RR Model ", date),
                 measure_id = ID, decomp_step = "step3", mark_best = T)