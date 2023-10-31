##########################################################################
## Purpose: Master Script to Submit Headache Squeeze
##########################################################################

rm(list = ls())
library(pacman, lib.loc = "FILEPATH")
pacman::p_load(data.table, magrittr, readr)

## SET OBJECTS
user <- Sys.info()[['user']]
proj <- "USERNAME"
date <- gsub("-", "_", Sys.Date())

## SOURCE FUNCTIONS
library(mortcore, lib = "FILEPATH")
source("FILEPATH")

## GET LOCATIONS
loc_dt <- get_location_metadata(location_set_id = 35)
loc_dt <- loc_dt[is_estimate == 1 & most_detailed == 1]
params <- data.table(location = loc_dt[, location_id])
params[, task_num := 1:.N]
save_dir <- "FILEPATH"
map_path <- "FILEPATH"
write.csv(params, map_path, row.names = F)

## SUBMIT JOBS
array_qsub(jobname = "headache_squeeze", 
           shell = "FILEPATH", 
           code = paste0("FILEPATH", "01_childsqueeze.R"),
           pass = list(map_path, save_dir),
           proj = "USERNAME",
           num_tasks = nrow(params),
           cores = 4, mem = 8, log = T, submit = T)

message("Checking to make sure all output files exist...")
finished <- NULL
for (i in 1:nrow(params)){
  if (file.exists("FILEPATH") &
      file.exists("FILEPATH") &
      file.exists("FILEPATH"){
    finished <- c(finished, i)
  }
}

if (!is.null(finished)) {
  params <- params[-finished, ]
} else {
  params <- NULL
}
message(print(params))

params[, task_num := 1:.N]
save_dir <- "FILEPATH"
map_path <- "FILEPATH"
write.csv(params, map_path, row.names = F)

## SAVE RESULSTS FOR IDIOPATHIC AND SECONDARY
message("saving results")
project <- "USERNAME "
sge_output_dir <- "FILEPATH"
slots <- 15
mem <- slots*2
results_shell <- "FILEPATH"

job_name <- paste0("-N save_results_definite")
script <- paste0("FILEPATH", "02_savedefinite.R")
sys_sub <- paste0("qsub -cwd ", project, sge_output_dir, job_name, " -l fthread=", slots, " -l m_mem_free=", mem, "G", " -q all.q")
system(paste(sys_sub, results_shell, script))
print(paste(sys_sub, results_shell, script))

job_name <- paste0("-N save_results_probable")
script <- paste0("FILEPATH", "03_saveprobable.R")
sys_sub <- paste0("qsub -cwd ", project, sge_output_dir, job_name, " -l fthread=", slots, " -l m_mem_free=", mem, "G", " -q all.q")
system(paste(sys_sub, results_shell, script))
print(paste(sys_sub, results_shell, script))

## DIAGNOSTIC PLOTS
source(paste0("FILEPATH", "squeeze_diagnostics.R"))
