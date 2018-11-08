##########################################################################
## Author: 
## Date: April 10th, 2017
## Purpose: Master Script to Submit Headache Squeeze
##########################################################################

rm(list = ls())
library(pacman, lib.loc = FILEPATH)
pacman::p_load(data.table, magrittr, readr)

## SET OBJECTS
user <- Sys.info()[['user']]
proj <- PROJ
epilepsy_dir <- FILEPATH
repo_dir <- FILEPATH
shell_path <- paste0(epilepsy_dir, "r_shell.sh")
date <- gsub("-", "_", Sys.Date())

## SOURCE FUNCTIONS
source(paste0(repo_dir, "job_array.R"))
source(FILEPATH)

## USER FUNCTIONS
job_hold <- function(job_name) {
  Sys.sleep(5)
  start.time <- proc.time()
  flag <- 0
  while (flag == 0) {
    if (system(paste0("qstat -r | grep ", job_name, "|wc -l"), intern = T) == 0) {
      flag <- 1
    } else {
      Sys.sleep(60)
    }
  }
  job.runtime <- proc.time() - start.time
  job.runtime <- job.runtime[3]
  Sys.sleep(10)
  print(paste0("Job ", job_name, " has completed. Time elapsed: ", job.runtime))
}

## GET PARAMS
message("getting params")
locs <- get_location_metadata(location_set_id = 9)
params <- list(location_id = unique(locs$location_id))

## SUBMIT JOBS
tries <- 3
while(tries != 0 & nrow(as.data.table(params)) != 0){
  params <- as.list(params)
  job.array.master(tester = F, paramlist = params, username = user, project = proj, 
                   jobname = "headache_squeeze",
                   childscript = paste0(repo_dir, "01_childsqueeze.R"), 
                   shell = shell_path)
  job_hold("headache_squeeze")
  
  Sys.sleep(60)
  finished <- NULL
  params <- as.data.table(params)
  message("Checking to make sure all output files exist...")
  for (i in 1:nrow(params)){
    if (file.exists(paste0(FILEPATH)) &
        file.exists(paste0(FILEPATH)) &
        file.exists(paste0(FILEPATH))){
      finished <- c(finished, i)
    }
  }
  
  if (!is.null(finished)) {
    params <- params[-finished, ]
  } else {
    params <- NULL
  }
  message(print(params))
  tries <- tries - 1
}

if (nrow(params) != 0) {
  stop("Failing jobs were relaunched 2x more and still did not all complete. ")
} else {
  message("Success! All jobs complete.")
}

## SAVE RESULSTS FOR IDIOPATHIC AND SECONDARY
message("saving results")
project <- "-P PROJECT "
sge_output_dir <- "-o FILEPATH -e FILEPATH "
slots <- 15
mem <- slots*2
results_shell <- FILEPATH

job_name <- paste0("-N save_results_definite")
script <- paste0(repo_dir, "02_savedefinite.R")
sys_sub <- paste0("qsub -cwd ", project, sge_output_dir, job_name, " -pe multi_slot ", slots, " -l mem_free=", mem, "G")
system(paste(sys_sub, results_shell, script))
print(paste(sys_sub, results_shell, script))

job_name <- paste0("-N save_results_probable")
script <- paste0(repo_dir, "03_saveprobable.R")
sys_sub <- paste0("qsub -cwd ", project, sge_output_dir, job_name, " -pe multi_slot ", slots, " -l mem_free=", mem, "G")
system(paste(sys_sub, results_shell, script))
print(paste(sys_sub, results_shell, script))

## DIAGNOSTIC PLOTS
source(paste0(repo_dir, "squeeze_diagnostics.R"))