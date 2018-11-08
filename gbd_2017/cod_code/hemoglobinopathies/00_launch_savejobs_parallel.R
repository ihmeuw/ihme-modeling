rm(list=ls())
os <- .Platform$OS.type


## QSUB
slot_number <- 2
next_script <- paste0("FILEPATH/01_saveresults.R")


##loop args
cause_list <- c(614, 615, 616, 618)

for (cause in cause_list){
  job_name <- paste0("save_hemog_", cause)
  
  system(paste( "qsub -P proj_custom_models -pe multi_slot", slot_number, "-N", job_name, "-e FILEPATH", "-o FILEPATH", "FILEPATH/r_shell_codem.sh", next_script, cause))
}