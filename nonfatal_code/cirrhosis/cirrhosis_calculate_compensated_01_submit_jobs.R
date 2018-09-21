#######################################################################################################################
## Author: USERNAME
## Description: Generate draws for compensated cirrhosis (ME=11654) from total cirrhosis (ME=11653) and decompensated cirrhosis (ME=1919) 
## Output: compensated cirrhosis draws 
## Notes: New modeling strategy introduced in 2016 (total cirrhosis - decompensated cirrhosis = compensated cirrhosis)
## Notes: First of two files (sends qsub to cirrhosis_calculate_compensated_02_run_jobs_in_parallel.R)
#######################################################################################################################

rm(list=ls())

## Working environment ----
os <- .Platform$OS.type
  lib_path <- "FILENAME"
  j<- "FILENAME"
  h<-"FILENAME"
  shell <- "FILENAME"

## Create filepaths ----
code_dir <- "FILENAME"
scratch <- "FILENAME"

unlink(file.path(scratch, Sys.Date()), recursive = T) # deletes folders and contents from a previous run on the same day

dir.create(file.path(scratch, Sys.Date()), showWarnings = F)
date <- file.path(scratch, Sys.Date())

dir.create(file.path(date, "01_draws"), showWarnings = F)
draws <- file.path(date, "01_draws")

dir.create(file.path(draws, "00_logs"), showWarnings = F)
logs <- file.path(draws, "00_logs")

dir.create(file.path(draws, "asymptomatic"), showWarnings = F)

## Use shared functions ----
source(paste0(j, "FILENAME/get_location_metadata.R"))
epi_locations <- get_location_metadata(location_set_id=22) # 22="Covariate Computation"
location_ids <- epi_locations[is_estimate==1 & most_detailed==1,]
location_ids <- location_ids[,location_id]

## Submit qsub ----
for(location in location_ids) {
  command <- paste0("qsub -pe multi_slot 1 -P proj_custom_models -o ",logs," -e ",logs, " -N ", paste0("cirrhosis_", location, " "), shell, " ", "FILENAME/cirrhosis_calculate_compensated_02_run_jobs_in_parallel.R ", "cirrhosis", " ", location, " ", draws)
  system(command)
}

## Wait for jobs to finish ----
expectedJobs <- length(location_ids)
completedJobs <- 0
while(expectedJobs > completedJobs) {
  print(paste0(completedJobs, ' of ', expectedJobs, ' files found...', Sys.time()))
  Sys.sleep(15)
  completedJobs = length(list.files(path=file.path(draws, "asymptomatic"), pattern=paste0("*_*.csv")))
}

print("Generated compensated cirrhosis draws for ME 11654")
print("Will now use save_results to upload ME 11654 using save_results.")
print("After that will apply the four proportion models and use save_results to upload the resulting models.")

## Submit qsub for post-processing of compensated cirrhosis draws
command<-"qsub -N compensated -pe multi_slot 15 -P proj_custom_models -e FILENAME -o FILENAME FILENAME/stata_shell.sh FILEPATH/cirrhosis_non-fatal_compensated_calculate_split_save.do" 
system(command)

print("Finished!")


# the end ----


