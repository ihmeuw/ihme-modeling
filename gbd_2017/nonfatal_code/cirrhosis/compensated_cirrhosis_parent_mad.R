#######################################################################################################################
## Author: 
## Description: Generate draws for compensated cirrhosis ID from total cirrhosis ID and decompensated cirrhosis ID
## Output: compensated cirrhosis draws 
## Notes: New modeling strategy introduced in 2016 (total cirrhosis - decompensated cirrhosis = compensated cirrhosis)
## Notes: First of two files (sends qsub to run 02_cirrhosis_parallel.R)
#######################################################################################################################

rm(list=ls())

## Working environment ----
os <- .Platform$OS.type
  lib_path <- "FILEPATH"
  lib_path <- "FILEPATH"
  j<- "FILEPATH"
  h<- "FILEPATH"
  shell <- "FILEPATH"
  scratch <- "FILEPATH"

## Create filepaths ----
date <- file.path(scratch, format(Sys.Date(), format = "%Y_%m_%d"))
#unlink(file.path(date, recursive = T)) # deletes folders and contents from a previous run on the same day

dir.create(date, showWarnings = F)

dir.create(file.path(date, "01_draws"), showWarnings = F)
draws <- file.path(date, "01_draws")

dir.create(file.path(draws, "00_logs"), showWarnings = F)
logs <- file.path(draws, "00_logs")

dir.create(file.path(draws, "asymptomatic"), showWarnings = F)

## Use shared functions ----
source(paste0(j, "FILEPATH"))
epi_locations <- get_location_metadata(location_set_id=9) # 9 = epi nonfatal estimation
location_ids <- epi_locations[is_estimate==1 & most_detailed==1,]
location_ids <- location_ids[,location_id]

## Submit qsub ----
for(location in location_ids) {
  command <- paste0("FILEPATH", "cirrhosis", " ", location, " ", draws)
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

#print("Generated compensated cirrhosis draws for ME")

#print("Will now use save_results to upload ME using save_results.")
#print("After that will apply the five proportion models and use save_results to upload the resulting models.")

## Submit qsub for post-processing of decompensated cirrhosis draws
command<- "qsub command"
system(command)

## Submit qsub for post-processing of compensated cirrhosis draws
command<- "qsub command" 
system(command)

print("Finished!")
# the end ----