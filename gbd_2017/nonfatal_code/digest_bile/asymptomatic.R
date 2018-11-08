###################################################################################################################
## Author: 
## Description: parent file to calculate asymptomatic gallbladder draws
## Output: asymptomatic gallbladder draw files ready for save results 
###################################################################################################################


rm(list=ls())

##working environment 
os <- .Platform$OS.type
if (os=="windows") {
  j<- "J:/"
  h<-"H:/"
} else {
  j<- "/home/j/"
  h<-"/homes/USERNAME/"
  shell <- FILEPATH
  scratch <- FILEPATH
}

functions_dir <- paste0(j, FILEPATH)

##draw directories
date <- file.path(scratch, format(Sys.Date(), format = "%Y_%m_%d"))
dir.create((date), showWarnings = T) 
dir.create(file.path(date, "01_draws"), showWarnings = T)
draws <- file.path(date, "01_draws")

##load in shared functions 
source(paste0(functions_dir, "get_location_metadata.R"))
source(paste0(functions_dir, "get_demographics.R"))

##get locations
epi_locations <- get_location_metadata(location_set_id=9)
location_ids <- epi_locations[is_estimate==1 & most_detailed==1,]
location_ids <- location_ids[,location_id]


##########################################################################################################
##parent code begins / send jobs on qsub
##########################################################################################################

##create cause list

causes<-list("bile") 


##create directories for draws and qsub jobs on cluster 
for(cause in causes) { 
  
  dir.create(file.path(draws, cause), showWarnings = T)
  cause_draws <- file.path(draws, cause) 
  dir.create(file.path(cause_draws, "00_logs"), showWarnings = T) 
  logs <- file.path(cause_draws, "00_logs") 
  
  for(location in location_ids) {
    
    command <- paste0("qsub -pe multi_slot 2 -P proj_custom_models -l mem_free=4g -o ",logs," -e ",logs, " -N ", paste0(cause,"_", location, " "), shell, " ", FILEPATH, cause, " ", location, " ", cause_draws)
    system(command)
    
  }
}

##job counter
if (cause=="bile") {
for (cause in causes) {
  expected_jobs <- length(location_ids)
  completed_jobs <- length(list.files(path=file.path(draws, cause, "asymptomatic")))
  while(expected_jobs > completed_jobs) {
    print(paste0(completed_jobs, " of ", expected_jobs, " ", cause, " jobs completed...", Sys.time()))
    Sys.sleep(60)
    completed_jobs <- length(list.files(path=file.path(draws, cause, "asymptomatic")))
  }
}
}

