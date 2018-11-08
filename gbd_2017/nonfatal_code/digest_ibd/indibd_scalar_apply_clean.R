###################################################################################################################
## Author:
## Based on: ic_scalar_apply.R
## Description: Generate redistribution scalar of indeterminate colitis and apply to ulcerative colitis and crohn's disease
## Output:  scaled draws ready for custom model save_results
## Notes: original stata script written by 
###################################################################################################################

## source("FILEPATH")

rm(list=ls())

##working environment 

os <- .Platform$OS.type
if (os=="windows") {
  lib_path <- "FILEPATH"
  j<- "J:/"
  h<-"H:/"
} else {
  lib_path <- "FILEPATH"
  j<- "FILEPATH"
  h<-"FILEPATH"
  shell <- file.path(h, "code/r_shell.sh")
  scratch <- "FILEPATH"
}

##turn on and off causes with 0/1 abd create a cause list

ulcerative_colitis <- 1
crohns_disease <- 1

causes<-list("ulcerative_colitis", "crohns_disease")

for (cause in causes){
  if (get(cause)<1){
    causes<-causes[-which(causes==cause)]
  }
}


##draw directories

date <- file.path(scratch, format(Sys.Date(), format = "%Y_%m_%d"))
dir.create(date, showWarnings = F)
dir.create(file.path(date, "01_draws"), showWarnings = F)
draws <- file.path(date, "01_draws")

##load in shared functions 

source(paste0(j, "FILEPATH"))

##get locations

epi_locations <- get_location_metadata(location_set_id=9)
location_ids <- epi_locations[is_estimate==1 & most_detailed==1,]
location_ids <- location_ids[,location_id]

##qsub and send jobs for each cause, location 

for(cause in causes) { 
  
  dir.create(file.path(draws, cause), showWarnings = F)
  cause_draws <- file.path(draws, cause)
  dir.create(file.path(cause_draws, "00_logs"), showWarnings = F)
  logs <- file.path(cause_draws, "00_logs")
  
  for(location in location_ids) {
    
    ##on prod cluster
    command <- paste0("qsub -pe multi_slot 1 -P proj_custom_models -l mem_free=4g -o ",logs," -e ",logs, " -N ", paste0(cause,"_", location, " "), shell, " ", "FILEPATH", cause, " ", location, " ", cause_draws)
    system(command)
    
  }
}

## job counter 
for (cause in causes) {
  expected_jobs <- length(location_ids)
  completed_jobs <- length(list.files(path=file.path(draws, cause, "adjusted_draws")))
  while(expected_jobs > completed_jobs) {
    print(paste0(completed_jobs, " of ", expected_jobs, " ", cause, " jobs completed...", Sys.time()))
    Sys.sleep(15)
    completed_jobs <- length(list.files(path=file.path(draws, cause, "adjusted_draws")))
  } 
}


