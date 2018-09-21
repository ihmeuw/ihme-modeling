###################################################################################################################
## Author: USERNAME
## Description: Generate redistribution scalar of indeterminate colitis and apply to ulcerative colitis and crohn's disease
## Output:  scaled draws ready for custom model save_results
###################################################################################################################

## source(FILEPATH)

rm(list=ls())

##working environment 

os <- .Platform$OS.type
if (os=="windows") {
  lib_path <- FILEPATH
  j<- FILEPATH
  h<-FILEPATH
} else {
  lib_path <- FILEPATH
  j<- FILEPATH
  h<- FILEPATH
  shell <- FILEPATH
  scratch <- FILEPATH
}

## deletes folders and contents from a previous run on the same day, commented out because we dont always want to do this 
  #unlink(file.path(scratch, Sys.Date()), recursive = T) 

##turn on and off causes with 0/1 abd create a cause list

ulcerative_colitis <- 0
crohns_disease <- 1

causes<- "ulcerative_colitis"

for (cause in causes){
  if (get(cause)<1){
    causes<-causes[-which(causes==cause)]
  }
}


##draw directories

date <- FILEPATH
dir.create(FILEPATH)
dir.create(FILEPATH)
draws <- FILEPATH

##load in shared functions 

source(paste0(j, FILEPATH))

##get locations

epi_locations <- get_location_metadata(location_set_id=22)
location_ids <- epi_locations[is_estimate==1 & most_detailed==1,]
location_ids <- location_ids[,location_id]


#############################################################################################################################################
##code starts here
#############################################################################################################################################


##qsub and send jobs for each cause, location 

for(cause in causes) { 
  
  dir.create(FILEPATH)
  cause_draws <- FILEPATH
  dir.create(FILEPATH)
  logs <- FILEPATH
  
  for(location in location_ids) {
    
    command <- paste0("qsub -pe multi_slot 1 -P proj_custom_models -l mem_free=4g -o ",logs," -e ",logs, " -N ", paste0(cause,"_", location, " "), shell, " ", "FILEPATH", cause, " ", location, " ", cause_draws)
    system(command)
    
  }
}

## job counter 

for (cause in causes) {
  expected_jobs <- length(location_ids)
  completed_jobs <- length(FILEPATH)))
  while(expected_jobs > completed_jobs) {
    print(paste0(completed_jobs, " of ", expected_jobs, " ", cause, " jobs completed...", Sys.time()))
    Sys.sleep(15)
    completed_jobs <- length(FILEPATH)))
  } 
}


