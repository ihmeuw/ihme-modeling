###################################################################################################################
## Author: USERNAME
## Description: calculate asymptomatic draws and prorportion estimates for symptomatic and asymptomatic 
## Output:  asymptomatic draws, proportion of symptomatic draws, proportion of asymptomatic draws, impairment-digestive sequela draws  
## Notes: currently done for PUD, gastritis, hernia, gallbladder -- needs to incorporate pancreatitis in GBD 2017
###################################################################################################################

## source("FILEPATH")

rm(list=ls())

##working environment 

os <- .Platform$OS.type
if (os=="windows") {
  lib_path <- #FILEPATH 
  j<- #FILEPATH 
  h<- #FILEPATH 
} else {
  lib_path <- #FILEPATH 
  j<- #FILEPATH 
  h<- #FILEPATH 
  shell <- #FILEPATH 
  scratch <- #FILEPATH 
}

## delete folders and contents from a previous run on the same day, commented out because we dont always want to do this 
  #unlink(file.path(scratch, Sys.Date()), recursive = T) 

##draw directories

date <- #FILEPATH 
dir.create((date), showWarnings = F)
dir.create(file.path(date, "01_draws"), showWarnings = F)
draws <- file.path(date, "01_draws")

##turn on and off causes with 0/1

pud <- 0
gastritis <- 0
bile <- 1
hernia <- 1

##load in shared functions 

source(paste0(j, #FILEPATH ))

##get locations


epi_locations <- get_location_metadata(location_set_id=22)
location_ids <- epi_locations[is_estimate==1 & most_detailed==1,]
location_ids <- location_ids[,location_id]



##########################################################################################################
##parent code begins / send jobs on qsub
##########################################################################################################

##create cause list

causes<-list("pud","gastritis","hernia","bile")

for (cause in causes){
  if (get(cause)<1){
    causes<-causes[-which(causes==cause)]
  }
}


##create directories for draws and qsub jobs on cluster 

for(cause in causes) { 
  
  dir.create #FILEPATH 
  cause_draws <- #FILEPATH 
  dir.create #FILEPATH 
  logs <- #FILEPATH 
  
  for(location in location_ids) {
    
    command <- paste0("qsub -pe multi_slot 2 -P proj_custom_models -l mem_free=4g -o ",logs," -e ",logs, " -N ", paste0(cause,"_", location, " "), shell, " ", "FILEPATH", cause, " ", location, " ", cause_draws)
    system(command)
    
  }
}

##job counter

if (cause=="bile" | cause=="hernia") {
for (cause in causes) {
  expected_jobs <- length(location_ids)
  completed_jobs <- length(list.files(FILEPATH)))
  while(expected_jobs > completed_jobs) {
    print(paste0(completed_jobs, " of ", expected_jobs, " ", cause, " jobs completed...", Sys.time()))
    Sys.sleep(15)
    completed_jobs <- length(list.files(FILEPATH)))
   }
  } 
} else {
  for (cause in causes) {
      expected_jobs <- length(location_ids)
      completed_jobs <- length(list.files(FILEPATH))))
      while(expected_jobs > completed_jobs) {
        print(paste0(completed_jobs, " of ", expected_jobs, " ", cause, " jobs completed...", Sys.time()))
        Sys.sleep(15)
        completed_jobs <- length(list.files(FILEPATH))))
    }
  }
}





