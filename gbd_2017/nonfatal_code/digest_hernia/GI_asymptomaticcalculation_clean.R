###################################################################################################################
## Author: 
## Based on asymptomatic.R
## Description: calculate asymptomatic draws from total and symptomatic 
## Output:  asymptomatic draws 
## Notes: currently done for hernia
###################################################################################################################

## source("FILEPATH")

rm(list=ls())

##working environment 

os <- .Platform$OS.type
if (os=="windows") {
  lib_path <- "FILEPATH"
  j<- "J:/"
  h<-"H:/"
  scratch <-"H:/giasymp"
} else {
  lib_path <- "FILEPATH"
  j<- "FILEPATH"
  h<-"FILEPATH"
  shell <- "FILEPATH"
  scratch <- "FILEPATH"
}

##draw directories

date <- file.path(scratch, format(Sys.Date(), format = "%Y_%m_%d"))
dir.create((date), showWarnings = F)
dir.create(file.path(date, "draws"), showWarnings = F)
draw_files <- file.path(date, "draws")

##turn on and off causes with 0/1

bile <- 0
hernia <- 1

##load in shared functions 

source(paste0(j, "FILEPATH"))

##get locations

##get a dataset with all the location hierarchy information
epi_locations <- get_location_metadata(location_set_id=9)
##make a dataset from the above set that only includes locations for which estimates are made and at the most detailed level
location_ids <- epi_locations[is_estimate==1 & most_detailed==1,]
##take the above data set, and make a variable that is just a list of values for the variable location_id, without the other columns (is this an array variable?)
location_ids <- location_ids[,location_id]

measure_ids <- c(5,6)


##########################################################################################################
##parent code begins / send jobs on qsub
##########################################################################################################

##create cause list

causes<-list("bile", "hernia")

##keep on the cause list only those causes set to 1 above

for (cause in causes){
  if (get(cause)<1){
    causes<-causes[-which(causes==cause)]
  }
}


##create directories for draws and qsub jobs on cluster 

for(cause in causes) { 
  
  
  dir.create(file.path(draw_files, cause), showWarnings = F)
  draws <- file.path(draw_files, cause)
  dir.create(file.path(draws, "logs"), showWarnings = F)
  logs <- file.path(draws, "logs")
  for(location in location_ids) {
      for (measure in measure_ids) {
    
   ##when running on prod cluster
    command <- paste0("qsub -pe multi_slot 2 -P proj_custom_models -l mem_free=4g -o ",logs," -e ",logs, " -N ", paste0(cause,"_", location,"_", measure, " "), shell, " ", "FILEPATH", " ", cause, " ", location, " ", draws, " ", measure)
    system(command)
    }
  }
}




