###################################################################################################################
## Author: NAME
## Description: Get incidence draws from parent model and save them for upload to a single sequela 
## Output:  Incidence draws from total DisMod model, in format/file path that can be uploaded to incidence of a custom sequela
## Notes: Currently done for PUD and gastritis
###################################################################################################################

## source("FILEPATH/pud_and_gastritis_incidence_parent.R")

rm(list=ls())

##working environment 

os <- .Platform$OS.type
if (os=="windows") {
  lib_path <- "FILEPATH_LIB"
  j<- "FILEPATH_J"
  h<-"FILEPATH_H"
  scratch <-"FILEPATH_H/giasymp"
} else {
  lib_path <- "FILEPATH_LIB"
  j<- "FILEPATH_J"
  h<-"FILEPATH_H"
  shell <- "FILEPATH/r_shell.sh"
  scratch <- "FILEPATH_SCRATCH/pud_and_gastritis"
}

## deletes folders and contents from a previous run on the same day, commented out because we dont always want to do this 
#unlink(file.path(scratch, Sys.Date()), recursive = T) 

##draw directories

date <- file.path(scratch, format(Sys.Date(), format = "%Y_%m_%d"))
dir.create((date), showWarnings = F)

##turn on and off causes with 0/1

pud <- 1
gastritis <- 1

##load in shared functions 

source("FILEPATH/get_location_metadata.R")

##get locations

##get a dataset with all the location hierarchy information
epi_locations <- get_location_metadata(location_set_id=9)
##make a dataset from the above set that only includes locations for which estimates are made and at the most detailed level
location_ids <- epi_locations[is_estimate==1 & most_detailed==1,]
##take the above data set, and make a variable that is just a list of values for the variable location_id, without the other columns 
location_ids <- location_ids[,location_id]

##########################################################################################################
##parent code begins / send jobs on qsub
##########################################################################################################

##create cause list

causes<-list("pud","gastritis")

##keep on the cause list only those causes set to 1 above

for (cause in causes){
  if (get(cause)<1){
    causes<-causes[-which(causes==cause)]
  }
}


##create directories for draws and qsub jobs on cluster 

for(cause in causes) { 
  
  total_incidence_files <- file.path(date, paste0("tot5_anoa6", cause))
  logs <- file.path(total_incidence_files, "00_logs")
  
  dir.create(file.path(total_incidence_files), showWarnings = F)
  dir.create(file.path(logs), showWarnings = F)
  
  for(location in location_ids) {
   
      ##GBD 2019 syntax on Buster
      command <- paste0("qsub -q all.q  -P proj_PROJECT_NAME -l m_mem_free=4G  -l fthread=5 -l h_rt=24:00:00  -o ",logs," -e ",logs, " -N ", paste0(cause,"_", location, " "), shell, " ", "FILEPATH/pud_and_gastritis_incidence_child.R", " ", cause, " ", location, " ", total_incidence_files)
      system(command)
      
  }
}

print("MY KIDS rock")



