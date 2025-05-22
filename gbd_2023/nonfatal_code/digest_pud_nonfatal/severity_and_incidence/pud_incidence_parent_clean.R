###################################################################################################################
## Description: Get incidence draws from parent model and save them for upload to a single sequela 
## Output:  Incidence draws from total DisMod model, in format/file path that can be uploaded to incidence of a custom sequela
###################################################################################################################

## source("FILEPATH")

rm(list=ls())

##working environment 

os <- .Platform$OS.type

if (Sys.info()[1] == "Windows") {
  lib_path <- "FILEPATH"
  j<- "J:/"
  h<-"H:/"
  shell <- file.path(h, "FILEPATH")
  scratch <- "FILEPATH"
  
} else  {
  lib_path <- "FILEPATH"
  j<- "FILEPATH"
  h<- "FILEPATH"
  shell <- file.path(h, "FILEPATH")
  scratch <- "FILEPATH"
}




##draw directories
date <- file.path(scratch, format(Sys.Date(), format = "%Y_%m_%d"))

# dir.create((date), showWarnings = F)
##turn on and off causes with 0/1
pud <- 0
gastritis <- 1

##load in shared functions 
source("FILEPATH/save_results_epi.R")
source("FILEPATH/get_location_metadata.R")

##get locations

# ##get a dataset with all the location hierarchy information
epi_locations <- get_location_metadata(location_set_id=35, gbd_round_id=7, decomp_step="step3") 
location_ids <- epi_locations[is_estimate==1 & most_detailed==1]
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
  
  total_incidence_files <- file.path(date, paste0("tot5_anoa6", cause,"/"))
  logs <- file.path(total_incidence_files, "00_logs")
  error <- file.path(total_incidence_files, "00_error")
  
  for(location in location_ids) {
    command <- paste0("qsub -q all.q  -P proj_rgud -l m_mem_free=4G  -l fthread=5 -l h_rt=24:00:00  -o ",logs," -e ",error, " -N ", paste0(cause,"_", location, " "), shell, " FILEPATH", " ", cause, " ", location, " ", total_incidence_files)
    system(command)
  }
}


print("FINISH")

#save results
save_results_epi(input_dir="FILEPATH", input_file_pattern = "{location_id}.csv", modelable_entity_id =16219, description = 'DESCRIPTION', mark_best='TRUE', measure_id = c(5,6), gbd_round_id=7, decomp_step='iterative', bundle_id = 6998, crosswalk_version_id = 20357 )
