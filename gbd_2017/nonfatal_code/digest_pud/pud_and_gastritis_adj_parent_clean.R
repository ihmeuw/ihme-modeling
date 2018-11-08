###
###3 June 2018

###Parent script for split severity and subtract

# source("FILEPATH")

# set up environment
rm(list=ls())

os <- .Platform$OS.type
if (os=="windows") {
  lib_path <- "FILEPATH"
  j<- "J:/"
  h<-"H:/"
  scratch <-"FILEPATH"
} else {
  lib_path <- "FILEPATH"
  j<- "FILEPATH"
  h<-"FILEPATH"
  shell <- "FILEPATH"
  scratch <- "FILEPATH"
}

date <- file.path(scratch, format(Sys.Date(), format = "%Y_%m_%d"))
dir.create(date, showWarnings = F)

# source central functions needed

source(paste0(j, "FILEPATH"))

# define objects

##create cause list
causes<-list("pud","gastritis")

##turn on and off causes with 0/1
pud <- 1
gastritis <- 1

for (cause in causes){
  if (get(cause)<1){
    causes<-causes[-which(causes==cause)]
  }
}

# get locations

epi_locations <- get_location_metadata(location_set_id=9)
location_ids <- epi_locations[is_estimate==1 & most_detailed==1,]
location_ids <- location_ids[,location_id]

# write loop with the qsub statement for all the causes and locations to loop over

for(cause in causes) { 
  
  adj_child_files <- file.path(date, paste0(cause, "_adj_child_files"))  
  asymp_files <- file.path(adj_child_files, paste0("asymp_", cause))
  mild_files <- file.path(adj_child_files, paste0("mild_", cause))
  mod_files <- file.path(adj_child_files, paste0("mod_", cause))
  adj_acute_files <- file.path(adj_child_files, paste0("adj_acute_", cause))
  adj_complic_files <- file.path(adj_child_files, paste0("adj_complic_", cause))
  logs <- file.path(adj_child_files, "00_logs")
  
  dir.create(file.path(adj_child_files), showWarnings = F)  
  dir.create(file.path(asymp_files), showWarnings = F)
  dir.create(file.path(mild_files), showWarnings = F)
  dir.create(file.path(mod_files), showWarnings = F)
  dir.create(file.path(adj_acute_files), showWarnings = F)
  dir.create(file.path(adj_complic_files), showWarnings = F)
  dir.create(file.path(adj_child_files, "00_logs"), showWarnings = F)
  
  for(location in location_ids) {
    command <- paste0("qsub -pe multi_slot 1 -P proj_custom_models -l mem_free=4g -o ",logs," -e ",logs, " -N ", paste0(cause,"_", location, "_adj_child "), shell, " ", "FILEPATH", cause, " ", location, " ", adj_child_files)
    system(command)
  }
}

