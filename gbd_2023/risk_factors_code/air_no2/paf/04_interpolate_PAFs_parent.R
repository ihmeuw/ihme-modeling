
#-------------------Header------------------------------------------------
# Author: 
# Purpose: Launches interpolate PAFs air_no2
#
#------------------Set-up--------------------------------------------------

#clear memory
rm(list=ls())

user <- "USER"


# runtime configuration
if (Sys.info()["sysname"] == "Linux") {
  j_root <- "FILEPATH/"
  h_root <- "~/"
  central_lib <- "FILEPATH/"
} else {
  j_root <- "J:/"
  h_root <- "H:/"
  central_lib <- "FILEPATH"
}


pacman::p_load(data.table, magrittr)
project <- "-A proj_erf" 
sge.output.dir <- "-e FILEPATH -o FILEPATH"
#sge.output.dir <- "" # toggle to run with no output files

r.shell <- "FILEPATH/execRscript.sh"
interpolate.script <- "FILEPATH/04_interpolate_PAFs.R"


#------------------Change each run-----------------------------------------

paf.version <- 23 
decomp <- "iterative"

#------------------Set up jobs---------------------------------------------

# Get location metadata for most-detailed locations
source("FILEPATH/get_location_metadata.R")
locs <- as.data.table(get_location_metadata(location_set_id=35, release_id=16))
locs <- locs[most_detailed==1]
locs <- locs$location_id


#create datatable of unique jobs
risk <- "air_no2"
me_id <- 26282
rei_id <- 404
  
dir.create(paste0("FILEPATH"))

for (l in locs){

  arg <- paste(risk,me_id,rei_id,paf.version,l)
  mem <- "--mem=5G"
  fthread <- "-c 10"
  runtime <- "-t 12:00:00"
  archive <- ""
  jname <- paste0("-J ","interpolate_PAFs_",risk)
  system(paste("sbatch",jname,mem,fthread,runtime,archive,project,"-p long.q",sge.output.dir,r.shell,"-s",interpolate.script,arg))
}



#********************************************************************************************************************************
