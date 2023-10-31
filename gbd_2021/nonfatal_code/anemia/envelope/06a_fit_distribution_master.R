###########################################################
### Project: Anemia
### Purpose: Fit Distribution to Modeled Mean & Standard Deviation. Launches parallelization of distribution type (ensemble or weibull)
###########################################################

#----CONFIG----------------------------------------------------------------------------------------------------------------------
# clear memory
rm(list=ls())

os <- .Platform$OS.type
if (os=="Windows") {
  j <- "FILEPATH"
  h <- "FILEPATH"
} 
if (os=="unix") {
  j <- "FILEPATH"
  h <- "FILEPATH"
}

# load packages, install if missing
library(data.table)

# Settings
distribution <- "ensemblemv2p"
project <- "-P PROJECT " 
draws.required <- 1000 
resume <- F 

###Specify Directories###
code.dir <- paste0("FILEPATH")
sge.output.dir <- "-o FILEPATH -e FILEPATH "
output_root <- paste0("FILEPATH/", distribution)

# create directories
dir.create(file.path(output_root), showWarnings = FALSE, recursive = TRUE)
dir.create(file.path("FILEPATH"), showWarnings = FALSE)
dir.create(file.path("FILEPATH"), showWarnings = FALSE)
dir.create(file.path("FILEPATH"), showWarnings = FALSE)

#********************************************************************************************************************************	
#DROP OLD FILES 
if (resume==FALSE){  
  for(meid in c(10489, 10490, 10491, 10507,10992)) {
    print(paste("DROPPING OLD RESULTS FOR MEID", meid))
    dir.create(paste0(output_root, "/", meid), showWarnings = FALSE)
    setwd(paste0(output_root, "/", meid))
    old_files <- list.files()
    file.remove(old_files)
  } 
}

#----LAUNCH JOBS-----------------------------------------------------------------------------------------------------------------
  
source(paste0("FILEPATH/get_location_metadata.R"))
loc_meta <- get_location_metadata(location_set_id=9,gbd_round_id=7)
loc_meta <- loc_meta[is_estimate == 1 & most_detailed == 1]
locs <- loc_meta$location_id
  
for(loc in locs){
  # Launch jobs
  jname <- paste0("anemia_distfit_loc_",loc)
  model.script <- paste0(code.dir, "/fit_ensemblemv2p_parallel.R")
  r.shell <- file.path(code.dir, "rshell.sh")
  sys.sub <- paste0("qsub ", project, sge.output.dir, " -N ", jname, " -l m_mem_free=10G -l fthread=5 -l h_rt=01:00:00 -q all.q")
  args <- paste(code.dir, output_root, loc, draws.required, distribution)
  
   system(paste(sys.sub, r.shell, model.script, args))
}
#*******************************************************************************************************************************

if(T) {
  print("ALL JOBS LAUNCHED - FEEL FREE TO QUIT R")
  print("TIMER TO SEE WHEN ALL JOBS ARE DONE - every 60s")
  finished <- list.files(paste0(output_root, "/10507"))
  while(length(finished) < length(locs)) {
    finished <- list.files(paste0(output_root, "/10507"))
    print(paste(length(finished), "of", length(locs), "jobs finished"))
    Sys.sleep(60)
  }
  print("CONGRATS - ALL JOBS FINISHED")
}
  