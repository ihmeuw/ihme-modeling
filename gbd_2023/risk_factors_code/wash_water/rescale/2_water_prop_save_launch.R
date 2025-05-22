rm(list=ls())

# runtime configuration
if (Sys.info()["sysname"] == "Linux") {
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
  central_lib <- "FILEPATH"
} else {
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
  central_lib <- "FILEPATH"
}
library(data.table)
library(fst)
library(readr)
source("FILEPATH/get_demographics_template.R")




project <- "-A proj_erf "
sge.output.dir <- "-e FILEPATH -o FILEPATH "
rshell <- "FILEPATH"

memory <- 100
cores.provided <- 1

#arguments ########################
release<-16
gbd<-"GBD2022" #for filepaths
out.dir<-paste0("FIELPATH/",gbd,"FILEPATH")
out.dir.fst <- paste0(out.dir,"FILEPATH")

# make sure out.dir is created
dir.create(out.dir, showWarnings = FALSE)

#get demographics
demo<-get_demographics_template(gbd_team = "epi",release_id = release)
locs<-unique(demo$location_id)


#launch job ###########################
  
  for (loc in locs) {
    
    arg_list <- paste(release, gbd, loc, out.dir,out.dir.fst)
    script <- file.path("-s FILEPATH/water_prop_save.R") # check that it is the right file
    mem <- paste0("--mem=",memory,"G")
    fthread <- paste0("-c ",cores.provided)
    runtime <- "-t 01:00:00"
    jname <- paste0("-J ", loc,"_water")
    
    # browser()
    system(paste("sbatch",jname,mem,fthread,runtime,project,"-p all.q",sge.output.dir,rshell,script, arg_list))
    
  }
