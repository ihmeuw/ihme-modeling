#-------------------Header------------------------------------------------
# Author: NAME
# Date: 10/8/2019
# Purpose: Launcher script for air pollution exposure
#          
# source("FILEPATH.R", echo=T)
#***************************************************************************

#------------------SET-UP--------------------------------------------------

# clear memory
rm(list=ls())

# runtime configuration
if (Sys.info()["sysname"] == "Linux") {
  j_root <- "ADDRESS"
  h_root <- "ADDRESS"
  central_lib <- "ADDRESS"
  } else {
  j_root <- "ADDRESS"
  h_root <- "ADDRESS"
  central_lib <- "ADDRESS"
  }


  project <- "-P ADDRESS "
  sge.output.dir <- " -o /FILEPATH -e FILEPATH "
  #sge.output.dir <- "" # toggle to run with no output files
  
  rshell <- "FILEPATH.sh"

# load packages, install if missing

lib.loc <- paste0(h_root,"R/",R.Version()$platform,"/",R.Version()$major,".",R.Version()$minor)
dir.create(lib.loc,recursive=T, showWarnings = F)
.libPaths(c(lib.loc,.libPaths()))

packages <- c("data.table","magrittr")

for(p in packages){
  if(p %in% rownames(installed.packages())==FALSE){
    install.packages(p)
  }
  library(p, character.only = T)
}

# Directories -------------------------------------------------------------

version <- 3
years <- c(1990, 1995, 2000, 2005, 2010, 2015, 2017, 2019)
draws.required <- 1000

#NOTE: INLA objects and samples for GBD 2019 were produced by NAME.
#      Only "prediction" and "exposure" steps were run by ERF team.

# C) Predictions
project <- "-P ADDRESS "
sge.output.dir <- " -o FILEPATH -e FILEPATH "
#sge.output.dir <- "" # toggle to run with no output files

rshell <- "FILEPATH.sh"

years <- c(1990, 1995, 2000, 2005, 2010, 2015, 2017, 2019)
# years <- 2019 # test on one year
draws.required <- 10
version <- 37

cores.provided <- 5
memory <- 200

script <- file.path("FILEPATH.R")

for(this.year in years){
  
  args <- paste(this.year, version, draws.required, cores.provided, "test")
  fthread <- paste0("-l fthread=",cores.provided)
  runtime <- "-l h_rt=03:00:00"
  archive <- "" # no jdrive access needed
  jname <- paste0("-N air_pm_predict_",this.year)
  
  system(paste("qsub",jname,mem,fthread,runtime,archive,project,"-q ADDRESS",sge.output.dir,rshell,script,args))
  
}

# D) Exposures

cores.provided <- 5
memory <- 400

script <- file.path(h_root,"FILEPATH.R")

for(this.year in years){
 
  args <- paste(this.year, version, draws.required, cores.provided, "test")
  mem <- paste0("-l m_mem_free=",memory,"G")
  fthread <- paste0("-l fthread=",cores.provided)
  runtime <- "-l h_rt=12:00:00"
  archive <- "" # no jdrive access needed
  jname <- paste0("-N air_pm_exposure_agg_",this.year)
  hold <- paste0("-hold_jid air_pm_predict_",this.year)
  
  system(paste("qsub",jname,hold,mem,fthread,runtime,archive,project,"-q ADDRESS",sge.output.dir,rshell,script,args))
  
}