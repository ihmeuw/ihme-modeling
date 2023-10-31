# clear memory
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

project <- "PROJECT"
sge.output.dir <- "FILEPATH"
#sge.output.dir <- "" # toggle to run with no output files
  
rshell <- "FILEPATH"

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

source(file.path(central_lib,"FILEPATH/get_location_metadata.R"))
locations <- get_location_metadata(location_set_id=35, gbd_round_id=7)
locs <- locations[most_detailed==1,location_id]

# Directories -------------------------------------------------------------

version <- VERSION

draws.required <- 1000
years <- c(1990,1995,2000:2020)

# A) Data Prep

memory <- 300
cores.provided <- 5

script <- file.path("FILEPATH/02a_DataPrep.R")

args <- paste(version, draws.required, cores.provided)
mem <- paste0("-l m_mem_free=",memory,"G")
fthread <- paste0("-l fthread=",cores.provided)
runtime <- "-l h_rt=06:00:00"
archive <- "" # no jdrive access needed
jname <- paste0("-N air_pm_prep")

system(paste("qsub",jname,mem,fthread,runtime,archive,project,"-q long.q",sge.output.dir,rshell,script,args))


# B) Run Models


memory <- 200
cores.provided <- 5

script <- file.path("FILEPATH/02b_RunModels.R")

args <- paste(version, draws.required, cores.provided)
mem <- paste0("-l m_mem_free=",memory,"G")
fthread <- paste0("-l fthread=",cores.provided)
runtime <- "-l h_rt=06:00:00"
archive <- "" # no jdrive access needed
jname <- paste0("-N air_pm_model")
hold <- paste0("-hold_jid air_pm_prep") # use this if you're launching them all at the same time


system(paste("qsub",jname,hold,mem,fthread,runtime,archive,project,"-q all.q",sge.output.dir,rshell,script,args))


# C) Predictions

memory <- 400
cores.provided <- 5

script <- file.path("FILEPATH/02c_Predictions.R")

for(this.year in years){

  args <- paste(this.year, version, draws.required, cores.provided)
  mem <- paste0("-l m_mem_free=",memory,"G")
  fthread <- paste0("-l fthread=",cores.provided)
  runtime <- "-l h_rt=10:00:00"
  archive <- "" # no jdrive access needed
  jname <- paste0("-N air_pm_predict_",this.year)
  hold <- paste0("-hold_jid air_pm_model")

  system(paste("qsub",jname,hold,mem,fthread,runtime,archive,project,"-q all.q",sge.output.dir,rshell,script,args))

}


# D) Exposures

memory <- 400  #they probably dont need this much but QPID wont give me mem output on these. UGH
cores.provided <- 5

script <- file.path("FILEPATH/02d_Exposures.R")

for(this.year in years){


  args <- paste(this.year, version, draws.required, cores.provided)
  fthread <- paste0("-l fthread=",cores.provided)
  runtime <- "-l h_rt=48:00:00"
  archive <- "" # no jdrive access needed
  jname <- paste0("-N air_pm_exposure_agg_",this.year)
  hold <- paste0("-hold_jid air_pm_predict_",this.year)
  mem <- paste0("-l m_mem_free=",memory,"G")

  system(paste("qsub",jname,hold,mem,fthread,runtime,archive,project,"-q long.q",sge.output.dir,rshell,script,args))

}


# E) PAF calc prep

memory <- 5  # max ram used was 2.7 G
cores.provided <- 5

script <- file.path("FILEPATH/PAF_calc_exposure_prep.R")

for(this.year in years){

  for(this.loc in sort(locs)){

    args <- paste(this.loc, this.year, version, draws.required)
    fthread <- paste0("-l fthread=",cores.provided)
    runtime <- "-l h_rt=24:00:00"
    archive <- "" # no jdrive access needed
    jname <- paste0("-N air_exp_collapse_",this.loc,"_",this.year)
    hold <- paste0("-hold_jid air_pm_predict_",this.year)
    mem <- paste0("-l m_mem_free=",memory,"G")

    system(paste("qsub",jname,hold,mem,fthread,runtime,archive,project,"-q long.q",sge.output.dir,rshell,script,args))


  }

}

