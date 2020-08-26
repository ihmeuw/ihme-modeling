#----HEADER----------------------------------------------------------------------------------------------------------------------
# Author: NAME
# Date: 7/10/18
# Purpose: Resaving PAFs into a format that is agreeable for save_results (not compatible with files saved by cause_id)
# source("~FILEPATH.R", echo=T)
# qsub -N save_air_pm_paf_test -pe multi_slot 1 -P ADDRESS -o FILEPATH -e FILEPATH FILEPATH.sh FILEPATH.R 
#*********************************************************************************************************************************
#------------------------Setup -------------------------------------------------------
# clear memory
rm(list=ls())
user <- "USERNAME"

# disable scientific notation
options(scipen = 999)

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

# load packages, install if missing
pacman::p_load(data.table, magrittr, ini)

#define args:
paf.version <- 50
risks <- data.table(risk=c("air_pmhap","air_pm","air_hap"), me_id=c(20260,8746,8747), rei_id=c(380,86,87))

project <- "-P proj_erf "

sge.output.dir <- paste0(" -o FILEPATH", user, "FILEPATH -e FILEPATH", user, "FILEPATH ")
#sge.output.dir <- "" # toggle to run with no output files

save.script <- "-s FILEPATH.R"
r.shell <- "FILEPATH.sh"

#----------------------------Functions---------------------------------------------------
#Get locations
source(file.path(central_lib,"FILEPATH.R"))
locations <- get_location_metadata(location_set_id=35, gbd_round_id=6)

home.dir <- "FILEPATH"

for(risk in risks$risk){
  paf.dir.new <- file.path(home.dir,risk,"FILEPATH",paf.version)
  dir.create(paf.dir.new,showWarnings = F)
}

out.mediation.dir <- file.path(home.dir,"FILEPATH",paf.version)
dir.create(out.mediation.dir,showWarnings = F)

#--------------------SCRIPT-----------------------------------------------------------------
locs <- locations[most_detailed==1,location_id]

pm.hap.dir <- file.path(home.dir,"FILEPATH",paf.version)
pm.hap.files <- list.files(pm.hap.dir)

pm.dir <- file.path(home.dir,"FILEPATH",paf.version)
pm.files <- list.files(pm.dir)

hap.dir <- file.path(home.dir,"FILEPATH",paf.version)
hap.files <- list.files(hap.dir)

  for(lid in locs){
    if(!(paste0(lid,".csv") %in% pm.hap.files)|
       !(paste0(lid,".csv") %in% pm.files)|
       !(paste0(lid,".csv") %in% hap.files)){
      
      args <- paste(lid,paf.version)
      mem <- "-l m_mem_free=2G"
      fthread <- "-l fthread=1"
      runtime <- "-l h_rt=04:00:00"
      archive <- ""
      jname <- paste0("-N ","resave_",lid)
      
      system(paste("qsub",jname,mem,fthread,runtime,archive,project,"-q ADDRESS",sge.output.dir,r.shell,save.script,args))
    }
  }