#----HEADER----------------------------------------------------------------------------------------------------------------------
# Author: NAME
# Date: 01/05/2018
# Purpose: Save results for envir_radon
# source("FILEPATH.R", echo=T)
#
#------------------------Setup -------------------------------------------------------
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

project <- "ADDRESS"

# load packages, install if missing
pacman::p_load(data.table, magrittr,ini)

#draw directories
exp.dir <-  paste0(j_root,"FILEPATH")

years <- 1990:2019

decomp <- "step4"

#----------------------------Functions---------------------------------------------------

#Save Results Epi (EXP)
source(file.path(central_lib,"FILEPATH.R"))

#Save Results Risk (RR, PAF)
source(file.path(central_lib,"FILEPATH.R"))

#get demographics
source(file.path(central_lib,"FILEPATH.R"))

#get draws
source(file.path(central_lib,"FILEPATH.R"))

#custom fx
"%ni%" <- Negate("%in%") # create a reverse %in% operator

#--------------------SCRIPT-----------------------------------------------------------------

  #PAF_calculator code:
  source("FILEPATH.R")
  launch_paf(90, year_id=years, decomp_step=decomp, cluster_proj=project)
  
  #PAF scatters
  source("FILEPATH.R")
  
  #hold for 2 hours until save results finishes
  Sys.sleep(60*60*2)
  
  paf_scatter(rei_id=90,measure_id=3,file_path=paste0("FILEPATH",decomp,".pdf"),add_isos=F,decomp_step=decomp,"_",01212020)
  paf_scatter(rei_id=90,measure_id=4,file_path=paste0("FILEPATH",decomp,".pdf"),add_isos=T,decomp_step=decomp,"_",01212020)