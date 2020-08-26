#----HEADER----------------------------------------------------------------------------------------------------------------------
# Author: NAME
# Date: 4/21/2018
# Purpose: Launches save results for air_pm, air_hap and air, new proportional pafs method
# source("FILEPATH.R", echo=T)
# qsub -N save_air -pe multi_slot 1 -P ADDRESS -o FILEPATH -e FILEPATH FILEPATH.sh FILEPATH.R 
#*********************************************************************************************************************************

#----CONFIG----------------------------------------------------------------------------------------------------------------------
# clear memory
rm(list=ls())

user <- 'USERNAME'

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

pacman::p_load(data.table, magrittr)

project <- "-P ADDRESS"
sge.output.dir <- paste0(" -o ADDRESS", user, "ADDRESS -e ADDRESS", user, "ADDRESS")

#sge.output.dir <- "" # toggle to run with no output files

save.script <- "-s FILEPATH.R"
r.shell <- "FILEPATH.sh"

paf.version <- 51
decomp <- "step4"

#create datatable of unique jobs
risks <- data.table(risk=c("air_pmhap","air_pm","air_hap"), me_id=c(20260,8746,8747), rei_id=c(380,86,87))

save <- function(i){

  args <- paste(risks[i,risk],
                risks[i,me_id],
                risks[i,rei_id],
                paf.version,
                decomp)
  mem <- "-l m_mem_free=75G"
  fthread <- "-l fthread=10"
  runtime <- "-l h_rt=06:00:00"
  archive <- "-l archive=TRUE" 
  jname <- paste0("-N ","save_results_",risks[i,risk])
  
  system(paste("qsub",jname,mem,fthread,runtime,archive,project,"-q long.q",sge.output.dir,r.shell,save.script,args))
}

complete <- lapply(1:nrow(risks),save)