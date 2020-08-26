
#-------------------Header------------------------------------------------
# Author: NAME
# Date: 3/19/2019
# Purpose: Save ST-GPR output
#          
# source("FILEPATH.R", echo=T)
# 10 threads, 100 GB
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
  }

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

#------------------DIRECTORIES--------------------------------------------------

source(file.path(central_lib,"FILEPATH.R"))

run_id <- 102800
decomp <- "step4"
                   
save_results_stgpr(stgpr_version_id=run_id,
                   risk_type="exposure",
                   description=paste0("Run_id: ",run_id,", Decomp ", decomp),
                   metric_id=3,
                   mark_best=TRUE,
                   age_expand=c(2:20,30,31,32,235),
                   sex_expand=c(1,2))