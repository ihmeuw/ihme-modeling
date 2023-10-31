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

source(file.path(central_lib,"FILEPATH/save_results_stgpr.R"))

run_id <- 161417
decomp <- "iterative"
                   
save_results_stgpr(stgpr_version_id=run_id,
                   risk_type="exposure",
                   description=paste0("Run_id: ",run_id,", Decomp ", decomp),
                   metric_id=3,
                   mark_best=TRUE,
                   age_expand=c(2:3,6:20,30,31,32,34,235,238,388,389), #add in 388, 389, 238, 34 for the new <5 age groups!
                   sex_expand=c(1,2))

