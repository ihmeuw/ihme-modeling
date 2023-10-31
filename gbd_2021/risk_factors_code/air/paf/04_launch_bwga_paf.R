
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

project <- "-P PROJECT"
sge.output.dir <- " -o FILEPATH -e FILEPATH "

#------------------Launch PAFs--------------------------------------------------
estimation_years <- c(1990,1995,2000,2005,2010,2015,2019,2020)


source("FILEPATH.R")
launch_paf(rei_id=380, 
           save_results=F, 
           decomp_step="iterative", 
           year_id=estimation_years, 
           gbd_round_id=7, 
           cluster_proj=project,
           # resume=T,
           m_mem_free="6G")




