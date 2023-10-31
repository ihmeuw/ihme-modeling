
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

packages <- c("data.table","magrittr","openxlsx","ggplot2","parallel","pbapply")

for(p in packages){
  if(p %in% rownames(installed.packages())==FALSE){
    install.packages(p)
  }
  library(p, character.only = T)
}

# Directories -------------------------------------------------------------

date <- format(Sys.Date(), "%m%d%y")

home_dir <- "FILEPATH"

in_dir <- "FILEPATH"
bwga_dir <- paste0(home_dir,"FILEPATH",date,"/") #this is the out_dir
dir.create(bwga_dir,recursive=T)

# Format files-------------------------------------------------------------

files <- list.files(in_dir,full.names = F)

locs <- substr(files,1,nchar(files)-13)

prep_n_save <- function(loc){
    file <- files[locs==loc]
    
    distr <- lapply(paste0(in_dir,file),readRDS)%>%rbindlist
    
    saveRDS(distr,paste0(bwga_dir,loc,".RDS"))
    
    print(paste0("Done with ",loc))
  }

pblapply(unique(locs),prep_n_save,cl = 20)
