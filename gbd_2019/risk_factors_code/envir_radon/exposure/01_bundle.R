
#-------------------Header------------------------------------------------
# Author: NAME
# Date: 06/18/2019
# Purpose: Uploading/editing bundle data for radon exposure
#          
# source("FILEPATH.R", echo=T)

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


# load packages, install if missing

lib.loc <- paste0(h_root,"R/",R.Version()$platform,"/",R.Version()$major,".",R.Version()$minor)
dir.create(lib.loc,recursive=T, showWarnings = F)
.libPaths(c(lib.loc,.libPaths()))

packages <- c("data.table","magrittr","openxlsx")

for(p in packages){
  if(p %in% rownames(installed.packages())==FALSE){
    install.packages(p)
  }
  library(p, character.only = T)
}

decomp <- "step4"
bundle <- 1529

# Directories & Functions -----------------------------------------------

#radon directory
home_dir <- "FILEPATH"
out_path <- file.path(j_root,"FILEPATH",bundle,"FILEPATH.xlsx")

#bundle functions
source(file.path(central_lib,"FILEPATH.R"))
source(file.path(central_lib,"FILEPATH.R"))

#bundle <- get_bundle_data(bundle,decomp_step = decomp)

dt <- openxlsx::read.xlsx(file.path(home_dir,"FILEPATH.xlsx"),
                          sheet="Extraction",rows=c(1,4:100000), na.strings=c("NA","<NA>")) %>% as.data.table

step1 <- fread(file.path(home_dir,"FILEPATH.csv"))

setnames(dt,c("year_start","year_end"),c("year_start_id","year_end_id"))

#drop excluded studies
dt <- dt[is_outlier==0]

#choose correct dataset
if(decomp=="step2"){
  dt <- dt[nid %in% step1$nid]
}

dt[,year_id:=round(mean(c(year_start_id,year_end_id)))]
dt[,seq:=as.numeric(NA)]
dt[,sex:="Both"]
dt[,age_group_id:=22]
dt[,measure:="continuous"]
dt[,upper:=as.numeric(NA)]
dt[,lower:=as.numeric(NA)]
dt[,val:=0]
dt[,variance:=10]

write.xlsx(dt,out_path,sheetName="extraction")

upload_bundle_data(bundle,decomp,out_path)