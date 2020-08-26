
#-------------------Header------------------------------------------------
# Author: NAME
# Date: 6/4/19
# Purpose: Get bundle, upload crosswalk version, and launch ST-GPR model
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

if (Sys.getenv('SGE_CLUSTER_NAME') == "NAME" ) {
  
  project <- "-P NAME"
  sge.output.dir <- " -o FILEPATH -e FILEPATH"
  #sge.output.dir <- "" # toggle to run with no output files
  
} else {
  
  project <- "-P NAME" 
  sge.output.dir <- " -o FILEPATH -e FILEPATH"
  #sge.output.dir <- "" # toggle to run with no output files
  
}

# load packages, install if missing

lib.loc <- paste0(h_root,"R/",R.Version()$platform,"/",R.Version()$major,".",R.Version()$minor)
dir.create(lib.loc,recursive=T, showWarnings = F)
.libPaths(c(lib.loc,.libPaths()))

packages <- c("data.table","magrittr","xlsx")

for(p in packages){
  if(p %in% rownames(installed.packages())==FALSE){
    install.packages(p)
  }
  library(p, character.only = T)
}

decomp <- "step4"
description <- "" 
n_draws <- 1000

# Update bundle and crosswalk version -------------------------------------------------------------

#functions
source(file.path(central_lib,"FILEPATH.R"))
source(file.path(central_lib,"FILEPATH.R"))
source(file.path(central_lib,"FILEPATH.R"))
source(file.path(central_lib,"FILEPATH.R"))
source(file.path(central_lib,"FILEPATH.R"))
source(file.path(central_lib,"FILEPATH.R"))
source(file.path(central_lib,"FILEPATH.R"))
source(file.path(central_lib,"FILEPATH.R"))

# bundle_dir <- "FILEPATH"
# 
# bundle_version <- save_bundle_version(4736,decomp_step=decomp)
# bundle <- get_bundle_version(bundle_version$bundle_version_id)
# 
# # prepping file for deletion of duplicates and outliering
# bundle[,unique:=1:.N,by=c("location_name","year_id","nid")]
# bundle <- bundle[order(year_id)][order(location_name)][order(nid)]
# 
# out_path <- paste0(bundle_dir,"bundle_version_",bundle_version$bundle_version_id,".xlsx")
# write.xlsx(bundle,out_path,row.names=F,sheetName="extraction")
# 
# upload_bundle_data(4736, decomp_step=decomp, filepath=out_path)
# 
# bundle_version <- save_bundle_version(4736,decomp_step=decomp)
# bundle <- get_bundle_version(bundle_version$bundle_version_id)
# 
# bundle[,crosswalk_parent_seq:=seq]
# out_path <- paste0(bundle_dir,"bundle_version_",bundle_version$bundle_version_id,".xlsx")
# write.xlsx(bundle,out_path,row.names=F,sheetName="extraction")
# 
# crosswalk_version <- save_crosswalk_version(bundle_version$bundle_version_id,out_path)
# 
# crosswalk_version[,description:=description]
# crosswalk_version[,bundle_version_id:=bundle_version$bundle_version_id]
# 
# if(file.exists(paste0(bundle_dir,"save_version_ids.csv"))){
#   log <- fread(paste0(bundle_dir,"save_version_ids.csv"))
#   ids_out <- rbind(log,crosswalk_version,fill=T,use.names=T)
# }else{
#   ids_out <- rbind(bundle_version,crosswalk_version,fill=T,use.names=T)
# }
# write.csv(ids_out,paste0(bundle_dir,"save_version_ids.csv"),row.names=F)

home_dir <- "FILEPATH"
crosswalk_version <- fread(file.path(home_dir,"FILEPATH.csv"))

# Launch ST-GPR -----------------------------------------------------------

central_root <- 'FILEPATH'
setwd(central_root)

source('FILEPATH.R')
source('FILEPATH.R')
source("FILEPATH.R")

config <- fread(file.path(home_dir,"FILEPATH.csv"))

# Arguments
new_config <- copy(config[nrow(config)])
new_config[,notes:=description]
new_config[,decomp_step:=decomp]
new_config[,crosswalk_version_id:=tail(crosswalk_version$crosswalk_version_id,1)]
new_config[,holdouts:=0] # Keep at 0 unless you want to run cross-validation
new_config[,draws:=n_draws]
new_config[,transform_offset:=NA]
new_config[,gpr_draws:=n_draws]

config <- rbind(config,new_config,fill=T,use.names=T)

config[,model_index_id:=1:.N]

write.csv(config,file.path(home_dir,"config.csv"),row.names=F)

# Registration
run_id <- register_stgpr_model(path_to_config=file.path(home_dir,"config.csv"),model_index_id = nrow(config))

# Sendoff

stgpr_sendoff(run_id, project)

run_dir <- file.path("FILEPATH",run_id)

dir.create(run_dir,recursive=T)

#write.csv(ids_out,paste0(run_dir,"/save_version_ids.csv"),row.names=F)

Sys.sleep(60*60)

#helper functions for once the run finishes
plot_gpr(run_id, output.path = paste0(home_dir,"/FILEPATH.pdf"))

plot_gpr(run_id, run.id2 = 58934, output.path = paste0("FILEPATH.pdf"))