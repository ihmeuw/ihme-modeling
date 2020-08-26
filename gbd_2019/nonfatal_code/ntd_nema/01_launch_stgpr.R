# Purpose: Launch STGPR models

#'[ General Set-Up]

# clear enviroment
rm(list = ls())

# set root
root <- paste0(FILEPATH, Sys.info()[7], "/")

# create new run dir
source(FILEPATH)
#gen_rundir(root = FILEPATH, acause = "ntd_nema", message = "GBD 2019 Final, step 4")

# set run dir
run_file <- fread(paste0(FILEPATH))
run_dir <- run_file[nrow(run_file), run_folder_path]
crosswalks_dir    <- paste0(run_dir, FILEPATH)
interms_dir    <- paste0(run_dir, FILEPATH)

# load packages / central functions / custom functions
library(metafor, lib.loc = FILEPATH)
library(msm, lib.loc = FILEPATH)
library(data.table)
library(ggplot2)
library(openxlsx)
source(FILEPATH)
source(FILEPATH)
source(FILEPATH)
source(FILEPATH)
source(FILEPATH)
source(FILEPATH)
source(FILEPATH)
source(FILEPATH)
source(FILEPATH)
source(paste0(root, FILEPATH)) # custom
source(paste0(root, FILEPATH)) # custom

# ST-GPR functions
central_root <- FILEPATH
setwd(central_root)

source(FILEPATH)
source(FILEPATH)

# function to launch stgpr models -- config contains parameters
stgpr_launcher <- function(worm, index_id){
  
  # register stgpr model
  run_id <- register_stgpr_model(paste0(FILEPATH, worm, FILEPATH), model_index_id = index_id)
  stgpr_sendoff(run_id, ADDRESS)

  print(paste0("Submitting ST-GPR model for ", worm, " with ST-GPR run_id of ", run_id ,"."))
  return(run_id)
}


worms <- c("ascariasis", "trichuriasis", "hookworm")

# stgpr tests
store <- data.table('run_id' = integer(), 'model_index' = integer())
for (model_index in c(41)){
  run_id <- stgpr_launcher('ascariasis', index_id = model_index)
  row <- data.table(run_id = run_id, model_index = model_index)
  store <- rbind(store, row)
}

fwrite(store, paste0(FILEPATH))

d10445 <- get_crosswalk_version(ADDRESS)
d8849 <- get_crosswalk_version(ADDRESS)
d830 <- get_crosswalk_version(ADDRESS)
