################################################################################
## DESCRIPTION: Register and launch ST-GPR Model from R ##
## INPUTS: Finalized processed anthropometric data ##
## OUTPUTS: Launched ST-GPR Model ##
## AUTHOR:  ##
## DATE: ##

rm(list = ls())

# System info
os <- Sys.info()[1]
user <- Sys.info()[7]

# Drives 
j <- if (os == "Linux") "FILEPATH" else if (os == "Windows") "FILEPATH"
h <- if (os == "Linux") paste0("FILEPATH", user, "/") else if (os == "Windows") "FILEPATH"

# Base filepaths
work_dir <- paste0(j, 'FILEPATH')
share_dir <- 'FILEPATH' # only accessible from the cluster
code_dir <- if (os == "Linux") paste0("FILEPATH", user, "/") else if (os == "Windows") ""
stgpr_dir <- 'FILEPATH'


## LOAD DEPENDENCIES
source(paste0(code_dir, 'FILEPATH/primer.R'))
source(paste0(code_dir, 'FILEPATH/version_tools.R'))
source(paste0(code_dir, 'FILEPATH/anthro_utilities.R'))
library(rhdf5, lib.loc = "FILEPATH")
source("FILEPATH/plot_gpr.R")
central_root <- "FILEPATH"
setwd(central_root)
source("FILEPATH/stgpr/api/public.R") # this is the new path for STGPR functions


# Specify cluster arguments for launch
cluster_project = 'PROJECT'
gbd_year <- as.numeric(str_extract(cycle_year, "(?<=gbd_?)\\d+")) # Pulls out the year character to match the config

# Read in STGPR configs
ow_config_path <- "FILEPATH"
ob_config_path <- "FILEPATH"
uw_config_path <- "FILEPATH"
suw_config_path <- "FILEPATH"
bmi_config_path <- "FILEPATH"
ow_config <- fread(ow_config_path)
ob_config <- fread(ob_config_path)
uw_config <- fread(uw_config_path)
suw_config <- fread(suw_config_path)
bmi_config <- fread(bmi_config_path)


# Set index ID object
index_ow <- c(1:2)
index_ob <- c(1:2)
index_uw <- c(1:2)
index_suw <- c(1:2)
index_bmi <- c(1:2)

master_track <- data.table()
for( m in c("ow","ob","uw", "suw","bmi")){ 
  index <- get(paste0("index_", m))
  for( i in index){
    # Create live tracking sheet
    config <- get(paste0( m, "_config"))
    tracking <- data.table(model_index_id = config[model_index_id == i, model_index_id], notes = config[model_index_id == i, notes])
    
    # Register
    config_path <- get(paste0( m, "_config_path"))
    stgpr_id <- register_stgpr_model(config_path, model_index_id = i)
    tracking[model_index_id == i, run_id := stgpr_id]
    
    # Sendoff
    stgpr_sendoff(stgpr_id, cluster_project)
    
    # # Save tracking sheet
    master_track <- rbind(master_track, tracking)
  }
}

# Link st_gpr run id with data tracking sheet
version_map_path <- "FILEPATH"
tracking <- fread(version_map_path)
tracking_temp <- tail(tracking,8)
tracking_temp[, stgpr_run_id := master_track$run_id[1:8]]
tracking_temp[, description := master_track$notes[1:8]]
tracking <- rbind(tracking[!crosswalk_version_id %in% tracking_temp$crosswalk_version_id], tracking_temp)
fwrite(tracking, version_map_path)

version_map_path <- "FILEPATH"
tracking <- fread(version_map_path)
tracking_temp <- tail(tracking,2)
tracking_temp[, stgpr_run_id := master_track$run_id[9:10]]
tracking_temp[, description := master_track$notes[9:10]]
tracking <- rbind(tracking[!crosswalk_version_id %in% tracking_temp$crosswalk_version_id], tracking_temp)
fwrite(tracking, version_map_path)

