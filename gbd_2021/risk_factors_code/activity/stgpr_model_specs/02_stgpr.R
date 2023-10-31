################################################################################
## DESCRIPTION: Register and sendoff STGPR models for mean MET
## INPUTS: stgpr config ##
## OUTPUTS: plots of run IDs ##
## AUTHOR:
## DATE:
################################################################################

rm(list = ls())

# System info
os <- Sys.info()[1]
user <- Sys.info()[7]

# Drives 
j <- if (os == "Linux") "FILEPATH" else if (os == "Windows") "FILEPATH"
h <- if (os == "Linux") paste0("FILEPATH", user, "/") else if (os == "Windows") "FILEPATH"


# Base filepaths
share_dir <- 'FILEPATH' # only accessible from the cluster
code_dir <- if (os == "Linux") paste0("FILEPATH", user, "/") else if (os == "Windows") ""
work_dir <- paste0(code_dir, "FILEPATH")

## LOAD DEPENDENCIES
source(paste0(code_dir, 'FILEPATH'))
source(paste0(code_dir, 'FILEPATH'))
central_root <- 'FILEPATH'
setwd(central_root)
source(paste0(central_root, 'FILEPATH'))
source(paste0(central_root, 'FILEPATH'))
library(rhdf5, lib.loc = "FILEPATH")
source("FILEPATH")
# Read in config and tracker
config <- fread(paste0(work_dir, "FILEPATH"))
master_track <- fread(paste0(work_dir, "FILEPATH"))

# Create tracking sheet
version = 9 # regression predictions with all-age, both-sex SR coef from GPAQ and IPAQ applied
index = 23 # model index ID in the config

tracking <- data.table('data_version' = version, 'model_index_id' = config[model_index_id == index, model_index_id], 
                       'notes' = config[model_index_id == index, notes])

# Register and sendoff
run_id <- register_stgpr_model(paste0(work_dir, "FILEPATH"), index)
tracking[model_index_id == index, run_id := run_id]
stgpr_sendoff(tracking[model_index_id == index, run_id], "proj_stgpr")

# Master track
master_track <- rbind(master_track, tracking)
fwrite(master_track, paste0(work_dir, "FILEPATH"))

# Plot it up
save_dir <- "FILEPATH"
plot_gpr(master_track[model_index_id == index, run_id], output.path = paste0(save_dir, master_track[model_index_id == index, run_id], "_plots.pdf"), no.subnats = T)
