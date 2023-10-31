################################################################################
## DESCRIPTION: Register and sendoff STGPR models for age-pattern
## INPUTS:
## OUTPUTS:
## AUTHOR:
## DATE: 05/11/20
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
source('FILEPATH')
source('FILEPATH')

# Read in config and master tracker
config <- fread(paste0(work_dir, "FILEPATH"))
master_track <- fread(paste0(work_dir, "FILEPATH"))

# Set index ID object
index = 11

# Create live tracking sheet
tracking <- data.table(model_index_id = config[model_index_id == index, model_index_id], notes = config[model_index_id == index, notes])

# Register
tracking[model_index_id == index, run_id := register_stgpr_model(paste0(work_dir, "FILEPATH"), index)]

# Sendoff
stgpr_sendoff(tracking[model_index_id == index, run_id], project = "proj_stgpr")

# Save tracking sheet
master_track <- rbind(master_track, tracking)
fwrite(master_track, paste0(work_dir, "FILEPATH"))
