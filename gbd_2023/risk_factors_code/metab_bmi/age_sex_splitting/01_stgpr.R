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
source("FILEPATH//stgpr/api/public.R") # this is the new path for STGPR functions

# Read in config and master tracker
config <- fread(paste0(work_dir, "age_pattern_config.csv"))
master_tracker <- fread(paste0(work_dir, "age_pattern_tracking.csv"))

# Set index ID object
index <- c(1:4)
for( i in index){
  # Create live tracking sheet
  tracking <- data.table(model_index_id = config[model_index_id == i, model_index_id], notes = config[model_index_id == i, notes])
  
  # Register
  r <- register_stgpr_model(paste0(work_dir, "age_pattern_config.csv"), i)
  tracking[model_index_id == i, run_id := r]
  
  # Sendoff
  stgpr_sendoff(tracking[model_index_id == i, run_id], project = "PROJECT")
  
  # Save tracking sheet
  master_tracker <- rbind(master_track, tracking)
}

# Save tracking sheet
fwrite(master_tracker, paste0(work_dir, "FILEPATH"))
