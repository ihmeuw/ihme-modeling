# Title: ST-GPR Sendoff
# Purpose: launch ST-GPR using specified config and data (either from a flat file or xwalk)
# Input: config edited in Excel and bundle data 7919
# Output: ST-GPR inpatient utilization rate per capita model
# Author: USERNAME

# Source ST-GPR functions
source("FILEPATH")

# Register and launch ST-GPR model
path_to_config <- paste0("FILEPATH")
run_id <- register_stgpr_model(path_to_config, model_index_id = 1)
stgpr_sendoff(run_id, "proj_hospital")

# Check model status every minute (so we don't overwhelm the db!) until it finishes
# Use Jobmon GUI https://jobmon-gui.ihme.washington.edu/ to monitor progress
status <- get_model_status(run_id)
while (status == 2) {
  cat("Model still running! Waiting a minute...\n")
  Sys.sleep(60)
  status <- get_model_status(run_id, verbose = TRUE)
}

print("Model finished!")
