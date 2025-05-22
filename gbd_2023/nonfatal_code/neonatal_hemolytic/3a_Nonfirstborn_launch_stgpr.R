################################################################################
## Purpose: Launch stgpr model for nonfirstborn prevalence by mother's age
## Input:   Bundle 7667
## Output:  STGPR model results
################################################################################

# setup ------------------------------------------------------------------------
source('FILEPATH')

# launch stgpr -----------------------------------------------------------------
config_filepath <- 'FILEPATH'
stgpr_version_id <- register_stgpr_model(path_to_config = config_filepath,
                                         model_index_id = 14)

stgpr_sendoff(stgpr_version_id,
              project = 'proj_nch',
              nparallel = 50,
              log_path = 'FILEPATH')

status <- get_model_status(stgpr_version_id)
while (status == 2) {
  cat("Model still running! Wait a minute...\n")
  Sys.sleep(60)
  status <- get_model_status(stgpr_version_id, verbose = TRUE)
}

print("Model finished!")