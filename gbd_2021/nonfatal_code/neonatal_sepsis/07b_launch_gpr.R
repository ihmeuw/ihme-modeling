########################################################################################################################
## Project: Neonatal Sepsis
## Purpose: Launch ST-GPR for Neonatal Sepsis
########################################################################################################################

########################################################################################################################
################################################# SET-UP ###############################################################
########################################################################################################################

# Clear workspace
rm(list=ls())
username <- Sys.info()[["user"]]
library(data.table)
library(dplyr, lib.loc="FILEPATH")
library(DBI, lib.loc="FILEPATH")
library(RMySQL, lib.loc="FILEPATH")
library(ini, lib.loc="FILEPATH")

########################################################################################################################
############################################ DEFINE FUNCTION ###########################################################
########################################################################################################################

launch_gpr <- function(best=FALSE, notes="", cluster_proj="PROJECT", draws=1000, decomp_step=NA) {

  # Set directories and load functions
  central_root <- 'FILEPATH'
  setwd(central_root)
  source('FILEPATH/register.R')
  source('FILEPATH/sendoff.R')
  source('FILEPATH/utility.r')
  
  config_path <- "FILEPATH/neonatal_sepsis_model_db.csv"
  config_file <- fread(config_path)[decomp_step==decomp_step&is_best==1,]
  if (config_file[duplicated(me_name),] %>% nrow > 0) stop(paste0("BREAK | You marked best multiple models for the same me_name: ",
                                                                  toString(config_file[duplicated(me_name), me_name])))
  RUNS <- NULL
  notes <- notes

## Set ST-GPR Launch Settings
  nparallel       <- 50                                       
  logs            <- file.path("FILEPATH/", username)  

## Batch launch all models 
  
  me_name     <- "neonatal_sepsis"
  my_model_id <- config_file$model_index_id
  data_notes  <- notes
  mark_best   <- best

  ## Assign models and run IDs
  run_id <- register_stgpr_model(path_to_config=config_path,
                         model_index_id = my_model_id)

  ## Launch ST-GPR for each run ID
  mapply(stgpr_sendoff, run_id, cluster_proj, logs, nparallel)

  # ## Save log to log file
  logs_path <- "FILEPATH/neonatal_sepsis_run_log.csv"
  logs_df   <- data.frame("date"         = format(lubridate::with_tz(Sys.time(), tzone="America/Los_Angeles"), "%m_%d_%y_%H%M"),
                          "me_name"      = me_name,
                          "decomp_step"  = decomp_step,
                          "crosswalk_version_id"  = config_file$crosswalk_version_id,
                          "run_id"       = run_id,
                          "is_best"      = best,
                          "notes"        = notes,
                          "most_recent"  = 1)
  if (best) {
    logs_file <- fread(logs_path)
    logs_file$is_best[logs_file$me_name==me_name] <- 0
    logs_file$most_recent[logs_file$me_name==me_name] <- 0
    logs_file <- rbind(logs_file,logs_df,fill=TRUE)
  } else {
    logs_file <- fread(logs_path)
    logs_file$most_recent[logs_file$me_name==me_name] <- 0
    logs_file <- rbind(logs_file,logs_df,fill=TRUE)
  }
  write.csv(logs_file, logs_path, row.names=FALSE)
  print(paste0("Log file saved for ", me_name, " under run_id ", run_id))
}


## END SCRIPT