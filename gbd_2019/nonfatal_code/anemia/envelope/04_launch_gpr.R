########################################################################################################################
## Project: Anemia
## Purpose: Launch and Save ST-GPR for Anemia GPR models
########################################################################################################################

########################################################################################################################
################################################# SET-UP ###############################################################
########################################################################################################################

# Clear workspace
rm(list=ls())
username <- Sys.info()[["user"]]
library(data.table)
library(dplyr)
########################################################################################################################
############################################ DEFINE FUNCTION ###########################################################
########################################################################################################################

launch_gpr <- function(group, best=FALSE, notes="", cluster_proj="proj_anemia", draws=1000, decomp_step=NA) {

  # Set directories and load functions
  central_root <- 'FILEPATH'
  setwd(central_root)
  source('FILEPATH/register.R')
  source('FILEPATH/sendoff.R')
  source('FILEPATH/utility.r')
  
  ref <- fread("FILEPATH/paths.csv")[me_group==group,] #path to file containing reference paths for every group/me
  config_path <- ref[,config_path]
  config_file <- fread(config_path)[decomp_step==decomp_step&is_best==1,]
  if (config_file[duplicated(me_name),] %>% nrow > 0) stop(paste0("BREAK | You marked best multiple models for the same me_name: ",
                                                                  toString(config_file[duplicated(me_name), me_name])))
  RUNS <- NULL
  notes <- notes

## Prep Data for Upload
  all_me_name <- as.vector(unlist(strsplit(ref$me_names, ",")))

  all_my_model_id <- NULL
  all_my_model_id <- lapply( 1:length(all_me_name), function(x) c(all_my_model_id, config_file[me_name==all_me_name[x], model_index_id]) )
  all_data_notes  <- rep(c(notes), length(all_me_name))
  all_crosswalk_versions <- as.numeric(as.vector(unlist(strsplit(ref$crosswalk_version_id, ","))))
  data_dir <- ref[,data_dir]

## Set ST-GPR Launch Settings
  nparallel       <- 50                                       
  logs            <- file.path("FILEPATH", username)  

## Batch launch all models 
  for (xx in 1:length(all_me_name)) {
  
  me_name     <- all_me_name[xx]
  my_model_id <- all_my_model_id[[xx]]
  data_notes  <- all_data_notes[xx]
  crosswalk_version_id <- all_crosswalk_versions[xx]
  mark_best   <- best

  ## Assign models and run IDs
  run_ids <- lapply(1:length(me_name), function(x) {
    register_stgpr_model(path_to_config=config_path,
                         model_index_id = my_model_id)
  })
  RUNS <- c(RUNS, run_ids)
  
  ## Launch ST-GPR for each run ID
  mapply(stgpr_sendoff, run_ids, cluster_proj, logs, nparallel)

  # ## Save log to log file
  logs_path <- ref[,logs_path]
  logs_df   <- data.frame("date"         = format(lubridate::with_tz(Sys.time(), tzone="America/Los_Angeles"), "%m_%d_%y_%H%M"),
                          "me_name"      = me_name,
                          "decomp_step"  = decomp_step,
                          "crosswalk_version_id"  = crosswalk_version_id,
                          "run_id"       = run_ids[[1]],
                          "data_id"      = NA,
                          "model_id"     = "",
                          #"data_path"    = data_path,
                          "is_best"      = best,
                          "notes"        = data_notes,
                          "status"       = "",
                          "best_gbd2016" = NA,
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
  print(paste0("Log file saved for ", me_name, " under run_id ", run_ids[[1]]))
}
}

## END SCRIPT