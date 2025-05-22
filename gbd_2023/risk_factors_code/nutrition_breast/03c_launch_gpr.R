########################################################################################################################
## Project: Breastfeeding
## Purpose: Launch and Save ST-GPR for All Indicators
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

launch_gpr <- function(group, best=FALSE, notes="", cluster_proj="PROJECT", draws=1000, decomp_step=NA) {
  
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
  data_dir <- ref[,data_dir]
  
  ## Set ST-GPR Launch Settings
  nparallel       <- 50                                       
  logs            <- file.path("FILEPATH", username)  
  
  ## Batch launch all models 
  for (xx in 1:length(all_me_name)) {
    
    me          <- all_me_name[xx]
    my_model_id <- all_my_model_id[[xx]]
    data_notes  <- all_data_notes[xx]
    mark_best   <- best
    
    ## Assign models and run IDs
    run_ids <- lapply(1:length(me), function(x) {
      register_stgpr_model(path_to_config=config_path,
                           model_index_id=my_model_id)
    })
    RUNS <- c(RUNS, run_ids)
    
    ## Launch ST-GPR for each run ID
    mapply(stgpr_sendoff, run_ids, cluster_proj, logs, nparallel)
    
    # ## Save log to log file
    logs_path <- ref[,logs_path]
    logs_df   <- data.frame("date"         = format(lubridate::with_tz(Sys.time(), tzone="America/Los_Angeles"), "%m_%d_%y_%H%M"),
                            "me_name"      = me,
                            "gbd_round_id" = 7,
                            "decomp_step"  = decomp_step,
                            "bundle_id"    = config_file[me_name==me,bundle_id],
                            "crosswalk_version_id"  = config_file[me_name==me,crosswalk_version_id],
                            "modelable_entity_id"   = config_file[me_name==me,modelable_entity_id],
                            "my_model_id"  = my_model_id,
                            "run_id"       = run_ids[[1]],
                            "data_id"      = NA,
                            "is_best"      = best,
                            "notes"        = data_notes,
                            "most_recent"  = 1)
    if (best) {
      logs_file <- fread(logs_path)
      logs_file$is_best[logs_file$me_name==me] <- 0
      logs_file$most_recent[logs_file$me_name==me] <- 0
      logs_file <- rbind(logs_file,logs_df,fill=TRUE)
    } else {
      logs_file <- fread(logs_path)
      logs_file$most_recent[logs_file$me_name==me] <- 0
      logs_file <- rbind(logs_file,logs_df,fill=TRUE)
    }
    write.csv(logs_file, logs_path, row.names=FALSE)
    print(paste0("Log file saved for ", me, " under run_id ", run_ids[[1]]))
  }
}

## END SCRIPT