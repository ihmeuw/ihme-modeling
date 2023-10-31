#----HEADER-------------------------------------------------------------------------------------------------------------
# Author:  USER
# Date:    November 2017
# Purpose: Calculate measles outbreak shocks
# Run:     FILEPATH
#***********************************************************************************************************************


########################################################################################################################
##### START-UP #########################################################################################################
########################################################################################################################


#----SETTINGS-----------------------------------------------------------------------------------------------------------
### directories
code_home <- file.path("/FILEPATH/measles")

### load functions
"/FILEPATH/collapse_point.R" %>% source
"/FILEPATH/cluster_tools.r" %>% source
"/FILEPATH/utility.r" %>% source

### options
me_id <- 1436 #GBD 2020 me_id req'd for ST-GPR
cols <- c("location_id", "year_id", "age_group_id", "sex_id")

### prep ST-GPR
if (run_shocks_adjustment | run_case_notification_smoothing) {
  ### load special functions
  central_root <- "FILEPATH"
  setwd(central_root)
  source("FILEPATH/register.R")
  source('FILEPATH/sendoff.R')
  
  ### load config
  config_path <- file.path("/FILEPATH/gpr_config_2020.csv")
  
  ### settings
  holdouts        <- 0
  ko_pattern      <- "country"
  cluster_project <- "proj_cov_vpd"
  draws           <- 1000
  nparallel       <- 40
  slots           <- 5
  master_slots    <- 8
  logs            <- file.path("FILEPATH", username)
}
#***********************************************************************************************************************


#----PREP---------------------------------------------------------------------------------------------------------------
### prep data function -- only for SHOCKS function, not case notif smoothing
prep_data <- function(measure=NULL, case_notif_data, dataset) {
  
  ### stop if don't specify measure
  if (is.null(measure)) stop("BREAK | must specify measure to prep by (ratio or difference)")
  
  ### pull modeled case counts and uncertainty
  modeled_cases <- fread(file.path(j.version.dir, dataset))
  
  cases_mean <- collapse_point(modeled_cases, variance=TRUE)
  setnames(cases_mean, "mean", "cases_model")
  
  ### get WHO case notification data
  setnames(case_notif_data, "cases", "cases_notifications")
  
  ### keep only data with both case notifications and modeled estimates (all years) for overlapping geographies
  cases <- merge(cases_mean[, .(location_id, year_id, cases_model, variance)], case_notif_data[, .(location_id, year_id, cases_notifications)], by=c("location_id", "year_id"), all=TRUE)
  cases <- cases[!is.na(cases_model) & !is.na(cases_notifications), ]
  
  # calculate as difference
  cases[, difference := cases_model - cases_notifications]
  #cases[, variance_difference := variance]
  cases <- merge(cases, pop_u5, by=c("location_id", "year_id"), all.x=TRUE)
  cases[, inc_rate := cases_notifications / pop]
  cases[, variance_difference := (inc_rate * (1 - inc_rate)) / pop]
  
  # calculate as ratio
  cases[, ratio := cases_model / cases_notifications]
  cases[, variance_ratio := sqrt( ( sqrt(variance) / cases_notifications ) ^ 2)]
  
  ### prep for st-gpr upload
  cases[, me_id := me_id]
  cases[, nid := 999999]
  cases[, age_group_id := 22]
  cases[, sex_id := 3]
  cases[, sample_size := NA]
  
  ### run model from ratio or difference
  cases[, data := get(measure)]
  cases[, variance := get(paste0("variance_", measure))]
  cases <- cases[, .(me_id, nid, location_id, year_id, age_group_id, sex_id, data, variance, sample_size)]
  
  return(cases)
  
}
#***********************************************************************************************************************


########################################################################################################################
##### PART ONE: CASE NOTIFICATION FILL #################################################################################
########################################################################################################################


case_notification_smoothing <- function(...) {
  #----PREP DATA----------------------------------------------------------------------------------------------------------
  ### get case notifications to smooth
  case_notif <- prep_case_notifs(add_subnationals=TRUE)
  # calculate incidence rate
  case_notif <- merge(case_notif, pop_u5, by=c("location_id", "year_id"), all.x=TRUE)
  case_notif[, data := cases / pop]
  case_notif[, me_id := me_id]
  case_notif[, nid := 999999]
  case_notif[, age_group_id := 22]
  case_notif[, sex_id := 3]
  case_notif[, sample_size := NA]
  case_notif[, is_outlier := 0]
  case_notif[data==0, data := 0.00001]  
  case_notif[, variance := (data * (1 - data)) / pop]
  case_notif <- case_notif[, .(me_id, nid, location_id, year_id, age_group_id, sex_id, data, variance, sample_size, is_outlier)]
  case_notif[, measure_id := 19] # measure id for continuous
  # meet requirement that column with data be named val
  setnames(case_notif, "data", "val")
  #remove outbreaks to prevent inflation of future years without data
  case_notif <- case_notif[(location_id %in% outbreak_loc_ids & year_id == 2019), val:= NA]
  
  ### save for ST-GPR smoothing
  data_path_notif <- file.path("FILEPATH/", paste0(custom_version, "_case_notifs_as_incidence.csv"))  #save copy of input CN to version dir for posterity
  gpr_path_notif <- file.path(code_home, "shocks/case_notifs_as_incidence.csv") #save a copy of gpr input to share for stgpr. overwrite this file each time run
  write.csv(case_notif, data_path_notif, row.names=FALSE)
  write.csv(case_notif, gpr_path_notif, row.names = FALSE)
  #***********************************************************************************************************************
  
  
  #----LAUNCH MODEL-------------------------------------------------------------------------------------------------------
  ### prep for data upload
  data_notes  <- "JRF 2020 data, no prelim 2020 case notifs included"
  data_path   <- data_path_notif
  
  ### prep config file path
  cfg <- fread(config_path)
  cfg[, path_to_data := as.character(path_to_data)]
  cfg[, path_to_data := gpr_path_notif] 
  
  # pull covariate to use as custom covariate if not using current best version in 01 script
  if(!is.null(custom_mcv1_coverage)){
    if(use_lagged_covs){
      j_path_to_custom_covar <- file.path("FILEPATH", paste0(custom_version, "_custom_covar.csv")) # keep covar values used for posterity
      share_path_to_custom_covar <- file.path(code_home, "shocks/custom_covar.csv") # overwrite each time, but must save here for stgpr to read
      
      custom_covar <- get_covariate_estimates(covariate_id = 2309, model_version_id = custom_mcv1_coverage, release_id = release_id)
      setnames(custom_covar, "mean_value", "cv_lagged_mcv1")
      write.csv(custom_covar, j_path_to_custom_covar, row.names=FALSE)
      write.csv(custom_covar, share_path_to_custom_covar, row.names = FALSE)
      }
    }
    
  
  if (use_lagged_covs) {
    cfg[, `:=` (gbd_covariates = "lagged_mcv1_coverage_prop", stage_1_model_formula = "data ~ lagged_mcv1_coverage_prop + (1|level_1) + (1|level_2) + (1|level_3)")]
    
    if(!is.null(custom_mcv1_coverage)){
      cfg[, `:=` (study_covs = "cv_lagged_mcv1", gbd_covariates = NA, stage_1_model_formula = "data ~ cv_lagged_mcv1 + (1|level_1) + (1|level_2) + (1|level_3)", path_to_custom_covariates = share_path_to_custom_covar)]
    }
    
  } else {
    cfg[, `:=` (gbd_covariates = "measles_vacc_cov_prop", stage_1_model_formula = "data ~ measles_vacc_cov_prop + (1|level_1) + (1|level_2) + (1|level_3)")]
  }
  cfg[, description := copy(data_notes)]
  
  
  ## let st-gpr pick offset?
  if(manual_stgpr_offset){
    cfg[, transform_offset := 0.001]
    write.csv(cfg, config_path, row.names=FALSE)
  } else {
    cfg[, transform_offset := NA]
    write.csv(cfg, config_path, row.names=FALSE)
  }
  
  ### launch models!
  if (decomp) {
    run_id <- register_stgpr_model(path_to_config = config_path)
    
    stgpr_sendoff(run_id=run_id, project=cluster_project, nparallel=nparallel)
    
  } else {
    ### register data
    my_data_id  <- register_data(me_name=me_name, path=data_path, user_id=username, notes=data_notes, is_best=0, bypass=TRUE)
    
    ### load config file to get model_ids and run_ids assigned
    run_ids     <- lapply(1:length(my_data_id), function(x) {
      data_id <- my_data_id[x]
      id <- register.config(path        = config_path,
                            my.model.id = my_model_id,
                            data.id     = data_id)
      return(id$run_id)
    })
    
    ### run entire pipeline for each new run_id
    mapply(submit.master, run_ids, holdouts, draws, cluster_project, nparallel, slots, model_root, logs, master_slots, ko_pattern)
  }
  
  ### save log to gpr log file
  logs_path <- file.path("FILEPATH/run_log_2020.csv")   
  logs_df   <- data.frame("date"        = format(lubridate::with_tz(Sys.time(), tzone="America/Los_Angeles"), "%m_%d_%y"),
                          "me_id"     = me_id,
                          "model_source"= "case_notifs",
                          "run_id"      = ifelse(decomp, run_id[[1]], run_ids[[1]]), 
                          "data_id"     = ifelse(decomp, "", my_data_id),
                          "model_id"    = "",
                          "data_path"   = data_path,
                          "is_best"     = 0,
                          "notes"       = data_notes,
                          "status"      = "")
  logs_file <- fread(logs_path) %>% rbind(., logs_df, fill=TRUE)
  write.csv(logs_file, logs_path, row.names=FALSE)
  # print(paste0("Log saved for run_id - ", run_ids[[1]], "; data - ", data_path, "; me - ", me_name))
  print(paste0("Log saved for run_id - ", run_id[[1]], "; data - ", data_path, "; me - ", me_id))  
  #***********************************************************************************************************************
  
  
  #----POST-PROCESSING----------------------------------------------------------------------------------------------------
  ### hold until job finishes
  print("Hold on Master")
  job_hold(paste0("MASTER_", ifelse(decomp, run_id[[1]], run_ids[[1]])))
  print("Master done")
  
  ### pull smoothed notifs and replace missingness with ST-GPR output
  # get clean case notifications
  case_notif <- prep_case_notifs(add_subnationals=TRUE)
  # create square dataset of all years for countries included in this set
  notif_square <- CJ(location_id=unique(case_notif$location_id), year_id=1980:year_end) %>% as.data.table
  # merge on case notifications
  notifs <- merge(notif_square, case_notif, by=c("location_id", "year_id"), all.x=TRUE)
  # get model output
  gpr_cases <- lapply(list.files(file.path("FIELPATH",
                                           ifelse(decomp, run_id[[1]], run_ids[[1]]),
                                           "draws_temp_0"), 
                                 full.names=TRUE), fread) %>% rbindlist
  # collapse draws and will resimulate draws in 01 script so that method for generating draws is same for admin data and cases from st-gpr
  gpr_cases_q <- collapse_point(gpr_cases)[, .(location_id, year_id, mean)]
  # calculate back out cases
  gpr_cases_q <- merge(gpr_cases_q, pop_u5, by=c("location_id", "year_id"), all.x=TRUE)
  
  ## back-substract ST-GPR offset manually
  offset <- model_load(run_id, "parameters")$transform_offset
  gpr_cases_q <- gpr_cases_q[, mean := mean - offset]
  gpr_cases_q[, mean := mean * pop]
  
  # replace missingness in notifications with ST-GPR output
  notifs <- merge(notifs, gpr_cases_q, by=c("location_id", "year_id"), all.x=TRUE)
  notifs[, notifications := cases]
  
  # round to whole number of cases
  notifs[is.na(notifications), notifications := round(mean, 0)]
  setnames(notifs, c("cases", "mean", "notifications"), c("case_notifications", "st_gpr_output", "combined"))
  
  
  ### save filled case notifications by location_id
  gpr_save_dir <- paste0("/FILEPATH/case_notifications_", custom_version, "/")
  if(!dir.exists(gpr_save_dir)) dir.create(gpr_save_dir)
  notif_locations <- unique(notifs$location_id)
  lapply(notif_locations, function(x) { fwrite(notifs[location_id==x, ],
                                               paste0(gpr_save_dir, x, ".csv"), row.names=FALSE) })
  print("Back to 01_natural_history_model")
  #***********************************************************************************************************************
}

### NOTHING BEYOND THIS POINT HAS BEEN UPDATED FOR ST-GPR (03/06/2019)
########################################################################################################################
##### PART TWO: SHOCKS ADJUSTMENT ######################################################################################
########################################################################################################################


shocks_adjustment <- function(...) {
  #----PREP DATA----------------------------------------------------------------------------------------------------------
  ### get ratio/difference between case notifications and modeled estimates
  case_notif <- prep_case_notifs()
  case_notif_smooth <- lapply(list.files("/FILEPATH", full.names=TRUE), fread) %>%
    rbindlist %>% setnames(., "st_gpr_output", "cases") %>% .[, .(location_id, year_id, cases)]
  data <- prep_data(measure="difference", case_notif_data=case_notif_smooth, dataset="01_case_predictions_from_model.csv")
  
  ### save for ST-GPR smoothing
  data_path_diff <- file.path(home, "data/to_model", paste0(custom_version, "_shocks_difference.csv"))
  write.csv(data, data_path_diff, row.names=FALSE)
  #***********************************************************************************************************************
  
  
  #----LAUNCH MODEL-------------------------------------------------------------------------------------------------------
  ### prep for data upload
  my_model_id <- fread(config_path)[best==1 & model_source=="difference", my_model_id]
  data_notes  <- "New run with 2017 hierarchy and new ST-GPR, lsvid 297, updated for 11.30 run"
  data_path   <- data_path_diff
  
  ### register data
  my_data_id  <- register_data(me_name=me_name, path=data_path, user_id=username, notes=data_notes, is_best=0, bypass=TRUE)
  
  ### load config file to get model_ids and run_ids assigned
  run_ids     <- lapply(1:length(my_data_id), function(x) {
    data_id <- my_data_id[x]
    id <- register.config(path        = config_path,
                          my.model.id = my_model_id,
                          data.id     = data_id)
    return(id$run_id)
  })
  
  ### run entire pipeline for each new run_id
  mapply(submit.master, run_ids, holdouts, draws, cluster_project, nparallel, slots, model_root, logs, master_slots, ko_pattern)
  
  ### save log to gpr log file
  logs_path <- file.path(home, "FILEPATH/run_log.csv")
  logs_df   <- data.frame("date"        = format(lubridate::with_tz(Sys.time(), tzone="America/Los_Angeles"), "%m_%d_%y"),
                          "me_name"     = me_name,
                          "model_source"= "difference",
                          "my_model_id" = my_model_id,
                          "run_id"      = run_ids[[1]],
                          "data_id"     = my_data_id,
                          "model_id"    = "",
                          "data_path"   = data_path,
                          "is_best"     = 0,
                          "notes"       = data_notes,
                          "status"      = "")
  logs_file <- fread(logs_path) %>% rbind(., logs_df, fill=TRUE)
  write.csv(logs_file, logs_path, row.names=FALSE)
  print(paste0("Log saved for run_id - ", run_ids[[1]], "; data - ", data_path, "; me - ", me_name))
  #***********************************************************************************************************************
  
  
  #----POST-PROCESSING----------------------------------------------------------------------------------------------------
  ### hold until ST-GPR model finishes
  job_hold(paste0("MASTER_", run_ids[[1]]))
  
  ### save to cluster folder
  output <- list.files(file.path("FILEPATH", run_ids[[1]], "draws_temp_1"))
  lapply(output, function(x) { fread(file.path("/FILEPATH", run_ids[[1]], "draws_temp_1", x)) %>%
      collapse_point %>%
      write.csv(., file.path(cl.shocks.dir, x), row.names=FALSE) })
  
  ### save log of run_id
  write.table(paste0("run_id used for shocks difference adjustment is ", run_ids[[1]]), file.path(j.version.dir.logs, "shocks_run_id.txt"))
  #***********************************************************************************************************************
}
