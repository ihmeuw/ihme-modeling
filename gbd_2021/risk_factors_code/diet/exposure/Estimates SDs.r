
################################################################################
## DESCRIPTION ## Apply regression to GBD 2020 exposure draws to get SDs
################################################################################

rm(list = ls())
# System info
os <- Sys.info()[1]
user <- Sys.info()[7]
# Drives 
j <- if (os == "Linux") "FILEPATH" else if (os == "Windows") "FILEPATH"
h <- if (os == "Linux") paste0("FILEPATH/", user, "/") else if (os == "Windows") "FILEPATH"
# Base filepaths
work_dir <- paste0(j, 'FILEPATH')
share_dir <- '/FIELPATH'
code_dir <- if (os == "Linux") paste0("FILEPATH", user, "/") else if (os == "Windows") ""
out_dir <- paste0(share_dir, "FILEPATH")

## LOAD DEPENDENCIES -----------------------------------------------------
source(paste0(code_dir, 'FILES/primer.R'))
source(paste0(code_dir, 'FILES/cluster_utils.R'))
library(dplyr)
library(readstata13)
library(readxl)
library(data.table)
source("FIELPATH/get_location_metadata.R")
source("FILEPATH/get_draws.R")


# constant arguments
beta_csv <- paste0(out_dir, "gbd_2020_betas_corrections.csv")
decomp_step <- "iterative"
gbd_round_id <- 7
max_year <- 2022

#---------------------------------------------------
if(interactive()){
  diet_ids <-fread("FIELPATH/diet_ids.csv")
  me <- "diet_transfat"
  loc_id <- 75
  save <- 0
  sd_id <- diet_ids[me_name == me, sd_id]
  exp_id <- diet_ids[me_name == me, new_exposure_me]
  
}else{
  args <-commandArgs(trailingOnly = TRUE)
  print(args)
  map_path <-args[1]
  me <- args[2]
  sd_id <- args[3]
  exp_id <- args[4]
  save <- as.numeric(args[5])
  params <- fread(map_path)
  task_id <- Sys.getenv("SGE_TASK_ID")
  loc_id <- params[task_num == task_id, location] 
}

# Set up 
save_dir <- paste0(out_dir, "/for_upload/", me ,"_", sd_id)
if(!dir.exists(save_dir)){dir.create(save_dir)}

# Get SD correction factors

if(!save){
  sd_corrections <- fread(beta_csv)
  sd_corrections[me_name=="diet_calcium", me_name := "diet_calcium_low"]
  regression_intercept <- sd_corrections[me_name==me, `intercept (log space)`]
  regression_beta <- sd_corrections[me_name==me, `beta (log space)`]
  sd_adjustment <- sd_corrections[me_name==me, `correction factor (normal space)`]
  
  # Get GBD 2020 drawsfor given location
  exposure_raw <- get_draws('modelable_entity_id', 
                            gbd_id = exp_id,
                            location_id=loc_id,
                            year_id=seq(1990, max_year, by = 1), 
                            sex_id=c(1, 2), 
                            age_group_id=c(seq(10, 20), 30, 31, 32, 235), 
                            gbd_round_id = gbd_round_id,
                            decomp_step = decomp_step,
                            num_workers = 10,
                            source = "epi")

  # Tranform exposure draws into SD using regression coefficient and SD adjustment factor
  draw_cols <- paste0("draw_",0:999)
  # if 0, offset by a little amount
  exposure_raw[, (draw_cols):= lapply(.SD, function(x) ifelse(x ==0, 1e-6 ,x)), .SDcols = draw_cols]
  # first log the means
  exposure_raw[, (draw_cols):= lapply(.SD, function(x) log(x)), .SDcols = draw_cols]
  # then apply the beta and intercept
  exposure_raw[, (draw_cols):= lapply(.SD, function(x) regression_intercept + x*regression_beta), .SDcols = draw_cols]
  # then move to normal space
  exposure_raw[, (draw_cols):= lapply(.SD, function(x) exp(x)), .SDcols = draw_cols]
  # then apply the SD correction factor
  exposure_raw[, (draw_cols):= lapply(.SD, function(x) x*sd_adjustment), .SDcols = draw_cols]
  
  # Write location specific file
  exposure_raw[, `:=` (measure_id = NULL, metric_id = NULL, log_regression_beta = regression_beta, log_regression_intercept = regression_intercept, sd_adj_factor = sd_adjustment)]
  setnames(exposure_raw, c("model_version_id", "modelable_entity_id"), c("exposure_mvid", "exposure_meid"))
  
  write.csv(exposure_raw, paste0(save_dir, "/", loc_id, ".csv"), row.names = F)
}

