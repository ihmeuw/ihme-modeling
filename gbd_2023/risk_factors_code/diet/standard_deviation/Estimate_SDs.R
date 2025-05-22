################################################################################
## DESCRIPTION ## Apply regression to GBD 2023 exposure draws to get SDs, then upload
## INPUTS ## CSV of betas and correction factors 
## OUTPUTS ## CSV of new SD draws, by location
## AUTHOR ## 
## DATE CREATED ## 

## REVISED
## AUTHOR ## 
## DATE ## 
## SUMMARY ## Separated out the launch script and reorganized for so that run script should not be edited.
################################################################################

rm(list = ls())

# System info
os <- Sys.info()[1]
user <- Sys.info()[7]

# Drives 
j <- if (os == "Linux") "FILEPATH" else if (os == "Windows") "J:/"
h <- if (os == "Linux") paste0("FILEPATH", user, "/") else if (os == "Windows") "H:/"

# Base filepaths
work_dir <- paste0(j, "FILEPATH")
share_dir <- "FILEPATH" # only accessible from the cluster
code_dir <- if (os == "Linux") paste0("FILEPATH", user, "/") else if (os == "Windows") ""


## LOAD DEPENDENCIES -----------------------------------------------------
source(paste0(code_dir, '"FILEPATH"/cluster_utils.R'))
library(dplyr)
library(readstata13)
library(readxl)
library(data.table)
source("FILEPATH/get_location_metadata.R")
source("FILEPATH/get_draws.R")
source("FILEPATH/save_results_epi.R")


#---------------------------------------------------
if(interactive()){
  diet_ids <-fread(paste0(out_dir, "diet_ids.csv"))
  me <- diet_ids[me_name == me, me]
  loc_id <- 10
  save <- 0
  draws <- 1000
  sd_id <- diet_ids[me_name == me, sd_id]
  exp_id <- diet_ids[me_name == me, new_exposure_me]
  mv_id <- diet_ids[me_name == me, mv_id]
  gbd_round <- "gbd2023"
}else{
  args <-commandArgs(trailingOnly = TRUE)
  print(args)
  map_path <-args[1]
  print(paste0("map_path: ", map_path))
  me <- args[2]
  print(paste0("me: ", me))
  sd_id <- args[3]
  print(paste0("sd_id: ", sd_id))
  exp_id <- args[4]
  print(paste0("exp_id: ", exp_id))
  mv_id <- args[5] 
  print(paste0("mv_id: ", mv_id))
  root_save_dir <- args[6]
  print(paste0("root_save_dir: ", root_save_dir))
  beta_csv <- args[7]
  print(paste0("beta_csv: ", beta_csv))
  best_runs_csv <- args[8]
  print(paste0("best_runs_csv: ", best_runs_csv))
  gbd_round <- args[9]
  print(paste0("gbd_round: ", gbd_round))
  gbd_release_id <- args[10]
  print(paste0("gbd_release_id: ", gbd_release_id))
  max_year <- args[11]
  print(paste0("max_year: ", max_year))
  draws <- as.numeric(args[12])
  print(paste0("draws: ", draws))
  best <- args[13]
  print(paste0("best: ", best))
  save <- as.numeric(args[14])
  print(paste0("save: ", save))
  
  params <- fread(map_path)

  out_dir <- paste0(share_dir, "diet/exposure/", gbd_round, "/modeling/mean_sd/")
  print(paste0("out_dir: ", out_dir)) 
}

# Set up 
save_dir <- paste0(root_save_dir, me ,"_", sd_id)
if(!dir.exists(save_dir)){dir.create(save_dir)}
print(paste0("save_dir: ", save_dir)) 

# Get SD correction factors
if(!save){
  sd_corrections <- fread(beta_csv)
  regression_intercept <- sd_corrections[me_name==me, `intercept (log space)`]
  regression_beta <- sd_corrections[me_name==me, `beta (log space)`]
  sd_adjustment <- sd_corrections[me_name==me, `correction factor (normal space)`]
  
  # Get draws for given location linked ot task ID
  exposure_raw <- get_draws('modelable_entity_id', 
                            gbd_id = exp_id,
                            version_id = mv_id,
                            year_id = seq(1990,2024),
                            sex_id = c(1, 2), 
                            age_group_id = c(seq(10, 20), 30, 31, 32, 235), 
                            release_id = gbd_release_id,
                            num_workers = 10,
                            source = "epi")

  print("Got Draws")
  
  #-----------------------------------------------------------------------------#
  
  # Transform exposure draws into SD using regression coefficient and SD adjustment factor
  draw_cols <- paste0("draw_", 0:(draws - 1))

  # if 0, offset by 1e-6
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

    print("exposure_raw is ready")



    loc_id <- unique(exposure_raw$location_id) #based on all locs in get draws.

    ##loc_id loop for save
    for (i in (loc_id)){
      print(i)
      exposure_raw_loc <- exposure_raw[location_id == i]
      write.csv(exposure_raw_loc, paste0(save_dir, "/", i, ".csv"), row.names = F)
      print(paste0("file for ", i, " was saved"))
    }
 
     
    #-----------------------------------------------------------------------------#
}


if(save){

  best_run_ids <- fread(best_runs_csv)   #file with best run_ids
 if(me == "diet_grains"){row <- best_run_ids[me_name == "diet_whole_grains"]
 }else{
    row <- best_run_ids[me_name == me]
  }

  print(paste0("save_dir: ", save_dir)) 
  
  save_results_epi(input_dir = save_dir,
                   input_file_pattern = "{location}.csv",
                   modelable_entity_id = sd_id,
                   release_id= gbd_release_id,
                   description = paste0("SDs based on 2021 mean-sd regression for risk: ", me,  ", round: ", gbd_round),
                   mark_best = best,
                   year_id = seq(1990,2024),
                   measure_id = 19,
                   n_draw= draws,
                   crosswalk_version_id = row$crosswalk_version_id,
                   bundle_id = row$bundle_id)

}
