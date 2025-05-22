# Purpose: estimate wasting for younger age groups based on calculation from GBD 2013 
#
#############################################################################################
###'                                [General Set-Up]                                      ###
#############################################################################################

rm(list = ls())

## Load functions and packages
code_root <- "FILEPATH"
data_root <- "FILEPATH"

# packages

source("FILEPATH/get_draws.R")
source("FILEPATH/get_model_results.R")
source("FILEPATH/get_location_metadata.R")
source("FILEPATH/get_demographics.R")
source("FILEPATH/get_population.R")
source(paste0(code_root, 'FILEPATH'))
library(data.table)
library(stringr)
library(readstata13)
library(argparse)

# set-up run directory

run_file <- fread(paste0("FILEPATH"))
run_dir <- run_file[nrow(run_file), run_folder_path]
crosswalks_dir    <- paste0(run_dir, "FILEPATH")
draws_dir <- paste0(run_dir, "FILEPATH")
interms_dir <- paste0(run_dir, "FILEPATH")

params_dir <- "FILEPATH"

# parrallelize helpers 
source(paste0(code_root, "FILEPATH"))
my_shell <- paste0(code_root, "FILEPATH")

#############################################################################################
###'                                 [Receive sbatch]                                     ###
#############################################################################################

task_id <- as.integer(Sys.getenv("ADDRESS"))
params  <- fread(param_path)

loc_id                <- params[task_id, location_id]
release_id           <- params[task_id, release_id]
restrict_hook         <- params[task_id, restrict_hook ]
restrict_ascar        <- params[task_id, restrict_ascar]
restrict_trichur      <- params[task_id, restrict_trichur]

wasting_age_ids <- c(2, 3, 388, 389,34,238)
gbd_round_id <- ADDRESS

#############################################################################################
###'                     [Do Calculation Once if any endemicity]                          ###
#############################################################################################

if (0 %in% c(restrict_hook, restrict_ascar, restrict_trichur)){
  
  heavy_draws <-  get_draws(source = ADDRESS,
                            gbd_id_type = "model_id", 
                            gbd_id = ADDRESS,
                            measure_id = 5,
                            status = "best",
                            age_group_id = c(2, 3, 388,389,34,238),
                            location_id = loc_id,
                            sex_id = c(1,2),
                            release_id = release_id
  )
  
  # sum over the three
  heavy_draws[, paste0("sumsth_", 0:999) := lapply(0:999, function(x) sum(get(paste0("draw_", x)))), by = c("age_group_id", "location_id", "sex_id", "year_id")]
  
  # if >1 then ceiling at 1
  for (draw_num in 0:999){
    heavy_draws[get(paste0("sumsth_", draw_num)) > 1, eval(paste0("sumsth_", draw_num)) := 1]
  }
  
  # is now proportions of all
  heavy_draws[, paste0("draw_", 0:999) := lapply(0:999, function(x) get(paste0("draw_", x)) / get(paste0("sumsth_", x)))]
  
  # if age groups 2 and 3 are na --set to 0
  heavy_draws[age_group_id %in% c(2,3),paste0("draw_", 0:999) := 0]
  
  setnames(heavy_draws, paste0("draw_", 0:999), paste0("prop_draw_", 0:999))
  
  #' [ Prep Wasting meids]
  
  wasting_draws <-  get_draws(source = ADDRESS,
                              gbd_id_type = "model_id", 
                              gbd_id = ADDRESS,
                              measure_id = 5,
                              status = "best",
                              age_group_id = wasting_age_ids,
                              release_id = release_id,
                              location_id = loc_id,
                              sex_id = c(1,2),
  )
  
  
  setnames(wasting_draws, paste0("draw_", 0:999), paste0("wasting_draw_", 0:999))
  wasting_draws[, paste0("wasting_draw_", 0:999) := lapply(0:999, function(x) sum(get(paste0("wasting_draw_", x)))), by = c("age_group_id", "location_id", "sex_id", "year_id")]
  wasting_draws <- wasting_draws[model_id== ADDRESS]
  wasting_draws[, model_id:= NULL]
  
  # Z-score change in weight size per heavy prevalent case.
  # Based on the Hall et al paper. Same since GBD2013.
  
  effsize_mean  <- 0.493826493	
  effsize_lower <- 0.389863021
  effsize_upper <- 0.584794532
  effsize_sd    <- (effsize_upper - effsize_lower) / (2 * qnorm(0.975))
  
  wasting_draws[, paste0("zchange_draw_", 0:999) := lapply(0:999, function(x) effsize_mean + (rnorm(n = 1) * effsize_sd))]
  
  #'[ Use Heavy and Wasting Draws]
  
  all_draws <- merge(heavy_draws, wasting_draws , by = c("location_id", "age_group_id", "measure_id", "metric_id", "sex_id", "year_id"), all.x = TRUE)
  all_draws[, paste0("sumsth_", 0:999) := lapply(0:999, function(x) get(paste0("wasting_draw_", x)) - 
                                                   pnorm( qnorm(get(paste0("wasting_draw_", x))) - get(paste0("zchange_draw_", x)) * get(paste0("sumsth_", x))))]
  
  all_draws[is.na(paste0("sumsth_", draw_num)) | get(paste0("wasting_draw_", draw_num)) == 0, eval(paste0("sumsth_", draw_num)) := 0]
  
  all_draws[, paste0("draw_", 0:999) := lapply(0:999, function(x) get(paste0("sumsth_", x)) * get(paste0("prop_draw_", x)))]
  
  all_draws[, paste0("sumsth_", 0:999) := NULL]
  all_draws[, paste0("prop_draw_", 0:999) := NULL]
  all_draws[, paste0("wasting_draw_", 0:999) := NULL]
  all_draws[, paste0("zchange_draw_", 0:999) := NULL]
  
  for (draw_num in 0:999){
    all_draws[is.na(get(paste0("draw_",draw_num))), eval(paste0("draw_", draw_num)) := 0]
  }
  
  all_draws[, c("version_id.x", "version_id.y") := NULL]
}

#############################################################################################
###'                                   [Write Draws]                                      ###
#############################################################################################

all_list <- list(list(restrict_ascar, ADDRESS, ADDRESS), list(restrict_trichur, ADDRESS, ADDRESS), list(restrict_hook, ADDRESS, ADDRESS))

for (element in all_list){
  
  restrict <- element[[1]]
  heavy    <- element[[2]]
  wasting  <- element[[3]]

#############################################################################################
###'                             [Restrict GR Locations]                                  ###
#############################################################################################

if (restrict == 1){
  
  draws <- gen_zero_draws(model_id= NA, location_id = NA, measure_id = 5, release_id = release_id, team = ADDRESS)
  draws[, location_id := loc_id]
  draws[, model_id:= wasting]
  draws[,metric_id := 3]
  write.csv(draws, paste0(draws_dir, wasting, "FILEPATH"), row.names = FALSE)
  cat(paste0("\n finished draws for ", wasting))
  
}

#############################################################################################
###'                          [Estimate Endemic Locations]                                ###
#############################################################################################

if (restrict == 0){
  
  draws <- gen_zero_draws(model_id= NA, location_id = NA, measure_id = 5, release_id = release_id, team = 'FILEPATH')
  draws[, location_id := loc_id]  
  draws[, model_id:= wasting]
  draws <- draws[!(age_group_id %in% c(2,3,388,389,238,34))]
  
  # check row binding
  draws <- rbind(draws, all_draws[model_id== heavy])
  
  # zero negative
  for (draw_num in 0:999){
    draws[get(paste0("draw_", draw_num)) < 0, eval(paste0("draw_", draw_num)) := 0]
    cat(as.character(draw_num))
  }
  
  draws[, model_id:= wasting]
  draws[, metric_id := 3]
  write.csv(draws, paste0(draws_dir, wasting, "FILEPATH"), row.names = FALSE)
  cat(paste0("\n finished draws for ", wasting))
}}
 