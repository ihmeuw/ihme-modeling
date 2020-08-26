#' [Title: STH Submit Wasting

#############################################################################################
###'                                [General Set-Up]                                      ###
#############################################################################################

rm(list = ls())

# packages
source(FILEPATH)
source(FILEPATH)
source(FILEPATH)
source(FILEPATH)
source(FILEPATH)
library(data.table)
library(stringr)
library(readstata13)
library(argparse)

# set-up run directory
run_file <- fread(paste0(FILEPATH))
run_dir <- run_file[nrow(run_file), run_folder_path]
crosswalks_dir    <- paste0(run_dir, FILEPATH)
draws_dir <- paste0(run_dir, FILEPATH)
interms_dir <- paste0(run_dir, FILEPATH)
params_dir <- FILEPATH

# parrallelize helpers 
source(FILEPATH)
my_shell <- FILEPATH

#############################################################################################
###'                                 [Receive Qsub]                                       ###
#############################################################################################


# Parse Arguments
print(commandArgs())
parser <- ArgumentParser()
parser$add_argument("--param_path", help = "param path", default = ADDRESS, type = "character") 

args <- parser$parse_args()
print(args)
list2env(args, environment()); rm(args)
sessionInfo()

task_id <- as.integer(Sys.getenv(ADDRESS))
params  <- fread(param_path)

loc_id                <- params[task_id, location_id]
decomp_step           <- params[task_id, decomp_step]
restrict_hook         <- params[task_id, restrict_hook ]
restrict_ascar        <- params[task_id, restrict_ascar]
restrict_trichur      <- params[task_id, restrict_trichur]

heavy_meids     <- c(ADDRESS, ADDRESS, ADDRESS)
wasting_meids   <- c(ADDRESS, ADDRESS)
wasting_age_ids <- c(2, 3, 4, 5)
gbd_round_id <- 6

#############################################################################################
###'                     [Do Calculation Once if any endemicity]                          ###
#############################################################################################

if (0 %in% c(restrict_hook, restrict_ascar, restrict_trichur)){
  
  heavy_draws <-  get_draws(source = "epi",
                            gbd_id_type = "modelable_entity_id", 
                            gbd_id = heavy_meids,
                            measure_id = 5,
                            status = "best",
                            age_group_id = c(2, 3, 4, 5),
                            decomp_step = decomp_step,
                            location_id = loc_id,
                            sex_id = c(1,2),
                            gbd_round_id = gbd_round_id
  )
  
  # sum over the three
  heavy_draws[, paste0("sumsth_", 0:999) := lapply(0:999, function(x) sum(get(paste0("draw_", x)))), by = c("age_group_id", "location_id", "sex_id", "year_id")]
  
  # if >1 then ceiling at 1
  for (draw_num in 0:999){
    heavy_draws[get(paste0("sumsth_", draw_num)) > 1, eval(paste0("sumsth_", draw_num)) := 1]
  }
  
  # is now proportions of all
  heavy_draws[, paste0("draw_", 0:999) := lapply(0:999, function(x) get(paste0("draw_", x)) / get(paste0("sumsth_", x)))]
  
  # if is.na then set to 0
  for (draw_num in 0:999){
    heavy_draws[is.na(get(paste0("sumsth_", draw_num))), eval(paste0("sumsth_", draw_num)) := 0]
  }
  
  setnames(heavy_draws, paste0("draw_", 0:999), paste0("prop_draw_", 0:999))

  
  #' [ Prep Wasting meids]
  wasting_draws <-  get_draws(source = "epi",
                              gbd_id_type = "modelable_entity_id", 
                              gbd_id = c(ADDRESS, ADDRESS),
                              measure_id = 5,
                              status = "best",
                              age_group_id = wasting_age_ids,
                              decomp_step = decomp_step,
                              location_id = loc_id,
                              sex_id = c(1,2),
                              gbd_round_id = gbd_round_id
  )
  
  
  setnames(wasting_draws, paste0("draw_", 0:999), paste0("wasting_draw_", 0:999))
  wasting_draws[, paste0("wasting_draw_", 0:999) := lapply(0:999, function(x) sum(get(paste0("wasting_draw_", x)))), by = c("age_group_id", "location_id", "sex_id", "year_id")]
  wasting_draws <- wasting_draws[modelable_entity_id == ADDRESS]
  wasting_draws[, modelable_entity_id := NULL]
  
  effsize_mean  <- 0.493826493	
  effsize_lower <- 0.389863021
  effsize_upper <- 0.584794532
  effsize_sd    <- (effsize_upper - effsize_lower) / (2 * qnorm(0.975))
  
  wasting_draws[, paste0("zchange_draw_", 0:999) := lapply(0:999, function(x) effsize_mean + (rnorm(n = 1) * effsize_sd))]
  
  #'[ Use Heavy and Wasting Draws]
  all_draws <- merge(heavy_draws, wasting_draws , by = c("location_id", "age_group_id", "measure_id", "metric_id", "sex_id", "year_id"), all.x = TRUE)
  all_draws[, paste0("sumsth_", 0:999) := lapply(0:999, function(x) get(paste0("wasting_draw_", x)) - 
                                                   pnorm( qnorm(get(paste0("wasting_draw_", x))) - get(paste0("zchange_draw_", x)) * get(paste0("sumsth_", x))))]
  
  for (draw_num in 0:999){
    all_draws[is.na(paste0("sumsth_", draw_num)) | get(paste0("wasting_draw_", draw_num)) == 0, eval(paste0("sumsth_", draw_num)) := 0]
  }
  
  all_draws[, paste0("draw_", 0:999) := lapply(0:999, function(x) get(paste0("sumsth_", x)) * get(paste0("prop_draw_", x)))]
  
  all_draws[, paste0("sumsth_", 0:999) := NULL]
  all_draws[, paste0("prop_draw_", 0:999) := NULL]
  all_draws[, paste0("wasting_draw_", 0:999) := NULL]
  all_draws[, paste0("zchange_draw_", 0:999) := NULL]
  
  for (draw_num in 0:999){
    all_draws[is.na(get(paste0("draw_",draw_num))), eval(paste0("draw_", draw_num)) := 0]
  }
  
  all_draws[, c("model_version_id.x", "model_version_id.y") := NULL]
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
  
  draws <- fread(paste0(interms_dir, FILEPATH))
  draws[, location_id := loc_id]
  draws[, modelable_entity_id := wasting]
  write.csv(draws, paste0(draws_dir, wasting, "/", loc_id, ".csv"), row.names = FALSE)
  cat(paste0("\n finished draws for ", wasting))
  
}

#############################################################################################
###'                          [Estimate Endemic Locations]                                ###
#############################################################################################

if (restrict == 0){
  
  # add ages not 2,3,4,5 as 0 
  draws <- fread(paste0(interms_dir, FILEPATH))
  draws[, location_id := loc_id]  
  draws[, modelable_entity_id := wasting]
  draws <- draws[!(age_group_id %in% c(2,3,4,5))]
  
  # check row binding
  draws <- rbind(draws, all_draws[modelable_entity_id == heavy])
  
  draws[, modelable_entity_id := wasting]
  write.csv(draws, paste0(draws_dir, wasting, "/", loc_id, ".csv"), row.names = FALSE)
  cat(paste0("\n finished draws for ", wasting))
}}
 