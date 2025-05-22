# Purpose: Create cfr model (VL - fatal)
# Notes: Ask why dropping certain areas and look into lme4 model

### ======================= BOILERPLATE ======================= ###
rm(list = ls())
code_root <-paste0("FILEPATH", Sys.info()[7])
data_root <- "FILEPATH"

# Toggle (Prod Arg Parsing VS. Interactive Dev) Common /ihme/ IO Paths
if (!is.na(Sys.getenv()["EXEC_FROM_ARGS"][[1]])) {
  library(argparse)
  print(commandArgs())
  parser <- ArgumentParser()
  parser$add_argument("--params_dir", type = "character")
  parser$add_argument("--draws_dir", type = "character")
  parser$add_argument("--interms_dir", type = "character")
  parser$add_argument("--logs_dir", type = "character")
  args <- parser$parse_args()
  print(args)
  list2env(args, environment()); rm(args)
  sessionInfo()
} else {
  params_dir <- paste0(data_root, "FILEPATH")
  draws_dir <- paste0(data_root, "FILEPATH")
  interms_dir <- paste0(data_root, "FILEPATH")
  logs_dir <- paste0(data_root, "FILEPATH")
}

library(data.table)
source("FILEPATH/get_demographics.R")
source("FILEPATH/get_population.R")
source("FILEPATH/get_cod_data.R")
source("FILEPATH/get_model_results.R")
source("FILEPATH/get_location_metadata.R")
source("FILEPATH/get_draws.R")

### paths to run interactively

run_file     <- fread(paste0(data_root, "FILEPATH"))
run_dir      <- run_file[nrow(run_file), run_folder_path]
draws_dir    <- paste0(run_dir, "FILEPATH")
interms_dir  <- paste0(run_dir, "FILEPATH")
logs_dir     <- paste0(run_dir, "FILEPATH")
params_dir   <- paste0("FILEPATH")
source(paste0(code_root, "FILEPATH"))

### ======================= Main Execution ======================= ###

zero_draw <- gen_zero_draws(model_id = 1, location_id = 1, measure_id = c(1), release_id = release_id, team = ADDRESS)

#'[ Restricted Locations]
  
if(value_endemicity == 0){
  cat("\n Loc is Restricted")
  zero_draw[, measure_id := 1]
  zero_draw[, location_id := loc_id]
  zero_draw[, cause_id := 348]
  zero_draw[, metric_id := 1]
  #'[CHANGED]
  write.csv(zero_draw, paste0(draws_dir, "FILEPATH", loc_id, ".csv"), row.names = FALSE)
}

#'[ Endemic Locations]

if(value_endemicity == 1){
  cat("\n Loc is not Restricted")
  # get incidence and population
  
  study_dems <- readRDS(paste0(data_root, 'FILEPATH', gbd_round_id, '.rds'))
  full_year_id <- study_dems$year_id
  
  #'[CHANGED]
  incidence <- fread(paste0(draws_dir, "/A/", loc_id, ".csv"))
  incidence <- incidence[measure_id == 6]
  
  incidence <- melt(incidence, measure.vars = paste0("draw_", 0:999), variable.name = "draw", value.name = "value")
  incidence <- incidence[, .("mean" = mean(value)), by = c("location_id", "year_id", "age_group_id", "sex_id")]
  incidence <- incidence[, .(location_id, year_id, age_group_id, sex_id, mean)]
  
  population<- get_population(location_id = loc_id, age_group_id = "all", sex_id = c(1,2), year_id = full_year_id, gbd_round_id = gbd_round_id, decomp_step = decomp_step)
  incidence <- merge(incidence, population, by = c("age_group_id", "location_id", "sex_id", "year_id"))
  incidence[, vl_cases := population * mean]
  incidence <- incidence[, .(age_group_id, location_id, sex_id, year_id, vl_cases)]
  
  # cfr
  cfr_draws <- as.data.table(fread(paste0(draws_dir, "FILEPATH", loc_id, ".csv")))
  
  # correct age group ids 4 -> 388 389 , 5 -> 238 34
  cfr_4 <- copy(cfr_draws[age_group_id == 3])
  cfr_388 <- copy(cfr_4[, age_group_id := 388])
  cfr_389 <- copy(cfr_4[, age_group_id := 389])
  
  cfr_5 <- copy(cfr_draws[age_group_id == 4])
  cfr_238 <- copy(cfr_5[, age_group_id := 238])
  cfr_34 <- copy(cfr_5[, age_group_id := 34])
  
  cfr_draws <- rbind(cfr_draws, cfr_388, cfr_389, cfr_238, cfr_34)
  cfr_draws <- cfr_draws[!(age_group_id %in% c(4,5))]
  
  setnames(cfr_draws, as.character(1:1000), paste0("draw_", 0:999))
  cfr_draws[, year_id := NULL]
  cfr_draws <- merge(incidence, cfr_draws, by = c("age_group_id", "location_id", "sex_id"), all.x = TRUE)
  
  if (loc_id == 435){ cfr_draws[year_id %in% 1990:1994, paste0("draw_", 0:999) := 0.69] }
  
  # calculate
  cfr_draws[, paste0("draw_", 0:999) := lapply(0:999, function(x) get(paste0("draw_", x)) * vl_cases)]
  
  #zero age group ids 2,3
  cfr_draws[age_group_id %in% c(2,3), paste0("draw_", 0:999) := 0]
  
  # write out
  cfr_draws[, metric_id := 1]
  write.csv(cfr_draws, paste0(draws_dir, "FILEPATH", loc_id, ".csv"), row.names = FALSE)
}