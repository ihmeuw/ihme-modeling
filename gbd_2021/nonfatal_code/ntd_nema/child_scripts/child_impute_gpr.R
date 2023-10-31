# Purpose: use DisMod Age Curve to break out STGPR Child Prevalence Estimates
#
### ======================= BOILERPLATE ======================= ###
rm(list = ls())

## Load functions and packages
code_root <-"FILEPATH"
data_root <- "FILEPATH"

params_dir <- "FILEPATH"
draws_dir <- "FILEPATH"
interms_dir <- "FILEPATH"
logs_dir <- "FILEPATH"
location_id <- 214

source("FILEPATH/get_demographics.R")
source("FILEPATH/get_model_results.R")
library(stringr)
library(data.table)
library(argparse, lib.loc= "FILEPATH")

# set run dir
run_file <- fread(paste0(data_root, "FILEPATH"))
run_dir <- run_file[nrow(run_file), run_folder_path]
crosswalks_dir    <- paste0(run_dir, "FILEPATH")
code_dir <- paste0(code_root, 'FILEPATH')

## Parse Arguments
print(commandArgs())
parser <- ArgumentParser()
parser$add_argument("--param_path_impute", help = "param path", default = "168", type = "character")

args <- parser$parse_args()
print(args)
list2env(args, environment()); rm(args)
sessionInfo()


#############################################################################################
###                               Compute draws function                                  ###
#############################################################################################

task_id <- as.integer(Sys.getenv("ADDRESS"))
params  <- fread(param_path_impute)

my_loc                <- params[task_id, location_ids]
stgpr_draws_directory <- params[task_id, stgpr_draws_directory]
my_worm               <- params[task_id, my_worm]
gbd_round_id          <- params[task_id, gbd_round_id]
decomp_step           <- params[task_id, decomp_step]

study_dems <- readRDS(paste0(data_root, "FILEPATH", gbd_round_id, ".rds"))
year_ids   <- study_dems$year_id
meids_directory     <- paste0(params_dir, "FILEPATH")

## Get locations to parrelize by

my_locs    <- study_dems$location_id
all_locs   <- study_dems$location_id
meids <- fread(meids_directory)
meids      <- meids[worm == my_worm]
all_cases  <- meids[model == "all"]$meid
draws_directory <- paste0(draws_dir, my_worm,  "/", all_cases, "/")

get_all_age_data <- function(df, pattern, restrictions, my_ages) {
  
  ## Get remaining age groups and apply ratio
  for(age in sort(my_ages)){
    
    ratio <- pattern[age_group_id == age]$ratio
    loc   <- unique(df$location_id)
    temp  <- copy(df)
    temp[, age_group_id := (age)]
    
    if (!(my_loc %in% restrictions)){
      for (draw in grep("draw", names(temp), value = T))
        set(temp, j = draw, value = temp[[draw]]*ratio)
      
    } else if (my_loc %in% restrictions) {
      
      for (draw in grep("draw", names(temp), value = T))
        set(temp, j = draw, value = temp[[draw]]*0)
    }
    
    if(age == 2) dt <- copy(temp)
    if(age != 2) dt <- rbind(dt, temp)
  }
  
  return(dt)
}

#############################################################################################
###                                     Get Pattern                                       ###
#############################################################################################

## Get locations where there are restrictions
if (my_worm == "ascariasis") {short <- "ascar"}
if (my_worm == "trichuriasis") {short <- "trichur"}
if (my_worm == "hookworm") {short <- "hook"}

restrict_loc <- fread(paste0(params_dir, "FILEPATH"))
restrict_loc <- unique(restrict_loc[value_endemicity == 0, location_id])

if (my_loc %in% restrict_loc){
  
  data <- gen_zero_draws(model_id= all_cases, location_id = NA, measure_id = 5, gbd_round_id = gbd_round_id, team = 'FILEPATH')
  data[, metric_id := 3]
  data[, model_id:= all_cases]
  write.csv(data, file = paste0(draws_directory, my_loc, "FILEPATH"), row.names = F)
  
  cat(paste0("Finished writing draws for location id ", my_loc, "\n", "\tNumber of rows loaded: ", nrow(data), "\n"))
  
}

#############################################################################################
###                          Compute draws for each age group                             ###
#############################################################################################

if (!(my_loc %in% restrict_loc)){
  
  ## Grab age data
  ages <- get_demographics("ADDRESS", gbd_round_id = gbd_round_id)$age_group_id
  if (my_worm == "ascariasis") age_den <- 6
  if (my_worm != "ascariasis") age_den <- 8
  
  ## Get correct MEIDS
  meids     <- fread(as.character(meids_directory))
  meids     <- meids[worm == my_worm]
  dismod    <- meids[model == "dismod"]$meid
  all_cases <- meids[model == "all"]$meid
  
  male_pattern <- get_model_results("ADDRESS",
                                    gbd_id       = ADDRESS,
                                    age_group_id = ages,
                                    location_id  = 1,
                                    year_id      = 2010,
                                    sex_id       = 1,
                                    status       = "best",
                                    decomp_step  = "ADDRESS", 
                                    gbd_round_id = gbd_round_id)
  
  female_pattern <- get_model_results("ADDRESS",
                                      gbd_id       = ADDRESS,
                                      age_group_id = ages,
                                      location_id  = 1,
                                      year_id      = 2010,
                                      sex_id       = 2,
                                      status       = "best",
                                      decomp_step  = "ADDRESS", 
                                      gbd_round_id = gbd_round_id)
  
  male_denominator   <- male_pattern[age_group_id == age_den]$mean
  female_denominator <- female_pattern[age_group_id == age_den]$mean
  
  male_pattern[, c("measure_id", "expected", "upper", "lower") := NULL]
  female_pattern[, c("measure_id", "expected", "upper", "lower") := NULL]
  
  male_pattern[, ratio := mean / male_denominator]
  female_pattern[, ratio := mean / female_denominator]
  print(age_den); print(dismod) ; print(all_cases) ; print(male_pattern$version_id)
  
  ## Get ST-GPR Draws
  cat(paste0("Writing file: ", draws_directory, my_loc, "FILEPATH", " -- ", Sys.time(), "\n"))
  
  print(paste0("loc ", my_loc))
  
  data <- tryCatch({
    fread(paste0(stgpr_draws_directory, my_loc, "FILEPATH"))
  } ,error = function(e){
    stop("location_id not in ST-GPR draws")
  })
  
  data[, location_id := my_loc]
  data <- data[year_id %in% year_ids]
  data <- data[, measure_id := 5]
  
  male   <- copy(data)
  female <- copy(data)
  
  ## Get remaining age groups and apply ratio utilizing the user function
  male   <- get_all_age_data(male, male_pattern, restrict_loc, ages)
  female <- get_all_age_data(female, female_pattern, restrict_loc, ages)
  
  male[, sex_id := 1]
  female[, sex_id := 2]
  
  ## Append and save
  data <- rbind(male, female)
  data[, metric_id := 3]
  data[, model_id:= all_cases]
  
  ## The following caps prevalence at draws >1 
  data[, (paste0("draw_", 0:999)) := lapply(.SD, function(x) ifelse(x > 1.0, 0.999, x)), .SDcols=paste0("draw_", 0:999)]
  
  write.csv(data, file = paste0(draws_directory, "FILEPATH"), row.names = F)
  
  cat(paste0("Finished writing draws for location id ", my_loc, "\n", "\tNumber of rows loaded: ", nrow(data), "\n"))
  
}