## Empty the environment
rm(list = ls())

## Set up focal drives
os <- .Platform$OS.type
if (os=="windows") {
  ADDRESS <- FILEPATH
  ADDRESS <- FILEPATH
} else {
  ADDRESS <- FILEPATH
  ADDRESS <- paste0(FILEPATH, Sys.info()[7], "/")
}

## Load functions and packages
source(FILEPATH)
source(FILEPATH)
source(FILEPATH)
source(FILEPATH)
source(FILEPATH)
source(FILEPATH)
library(stringr)
library(argparse, lib.loc= FILEPATH)

## Parse Arguments
print(commandArgs())
parser <- ArgumentParser()
parser$add_argument("--param_path_impute", help = "param path", default = ADDRESS, type = "character")

args <- parser$parse_args()
print(args)
list2env(args, environment()); rm(args)
sessionInfo()

gbd_round_id <- 6

#############################################################################################
###                               Compute draws function                                  ###
#############################################################################################

task_id <- as.integer(Sys.getenv(ADDRESS))
params  <- fread(param_path_impute)

my_loc                <- params[task_id, location_ids]
year_ids              <- params[task_id, year_ids]
year_ids              <- as.numeric(str_split(year_ids, " ", simplify = TRUE))
draws_directory       <- params[task_id, draws_directory]
stgpr_draws_directory <- params[task_id, stgpr_draws_directory]
meids_directory       <- params[task_id, meids_directory]
my_worm               <- params[task_id, my_worm]
params_dir            <- params[task_id, params_directory]

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

## Grab age data
ages <- get_demographics("epi", 6)$age_group_id
if (my_worm == "ascariasis") age_den <- 6
if (my_worm != "ascariasis") age_den <- 8

## Get correct MEIDS
meids     <- fread(meids_directory)
meids     <- meids[worm == my_worm]
dismod    <- meids[model == "dismod"]$meid
all_cases <- meids[model == "all"]$meid

## Get pattern for remaining age groups to apply to STG-PR draws
male_pattern <- get_model_results("epi",
                                  gbd_id       = dismod,
                                  age_group_id = ages,
                                  location_id  = 1,
                                  year_id      = 2019,
                                  sex_id       = 1,
                                  status       = "best",
                                  decomp_step  = "step4", 
                                  gbd_round_id = gbd_round_id)

female_pattern <- get_model_results("epi",
                                    gbd_id       = dismod,
                                    age_group_id = ages,
                                    location_id  = 1,
                                    year_id      = 2019,
                                    sex_id       = 2,
                                    status       = "best",
                                    decomp_step  = "step4", 
                                    gbd_round_id = gbd_round_id)

male_denominator   <- male_pattern[age_group_id == age_den]$mean
female_denominator <- female_pattern[age_group_id == age_den]$mean

male_pattern[, c("measure_id", "expected", "upper", "lower") := NULL]
female_pattern[, c("measure_id", "expected", "upper", "lower") := NULL]

male_pattern[, ratio := mean / male_denominator]
female_pattern[, ratio := mean / female_denominator]
print(age_den); print(dismod) ; print(all_cases) ; print(male_pattern$model_version_id)

## Get locations where there are restrictions
if (my_worm == "ascariasis") {short <- "ascar"}
if (my_worm == "trichuriasis") {short <- "trichur"}
if (my_worm == "hookworm") {short <- "hook"}

restrict_loc <- fread(paste0(params_dir, FILEPATH, short, FILEPATH))
restrict_loc <- unique(restrict_loc[value_endemicity == 0, location_id])

if (my_loc %in% restrict_loc){
  
  cat("location has geographic restrictions. Zeroing everything out. \n")
  
  # Canada has zero cases, and is the skeleton for all 0 draws 
  worm_draws <- get_draws(gbd_id_type = "modelable_entity_id", gbd_id = all_cases, source = "epi",
                          location_id = 101, decomp_step = "step1",  version_id = ADDRESS)
  # ^^ do not edit -- need static generic zero draw file
  
  worm_draws[, c("modelable_entity_id", "metric_id", "model_version_id") := NULL]
  worm_draws[, location_id := my_loc]
  
  write.csv(worm_draws, file = paste0(draws_directory, my_loc, ".csv"), row.names = F)
  cat(paste("Finished location", my_loc, "\n"))
}

#############################################################################################
###                          Compute draws for each age group                             ###
#############################################################################################

if (!(my_loc %in% restrict_loc)){
  
## Get ST-GPR Draws
cat(paste0("Writing file: ", draws_directory, my_loc, ".csv", " -- ", Sys.time(), "\n"))

print(paste0("loc ", my_loc))

data <- tryCatch({
  fread(paste0(stgpr_draws_directory, my_loc, ".csv"))
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
write.csv(data, file = paste0(draws_directory, my_loc, ".csv"), row.names = F)

cat(paste0("Finished writing draws for location id ", my_loc, "\n", "\tNumber of rows loaded: ", nrow(data), "\n"))

}