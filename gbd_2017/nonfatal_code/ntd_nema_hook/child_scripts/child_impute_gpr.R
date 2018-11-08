## Empty the environment
rm(list = ls())

## Set up focal drives
os <- .Platform$OS.type
if (os=="windows") {
  j <- "FILEPATH"
  h <- "FILEPATH"
} else {
  j <- "FILEPATH"
  h <- "FILEPATH"
}

## Load functions and packages
gbd_functions <- "FILEPATH"
source(paste0(gbd_functions, "get_location_metadata.R"))
source(paste0(gbd_functions, "get_model_results.R"))
source(paste0(gbd_functions, "get_draws.R"))
source(paste0(gbd_functions, "get_population.R"))
source(paste0(gbd_functions, "get_demographics.R"))

library(argparse)
library(assertable)
library(parallel)
library(data.table)
library(readstata13)

## Parse Arguments
print(commandArgs())
parser <- ArgumentParser()
parser$add_argument("--my_loc", help = "location id", default = 168, type = "integer")
parser$add_argument("--year_ids", help = "Year IDs",
                    default = c(1990, 1995, 2000, 2005, 2010, 2017), nargs = "+", type = "integer")
parser$add_argument("--output_directory", help = "Site where draws will be stored",
                    default = "FILEPATH", type = "character")
parser$add_argument("--draw_directory", help = "Site where ST-GPR draws are currently stored",
                    default = "FILEPATH", type = "character")
parser$add_argument("--meids_directory", help = "Directory where meids are stored",
                    default = "FILEPATH", type = "character")
parser$add_argument("--my_worm", help = "Worm we are modelling", default = "ascariasis", type = "character")

args <- parser$parse_args()
print(args)
list2env(args, environment()); rm(args)
sessionInfo()

#############################################################################################
###                               Compute draws function                                  ###
#############################################################################################

get_all_age_data <- function(df, pattern, restrictions, my_ages) {

  ## Get remaining age groups and apply ratio
  for(age in sort(my_ages)){

    ratio <- pattern[age_group_id == age]$ratio
    loc   <- unique(df$location_id)
    temp  <- copy(df)
    temp[, age_group_id := (age)]

    if (!(loc %in% restrictions)){

      for (draw in grep("draw", names(temp), value = T))
        set(temp, j = draw, value = temp[[draw]]*ratio)

    } else if (loc %in% restrictions) {

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
ages <- get_demographics("ADDRESS", 5)$age_group_id
if (my_worm == "ascariasis") age_den <- 6
if (my_worm != "ascariasis") age_den <- 8

## Get correct MEIDS
meids     <- fread(meids_directory)
meids     <- meids[worm == my_worm]
dismod    <- meids[model == "dismod"]$meid
all_cases <- meids[model == "all"]$meid

## Get pattern for remaining age groups to apply to STG-PR draws
## trich was 325661; hookworm was 322529; asc is now 325682
male_pattern <- get_model_results("ADDRESS",
                                  gbd_id       = dismod,
                                  age_group_id = ages,
                                  location_id  = 1,
                                  year_id      = 2017,
                                  sex_id       = 1,
                                  status       = "best")

female_pattern <- get_model_results("ADDRESS",
                                    gbd_id       = dismod,
                                    age_group_id = ages,
                                    location_id  = 1,
                                    year_id      = 2017,
                                    sex_id       = 2,
                                    status       = "best")

male_denominator   <- male_pattern[age_group_id == age_den]$mean
female_denominator <- female_pattern[age_group_id == age_den]$mean

male_pattern[, c("measure_id", "expected", "upper", "lower") := NULL]
female_pattern[, c("measure_id", "expected", "upper", "lower") := NULL]

male_pattern[, ratio := mean / male_denominator]
female_pattern[, ratio := mean / female_denominator]
print(age_den); print(dismod) ; print(all_cases) ; print(male_pattern$model_version_id)
## Get locations where there are restrictions
restrict_loc <- get_model_results("ADDRESS", gbd_id = all_cases, measure_id = 5, age_group_id = 27, year_id = 2017,
                                  sex_id = 1, location_set_version_id = 319, gbd_round_id = 5)

restrict_loc <- restrict_loc[mean == 0]
restrict_loc <- unique(restrict_loc$location_id)

#############################################################################################
###                          Compute draws for each age group                             ###
#############################################################################################

## Get ST-GPR Draws
cat(paste0("Writing file: ", output_directory, my_loc, ".csv", " -- ", Sys.time(), "\n"))
data <- fread(paste0(draw_directory, my_loc, ".csv"))
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
write.csv(data, file = paste0(output_directory, my_loc, ".csv"), row.names = F)
cat(paste0("Finished writing draws for location id ", my_loc, "\n", "\tNumber of rows loaded: ", nrow(data), "\n"))
