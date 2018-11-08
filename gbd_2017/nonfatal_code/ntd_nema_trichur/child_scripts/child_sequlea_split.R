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
parser$add_argument("--is_heavy", help = "numeric boolean for if we want to compute heavy infestation or not", default = 1, type = "integer")
parser$add_argument("--my_worm", help = "Worm we are modelling", default = "ascariasis", type = "character")
parser$add_argument("--output_directory", help = "Site where draws will be stored",
                    default = "FILEPATH", type = "character")
parser$add_argument("--prop_directory", help = "Site where proportions are stored",
                    default = "FILEPATH", type = "character")
parser$add_argument("--meids_directory", help = "Directory where meids are stored",
                    default = "FILEPATH", type = "character")

args <- parser$parse_args()
print(args)
list2env(args, environment()); rm(args)
sessionInfo()

#############################################################################################
###                           Processes for restricted locations                          ###
#############################################################################################

cat(paste0("Output directory: ", output_directory, "\n"))
meids     <- fread(meids_directory)
meids     <- meids[worm == my_worm]
all_cases <- meids[model == "all"]$meid

## Set up geographic restrictions first:
restrict_loc <- get_model_results("ADDRES", gbd_id = all_cases, measure_id = 5, age_group_id = 27, year_id = 2017,
                                  sex_id = 1, location_set_version_id = 319, status = "best")

restrict_loc <- restrict_loc[mean == 0]
restrict_loc <- unique(restrict_loc$location_id)

## Condition with geographic restriction
if (my_loc %in% restrict_loc){
  cat("location has geogrpahic restrictions. Zeroing everything out. \n")

  asc_draws <- get_draws(gbd_id_type = "modelable_entity_id", gbd_id = all_cases, source = "ADDRES",
                         location_id = my_loc, status = "best")

  asc_draws[, c("modelable_entity_id", "metric_id", "model_version_id") := NULL]

  write.csv(asc_draws, file = paste0(output_directory, my_loc, ".csv"), row.names = F)

}

#############################################################################################
###                           Processes for unrestricted locations                        ###
#############################################################################################

## Conditon
if (!(my_loc %in% restrict_loc)){

  prop <- fread(prop_directory)
  prop <- prop[location_id == my_loc]

  ## Get draws
  asc_draws <- get_draws(gbd_id_type = "modelable_entity_id", gbd_id = all_cases, source = "ADDRES",
                         location_id = my_loc, status = "best")

  pops <- get_population(age_group_id = unique(asc_draws$age_group_id),
                         location_id  = unique(asc_draws$location_id),
                         year_id      = unique(asc_draws$year_id), sex_id = 1:2)
  pops[, run_id := NULL]
  asc_draws <- merge(asc_draws, pops, by = setdiff(names(pops), "population"))

  ## Convert to case space
  for (draw in grep("draw", names(asc_draws), value = T))
       set(asc_draws, j = draw, value = asc_draws[[draw]]*asc_draws[, population])

  ## Loop through years
  for (year in unique(asc_draws$year_id)){
    cat(paste0("Applying proportion for year id ", year, "\n"))
    temp <- copy(asc_draws)
    temp <- temp[year_id == year]

    ## Here we save the proportions of infestation. If the location is missing
    ## we apply the regional average.
    if (my_loc %in% unique(prop$location_id)){

      if (is_heavy == T){
        cat("\tUsing heavy location proportions \n")
        prop_age0_4   <- prop[year_id == year & age == "0to4"]$prop_heavy
        prop_age5_9   <- prop[year_id == year & age == "5to9"]$prop_heavy
        prop_age10_14 <- prop[year_id == year & age == "10to14"]$prop_heavy
        prop_age15    <- prop[year_id == year & age == "15plus"]$prop_heavy
      } else if (is_heavy == F){
        cat("\tUsing medium location proportions \n")
        prop_age0_4   <- prop[year_id == year & age == "0to4"]$prop_med
        prop_age5_9   <- prop[year_id == year & age == "5to9"]$prop_med
        prop_age10_14 <- prop[year_id == year & age == "10to14"]$prop_med
        prop_age15    <- prop[year_id == year & age == "15plus"]$prop_med
      }

    } else if (!(my_loc %in% unique(prop$location_id))){
      cat("\tapplying regional average proportion \n")
      locs <- get_location_metadata(location_set_id = 35, gbd_round_id = 5)
      locs <- locs[location_id == my_loc]

      prop <- fread(prop_directory)
      prop <- prop[region_name == locs$region_name]

      if (nrow(prop) == 0) {
        cat(paste0("Missing region for location ", my_loc, " -- now breaking"))
        write.csv(my_loc, paste0(output_directory, my_loc, "ERROR.csv"), row.names = F)
        break
      }

      if (is_heavy == T){
        cat("\tUsing heavy regional proportions \n")
        prop_age0_4   <- unique(prop[year_id == year & age == "0to4"]$region_heavy_ave)
        prop_age5_9   <- unique(prop[year_id == year & age == "5to9"]$region_heavy_ave)
        prop_age10_14 <- unique(prop[year_id == year & age == "10to14"]$region_heavy_ave)
        prop_age15    <- unique(prop[year_id == year & age == "15plus"]$region_heavy_ave)
      } else if (is_heavy == F)
        cat("\tUsing medium regional proportions \n")
        prop_age0_4   <- unique(prop[year_id == year & age == "0to4"]$region_med_ave)
        prop_age5_9   <- unique(prop[year_id == year & age == "5to9"]$region_med_ave)
        prop_age10_14 <- unique(prop[year_id == year & age == "10to14"]$region_med_ave)
        prop_age15    <- unique(prop[year_id == year & age == "15plus"]$region_med_ave)

    }

    ## Merge proportion onto dataframe
    temp[age_group_id <= 5, heavy_prop := prop_age0_4]
    temp[age_group_id == 6, heavy_prop := prop_age5_9]
    temp[age_group_id == 7, heavy_prop := prop_age10_14]
    temp[age_group_id >= 8, heavy_prop := prop_age15]

    ## Apply proportion by age group
    for (draw in grep("draw", names(temp), value = T))
      set(temp, j = draw, value = temp[[draw]]*temp[, heavy_prop])

    if (year == 1990) data <- copy(temp)
    if (year != 1990) data <- rbind(data, temp)

  }

  ## Convert back to prevalence space for upload
  for (draw in grep("draw", names(data), value = T))
    set(data, j = draw, value = data[[draw]] / data[, population])

  data[, c("modelable_entity_id", "metric_id", "model_version_id", "population", "heavy_prop") := NULL]

  cat(paste("Finished location", my_loc, "\n"))
  write.csv(data, file = paste0(output_directory, my_loc, ".csv"), row.names = F)

}
