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

library(argparse)
library(assertable)
library(parallel)
library(data.table)
library(readstata13)

## Parse Arguments
print(commandArgs())
parser <- ArgumentParser()
parser$add_argument("--my_loc", help = "location id", default = 168, type = "integer")
parser$add_argument("--my_worm", help = "Worm we are modelling", default = "ascariasis", type = "character")
parser$add_argument("--meids_directory", help = "Directory where meids are stored",
                    default = "FILEPATH", type = "character")
parser$add_argument("--output_directory", help = "Site where draws will be stored",
                    default = "FILEPATH", type = "character")

args <- parser$parse_args()
print(args)
list2env(args, environment()); rm(args)
sessionInfo()


#############################################################################################
###                           Processes for restricted locations                          ###
#############################################################################################

## Get correct MEIDS
meids     <- fread(meids_directory)
meids     <- meids[worm == my_worm]
all_cases <- meids[model == "all"]$meid
heavy     <- meids[model == "heavy"]$meid
mild      <- meids[model == "mild"]$meid

## Set up geographic restrictions first:
restrict_loc <- get_model_results("ADDRESS", gbd_id = all_cases, measure_id = 5, age_group_id = 22, year_id = 2017,
                                  sex_id = 1, location_set_version_id = 319, status = "best")

restrict_loc <- restrict_loc[mean == 0]
restrict_loc <- unique(restrict_loc$location_id)

## Condition with geographic restriction
if (my_loc %in% restrict_loc){
  cat("location has geogrpahic restrictions. Zeroing everything out. \n")

  my_draws <- get_draws(gbd_id_type = "modelable_entity_id", gbd_id = all_cases, source = "ADDRESS",
                        location_id = my_loc, status = "best")

  my_draws[, c("modelable_entity_id", "metric_id", "model_version_id") := NULL]

  write.csv(my_draws, paste0(output_directory, my_loc, ".csv"), row.names = F)

}

#############################################################################################
###                           Processes for unrestricted locations                        ###
#############################################################################################

## Conditon
if (!(my_loc %in% restrict_loc)){

  ## Pull in draws
  cat("Loading in parent, heavy, and moderate draws.\n")
  parent_draws <- get_draws(gbd_id_type = "modelable_entity_id", gbd_id = all_cases, source = "ADDRESS",
                            location_id = my_loc, status = "best")

  heavy_draws  <- get_draws(gbd_id_type = "modelable_entity_id", gbd_id = heavy, source = "ADDRESS",
                            location_id = my_loc, status = "best")

  med_draws    <- get_draws(gbd_id_type = "modelable_entity_id", gbd_id = mild, source = "ADDRESS",
                            location_id = my_loc, status = "best")

  ## Remove unnecessary columns
  parent_draws[, c("metric_id", "modelable_entity_id", "model_version_id") := NULL]
  heavy_draws[, c("metric_id", "modelable_entity_id", "model_version_id") := NULL]
  med_draws[, c("metric_id", "modelable_entity_id", "model_version_id") := NULL]

  ## Change names of draws to prep for merges
  for (my_draw in grep("draw", names(parent_draws), value = T))
    setnames(parent_draws, old = my_draw, new = paste0("parent_", strsplit(my_draw, "_")[[1]][2]))

  for (my_draw in grep("draw", names(heavy_draws), value = T))
    setnames(heavy_draws, old = my_draw, new = paste0("heavy_", strsplit(my_draw, "_")[[1]][2]))

  for (my_draw in grep("draw", names(med_draws), value = T))
    setnames(med_draws, old = my_draw, new = paste0("med_", strsplit(my_draw, "_")[[1]][2]))

  ## Merge everything together
  cat("Now mergeing together data and running subtractions to compute asymptompatic draws.\n")
  df <- merge(parent_draws, heavy_draws, by = c("location_id", "measure_id", "sex_id", "year_id", "age_group_id"))
  df <- merge(df, med_draws, by = c("location_id", "measure_id", "sex_id", "year_id", "age_group_id"))
  rm(list = c("parent_draws", "heavy_draws", "med_draws"))

  ## Subtract to get asymptomatic
  for (i in 0:999){

    parent_name <- paste0("parent_", i)
    heavy_name  <- paste0("heavy_", i)
    med_name    <- paste0("med_", i)
    draw_name   <- paste0("draw_", i)

    df[, (draw_name) := get(parent_name) - get(heavy_name) - get(med_name)]
    df[, (parent_name) := NULL][, (heavy_name) := NULL][, (med_name) := NULL]
  }

  ## Check for negative values
  anynegs <- which(apply(df, 1, function(row) any(row < 0)))
  if (length(anynegs) != 0){
    cat("ERROR: Negative value detected. Now breaking \n")
    cat(anynegs)
    write.csv(df, paste0(output_directory, "ERROR_", my_loc,".csv"), row.names = F)
    break
  }

  ## Done
  cat("Draws complete!\n")
  write.csv(df, paste0(output_directory, my_loc, ".csv"), row.names = F)

}
