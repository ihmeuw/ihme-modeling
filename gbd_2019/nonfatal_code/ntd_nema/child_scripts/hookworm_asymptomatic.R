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

library(bindrcpp, lib.loc = paste0(FILEPATH))
library(glue, lib.loc = paste0(FILEPATH))
library(argparse, lib.loc= paste0(FILEPATH))
library(assertable)
library(parallel)
library(data.table)
library(readstata13)
library(stringr)


## Parse Arguments
print(commandArgs())
parser <- ArgumentParser()
parser$add_argument("--param_path", help = "param path", default = ADDRESS, type = "character")

args <- parser$parse_args()
print(args)
list2env(args, environment()); rm(args)
sessionInfo()

task_id <- as.integer(Sys.getenv(ADDRESS))
params  <- fread(param_path)

my_loc                <- params[task_id, location_id]

gbd_round_id <- 6

#############################################################################################
###                           Processes for restricted locations                          ###
#############################################################################################

restrict_loc <- fread(FILEPATH)
restrict_loc <- unique(restrict_loc[value_endemicity == 0, location_id])
# zero poland and italy
locs <- get_location_metadata(35, gbd_round_id = gbd_round_id)
restrict <- locs[parent_id %in% c(86, 51), location_id]
restrict_loc <- c(restrict_loc, restrict)


run_file <- fread(paste0(FILEPATH))
run_dir <- run_file[nrow(run_file), run_folder_path]

draws_dir    <- paste0(run_dir, FILEPATH)
interms_dir  <- paste0(run_dir, FILEPATH)
logs_dir     <- paste0(run_dir, FILEPATH)
params_dir <- paste0(FILEPATH)
code_dir <- paste0(FILEPATH)

draws_dir <- paste0(draws_dir, ADDRESS)


if (my_loc %in% restrict_loc){
  
  cat("location has geographic restrictions. Zeroing everything out. \n")
  
  # Canada has zero cases, and is the skeleton for all 0 draws 
  worm_draws <- get_draws(gbd_id_type = "modelable_entity_id", gbd_id = ADDRESS, source = "epi",
                          location_id = 101, decomp_step = "step4",  status = 'best', gbd_round_id = gbd_round_id)
  # ^^ do not edit -- need static generic zero draw file
  
  worm_draws[, c("modelable_entity_id", "metric_id", "model_version_id") := NULL]
  worm_draws[, location_id := my_loc]
  
  write.csv(worm_draws, file = paste0(draws_dir, my_loc, ".csv"), row.names = F)
  cat(paste("Finished location", my_loc, "\n"))
}

#############################################################################################
###                           Processes for unrestricted locations                        ###
#############################################################################################

## Conditon
if (!(my_loc %in% restrict_loc)){

  ## Pull in draws
  cat("Loading in draws:\n")
  cat("\tGetting parent draws\n")
 
  parent_draws <- get_draws(gbd_id_type = "modelable_entity_id", gbd_id = ADDRESS, source = "epi",
                            location_id = my_loc, status = "best", decomp_step = "step4", year_id = c(1990, 1995, 2000, 2005, 2010, 2015, 2017, 2019), gbd_round_id = gbd_round_id)

  cat("\tGetting severe anemia draws\n")
  severe_draws <- get_draws(gbd_id_type = "modelable_entity_id", gbd_id = ADDRESS, source = "epi",
                            year_id = c(1990, 1995, 2000, 2005, 2010, 2015, 2017, 2019),
                            location_id = my_loc, status = "best",
                            decomp_step = "step4", gbd_round_id = gbd_round_id) 

  cat("\tGetting moderate anemia draws\n")
  mod_draws    <- get_draws(gbd_id_type = "modelable_entity_id", gbd_id = ADDRESS, source = "epi",
                            location_id = my_loc, status = "best", decomp_step = "step4", year_id = c(1990, 1995, 2000, 2005,2010, 2015, 2017, 2019), gbd_round_id = gbd_round_id)

  cat("\tGetting mild anemia draws\n")
  mild_draws   <- get_draws(gbd_id_type = "modelable_entity_id", gbd_id = ADDRESS, source = "epi",
                            location_id = my_loc, status = "best", decomp_step = "step4", year_id = c(1990, 1995, 2000, 2005, 2010, 2015, 2017, 2019), gbd_round_id = gbd_round_id)

  ## Remove unnecessary columns
  parent_draws[, c("metric_id", "modelable_entity_id", "model_version_id") := NULL]
  severe_draws[, c("metric_id", "modelable_entity_id", "model_version_id") := NULL]
  mild_draws[, c("metric_id", "modelable_entity_id", "model_version_id") := NULL]
  mod_draws[, c("metric_id", "modelable_entity_id", "model_version_id") := NULL]

  ## Change names of draws to prep for merges
  for (my_draw in grep("draw", names(parent_draws), value = T))
    setnames(parent_draws, old = my_draw, new = paste0("parent_", strsplit(my_draw, "_")[[1]][2]))

  for (my_draw in grep("draw", names(severe_draws), value = T))
    setnames(severe_draws, old = my_draw, new = paste0("severe_", strsplit(my_draw, "_")[[1]][2]))

  for (my_draw in grep("draw", names(mod_draws), value = T))
    setnames(mod_draws, old = my_draw, new = paste0("mod_", strsplit(my_draw, "_")[[1]][2]))

  for (my_draw in grep("draw", names(mild_draws), value = T))
    setnames(mild_draws, old = my_draw, new = paste0("mild_", strsplit(my_draw, "_")[[1]][2]))

  ## Merge everything together
  cat("Now mergeing together data and running subtractions to compute exclusivity draws.\n")
  merge_cols <- c("location_id", "measure_id", "sex_id", "year_id", "age_group_id")

  df <- merge(parent_draws, severe_draws, by = merge_cols)
  df <- merge(df, mod_draws, by = merge_cols)
  df <- merge(df, mild_draws, by = merge_cols)

  rm(list = c("parent_draws", "severe_draws", "mod_draws", "mild_draws"))

  ## Subtract to get asymptomatic
  for (i in 0:999){

    parent_name <- paste0("parent_", i)
    severe_name <- paste0("severe_", i)
    mod_name    <- paste0("mod_", i)
    mild_name   <- paste0("mild_", i)
    draw_name   <- paste0("draw_", i)

    df[, (draw_name) := get(parent_name) - get(severe_name) - get(mod_name) - get(mild_name)]
    df[, (parent_name) := NULL][, (severe_name) := NULL][, (mod_name) := NULL][, (mild_name) := NULL]
  }

  ## Check for negative values
  anynegs <- which(apply(df, 1, function(row) any(row < 0)))
  if (length(anynegs) != 0){
    cat("ERROR: Negative value detected. Now breaking \n")
    cat("\tNegatives at row: ",anynegs, "\n")
    write.csv(df, paste0(draws_dir, my_loc, ".csv"), row.names = F)
    cat("\tSaving original file in error directory. Zeroing cells with negative values\n")
    df[df < 0] <- 0
  }

  ## Done !
  cat("Draws complete!\n")
  write.csv(df, paste0(draws_dir, my_loc, ".csv"), row.names = F)

}

