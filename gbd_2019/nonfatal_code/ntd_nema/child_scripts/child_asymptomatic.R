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
source(paste0(FILEPATH))
source(paste0(FILEPATH))
source(paste0(FILEPATH))
source(paste0(FILEPATH))

library(argparse, lib.loc= paste0(FILEPATH))
library(assertable)
library(parallel)
library(data.table)
library(readstata13)

## Parse Arguments
print(commandArgs())
parser <- ArgumentParser()
parser$add_argument("--param_path_asymp", help = "param path", default = "", type = "character")

args <- parser$parse_args()
print(args)
list2env(args, environment()); rm(args)
sessionInfo()

gbd_round_id <- 6

#############################################################################################
###                                        Set-Up                                         ###
#############################################################################################

## Array Job Set-Up
task_id <- as.integer(Sys.getenv(ADDRESS))
params  <- fread(param_path_asymp)

my_loc <- params[task_id, location_ids]
draws_directory <- params[task_id, draws_directory]
meids_directory <- params[task_id, meids_directory]
my_worm <- params[task_id, my_worm]
params_dir <- params[task_id, params_directory]

## Get correct MEIDS
meids     <- fread(meids_directory)
meids     <- meids[worm == my_worm]
all_cases <- meids[model == "all"]$meid
heavy     <- meids[model == "heavy"]$meid
mild      <- meids[model == "mild"]$meid
asymp     <- meids[model == "asymp"]$meid

#############################################################################################
###                           Processes for restricted locations                          ###
#############################################################################################

## Set up geographic restrictions first:

# GR's  
if (my_worm == "ascariasis") {short <- "ascar"}
if (my_worm == "trichuriasis") {short <- "trichur"}
if (my_worm == "hookworm") {short <- "hook"}

restrict_loc <- fread(paste0(params_dir, FILEPATH, short, FILEPATH))
restrict_loc <- unique(restrict_loc[value_endemicity == 0, location_id])

if (my_loc %in% restrict_loc){
  
  cat("location has geographic restrictions. Zeroing everything out. \n")
  
  # Canada has zero cases, and is the skeleton for all 0 draws 
  worm_draws <- get_draws(gbd_id_type = "modelable_entity_id", gbd_id = all_cases, source = "epi",
                          location_id = 101, decomp_step = "step1",  version_id = ADDRESS, gbd_round_id = gbd_round_id)
  # ^^ do not edit -- need static generic zero draw file
  
  worm_draws[, c("modelable_entity_id", "metric_id", "model_version_id") := NULL]
  worm_draws[, location_id := my_loc]
  
  write.csv(worm_draws, file = paste0(draws_directory, my_loc, ".csv"), row.names = F)
  cat(paste("Finished location", my_loc, "\n"))
}


#############################################################################################
###                           Processes for unrestricted locations                        ###
#############################################################################################

## Conditon
if (!(my_loc %in% restrict_loc)){

  ## Pull in draws
  cat("Loading in parent, heavy, and moderate draws.\n")
  parent_draws <- get_draws(gbd_id_type = "modelable_entity_id", gbd_id = all_cases, source = "epi",
                            location_id = my_loc, decomp_step = "step4", status = "best", gbd_round_id = gbd_round_id)

  heavy_draws  <- get_draws(gbd_id_type = "modelable_entity_id", gbd_id = heavy, source = "epi",
                            location_id = my_loc, decomp_step = "step4", status = "best", gbd_round_id = gbd_round_id)

  med_draws    <- get_draws(gbd_id_type = "modelable_entity_id", gbd_id = mild, source = "epi",
                            location_id = my_loc, decomp_step = "step4", status = "best", gbd_round_id = gbd_round_id) 
  
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
    print(i)
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
    write.csv(df, paste0(draws_directory, "ERROR_", my_loc,".csv"), row.names = F)
    break
  }

  ## Done !
  cat("Draws complete!\n")
  write.csv(df, paste0(draws_directory, my_loc, ".csv"), row.names = F)

}