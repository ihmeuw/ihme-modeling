# Purpose: calculate asymptomatic sequelae (asymp = all - heavy - mild)
#
### ======================= BOILERPLATE ======================= ###
rm(list = ls())
code_root <-"FILEPATH"
data_root <- "FILEPATH"

## Load functions and packages
source(paste0("FILEPATH/get_location_metadata.R"))
source(paste0("FILEPATH/get_model_results.R"))
source(paste0("FILEPATH/get_draws.R"))
source(paste0(code_root, 'FILEPATH/processing.R'))
library(argparse)
library(data.table)
library(readstata13)

# set run dir
params_dir <- "FILEPATH"
logs_dir <- "FILEPATH"
run_file <- fread(paste0(data_root, "FILEPATH"))
run_dir <- run_file[nrow(run_file), run_folder_path]
crosswalks_dir    <- paste0(run_dir, "FILEPATH")
code_dir <- paste0(code_root, 'FILEPATH')

## Parse Arguments
print(commandArgs())
parser <- ArgumentParser()
parser$add_argument("--param_path_asymp", help = "param path", default = "", type = "character")

args <- parser$parse_args()
print(args)
list2env(args, environment()); rm(args)
sessionInfo()

#############################################################################################
###                                        Set-Up                                         ###
#############################################################################################

## Array Job Set-Up

task_id <- as.integer(Sys.getenv("ADDRESS"))
params  <- fread(param_path_asymp)

my_loc <- params[task_id, location_ids]
my_worm <- params[task_id, my_worm]
release_id <- params[task_id, release_id]

## Get correct MEIDS
meids_directory     <- paste0(params_dir, "FILEPATH")
meids     <- fread(meids_directory)
meids     <- meids[worm == my_worm]
all_cases <- meids[model == "all"]$meid
heavy     <- meids[model == "heavy"]$meid
mild      <- meids[model == "mild"]$meid
asymp     <- meids[model == "asymptomatic"]$meid
if (all_cases == ADDRESS) {asymp <- ADDRESS}

draws_directory <- paste0('FILEPATH')

#############################################################################################
###                           Processes for restricted locations                          ###
#############################################################################################

## Set up geographic restrictions first:

# GR's  

if (my_worm == "ascariasis") {short <- "ascar"}
if (my_worm == "trichuriasis") {short <- "trichur"}
if (my_worm == "hookworm") {short <- "hook"}

restrict_loc <- fread(paste0(params_dir, "FILEPATH"))
restrict_loc <- unique(restrict_loc[value_endemicity == 0, location_id])

if (my_loc %in% restrict_loc){
  
  data <- gen_zero_draws(model_id= asymp, location_id = NA, measure_id = 5, release_id = release_id, team = 'ADDRESS')
  data[, metric_id := 3]
  data[, model_id:= asymp]
  write.csv(data, file = paste0(draws_directory, 'FILEPATH'), row.names = F)
  
}

#############################################################################################
###                           Processes for unrestricted locations                        ###
#############################################################################################

## Conditon
if (!(my_loc %in% restrict_loc)){

  ## Pull in draws
  cat("Loading in parent, heavy, and moderate draws.\n")
  parent_draws <- get_draws(gbd_id_type = "model_id", gbd_id = all_cases, source = "ADDRESS",
                            location_id = my_loc, release_id = release_id, status = "best")

  heavy_draws  <- get_draws(gbd_id_type = "model_id", gbd_id = heavy, source = "ADDRESS",
                            location_id = my_loc, release_id = release_id, status = "best")

  med_draws    <- get_draws(gbd_id_type = "model_id", gbd_id = mild, source = "ADDRESS",
                            location_id = my_loc, release_id = release_id, status = "best") 
  
  ## Remove unnecessary columns
  parent_draws[, c("metric_id", "model_id", "version_id") := NULL]
  heavy_draws[, c("metric_id", "model_id", "version_id") := NULL]
  med_draws[, c("metric_id", "model_id", "version_id") := NULL]

  ## Change names of draws to prep for merges
  for (my_draw in grep("draw", names(parent_draws), value = T))
    setnames(parent_draws, old = my_draw, new = paste0("pFILEPATH", strsplit(my_draw, "_")[[1]][2]))

  for (my_draw in grep("draw", names(heavy_draws), value = T))
    setnames(heavy_draws, old = my_draw, new = paste0("FILEPATH", strsplit(my_draw, "_")[[1]][2]))

  for (my_draw in grep("draw", names(med_draws), value = T))
    setnames(med_draws, old = my_draw, new = paste0("FILEPATH", strsplit(my_draw, "_")[[1]][2]))

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
    write.csv(df, paste0(draws_directory, "FILEPATH"), row.names = F)
    break
  }

  ## Done !
  cat("Draws complete!\n")
  df[, metric_id := 3]
  df[, model_id:= asymp]
  write.csv(df, paste0(draws_directory, "FILEPATH"), row.names = F)

}