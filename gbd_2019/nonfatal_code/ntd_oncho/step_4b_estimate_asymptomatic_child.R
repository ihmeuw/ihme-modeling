# The code is taking all oncho prevalence - the various final sequelas, if <0 then it 0's out

### ======================= BOILERPLATE ======================= ###
rm(list = ls())
code_root <-paste0("FILEPATH", Sys.info()[7])
data_root <- "FILEPATH"

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
  location_id <- 214
}

# Source relevant libraries
library(data.table)
library(stringr)
library(argparse, lib.loc= "FILEPATH")
source("FILEPATH")
source("FILEPATH")
library(parallel)
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")

code_dir   <- paste0(code_root, "FILEPATH")

source("FILEPATH")
my_shell <- paste0("FILEPATH")

gbd_round_id <- 6
decomp_step <- 'step4' 

### ======================= MAIN ======================= ###
## Parse Arguments
print(commandArgs())
parser <- ArgumentParser()
parser$add_argument("--param_path", help = "param_path", default = "168", type = "character")

args <- parser$parse_args()
print(args)
list2env(args, environment()); rm(args)
sessionInfo()

task_id <- as.integer(Sys.getenv("SGE_TASK_ID"))
params  <- fread(param_path)

my_loc <- params[task_id, location_id]

#############################################################################################
###                               Draw Computation function                               ###
#############################################################################################

process_draws <- function (parent_draws, severe_id, mod_id, mild_id, decomp_step) {

  ## Pull in draws
  cat("\tGetting severe draws\n")
  severe_draws <- get_draws(gbd_id_type = "modelable_entity_id", gbd_id = severe_id, source = "epi",
                            location_id = my_loc, status = "best", decomp_step = decomp_step, gbd_round_id = gbd_round_id)

  cat("\tGetting moderate draws\n")
  mod_draws    <- get_draws(gbd_id_type = "modelable_entity_id", gbd_id = mod_id, source = "epi",
                            location_id = my_loc, status = "best", decomp_step = decomp_step, gbd_round_id = gbd_round_id)

  cat("\tGetting mild draws\n")
  mild_draws   <- get_draws(gbd_id_type = "modelable_entity_id", gbd_id = mild_id, source = "epi",
                            location_id = my_loc, status = "best", decomp_step = decomp_step, gbd_round_id = gbd_round_id)

  ## Remove unnecessary columns
  severe_draws[, c("metric_id", "modelable_entity_id", "model_version_id") := NULL]
  mild_draws[, c("metric_id", "modelable_entity_id", "model_version_id") := NULL]
  mod_draws[, c("metric_id", "modelable_entity_id", "model_version_id") := NULL]

  ## Change names of draws to prep for merges
  for (my_draw in grep("draw", names(parent_draws), value = T))
    setnames(parent_draws, old = my_draw, new = paste0("parent_", strsplit(my_draw, "_")[[1]][2]))

  for (my_draw in grep("draw", names(severe_draws), value = T)){
    if (severe_id %in% c(ADDRESS)) severe_draws[, (my_draw) := get(my_draw) * (8/33)]
    setnames(severe_draws, old = my_draw, new = paste0("severe_", strsplit(my_draw, "_")[[1]][2]))
  }

  for (my_draw in grep("draw", names(mod_draws), value = T)){
    if (mod_id %in% c(ADDRESS)) mod_draws[, (my_draw) := get(my_draw) * (8/33)]
    setnames(mod_draws, old = my_draw, new = paste0("mod_", strsplit(my_draw, "_")[[1]][2]))
  }

  for (my_draw in grep("draw", names(mild_draws), value = T)){
    if (mild_id %in% c(ADDRESS)) mild_draws[, (my_draw) := get(my_draw) * (8/33)]
    setnames(mild_draws, old = my_draw, new = paste0("mild_", strsplit(my_draw, "_")[[1]][2]))
  }

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
  df <- negative_check(df)
  return(df)
}

process_itch_draws <- function(parent_draws, severe_id, mod_id, decomp_step){

  ## Pull in draws
  cat("\tGetting severe draws\n")
  severe_draws <- get_draws(gbd_id_type = "modelable_entity_id", gbd_id = severe_id, source = "epi",
                            location_id = my_loc, status = "best", decomp_step = decomp_step, gbd_round_id = gbd_round_id)

  cat("\tGetting moderate draws\n")
  mod_draws    <- get_draws(gbd_id_type = "modelable_entity_id", gbd_id = mod_id, source = "epi",
                            location_id = my_loc, status = "best", decomp_step = decomp_step, gbd_round_id = gbd_round_id)

  ## Remove unnecessary columns
  severe_draws[, c("metric_id", "modelable_entity_id", "model_version_id") := NULL]
  mod_draws[, c("metric_id", "modelable_entity_id", "model_version_id") := NULL]

  ## Change names of draws to prep for merges
  for (my_draw in grep("draw", names(parent_draws), value = T))
    setnames(parent_draws, old = my_draw, new = paste0("parent_", strsplit(my_draw, "_")[[1]][2]))

  for (my_draw in grep("draw", names(severe_draws), value = T))
    setnames(severe_draws, old = my_draw, new = paste0("severe_", strsplit(my_draw, "_")[[1]][2]))

  for (my_draw in grep("draw", names(mod_draws), value = T))
    setnames(mod_draws, old = my_draw, new = paste0("mod_", strsplit(my_draw, "_")[[1]][2]))

  ## Merge everything together
  cat("Now mergeing together data and running subtractions to compute exclusivity draws.\n")
  merge_cols <- c("location_id", "measure_id", "sex_id", "year_id", "age_group_id")

  df <- merge(parent_draws, severe_draws, by = merge_cols)
  df <- merge(df, mod_draws, by = merge_cols)
  rm(list = c("parent_draws", "severe_draws", "mod_draws"))

  ## Subtract to get asymptomatic
  for (i in 0:999){

    parent_name <- paste0("parent_", i)
    severe_name <- paste0("severe_", i)
    mod_name    <- paste0("mod_", i)
    draw_name   <- paste0("draw_", i)

    df[, (draw_name) := get(parent_name) - get(severe_name) - get(mod_name)]
    df[, (parent_name) := NULL][, (severe_name) := NULL][, (mod_name) := NULL]
  }

  ## Check for negative values
  df <- negative_check(df)
  return(df)
}

## Function to check for negative values
negative_check <- function(dt){

  anynegs <- which(apply(dt, 1, function(row) any(row < 0)))
  if (length(anynegs) != 0){
    cat("ERROR: Negative value detected. Now breaking \n")
    cat("\tNegatives at row: ",anynegs, "\n")
    cat("\tSaving original file in error directory. Zeroing cells with negative values\n")
    dt[dt < 0] <- 0
  }
  return(dt)
}

#############################################################################################
###                           Processes for restricted locations                          ###
#############################################################################################

## Set up geographic restrictions first:
restrict_loc <- get_model_results("epi", gbd_id = ADDRESS, measure_id = 5, age_group_id = 27, year_id = 2017,
                                  sex_id = 1, location_set_version_id = 319, gbd_round_id = gbd_round_id, decomp_step = decomp_step)

restrict_loc <- restrict_loc[mean == 0]
restrict_loc <- unique(restrict_loc$location_id)



## Condition with geographic restriction
if (my_loc %in% restrict_loc){
  cat("location has geogrpahic restrictions. Zeroing everything out. \n")

  # pulls the location-specific all prevalence draw since will be zero
  my_draws <- get_draws(gbd_id_type = "modelable_entity_id", gbd_id = ADDRESS, source = "epi",
                        location_id = my_loc, status = "best", decomp_step = decomp_step, gbd_round_id = gbd_round_id)

  my_draws[, c("modelable_entity_id", "metric_id", "model_version_id") := NULL]

  write.csv(my_draws, paste0(draws_dir, "FILEPATH", my_loc, ".csv"), row.names = F)

}

#############################################################################################
###                           Processes for unrestricted locations                        ###
#############################################################################################

## Conditon
if (!(my_loc %in% restrict_loc)){

  cat("Loading in draws:\n")
  cat("\tGetting parent draws\n")
  my_parent <- get_draws(gbd_id_type = "modelable_entity_id", gbd_id = ADDRESS, source = "epi",
                         location_id = my_loc, status = "best", decomp_step = decomp_step, gbd_round_id = gbd_round_id)

  
  my_parent[, c("metric_id", "modelable_entity_id", "model_version_id") := NULL]
  my_parent <- process_draws(my_parent, 2515, 1496, 1495, decomp_step = decomp_step)FILEPATH 
  my_parent <- process_draws(my_parent, 2956, 2958, 2957, decomp_step = decomp_step)FILEPATH 
  my_parent <- process_itch_draws(my_parent, 2621, 2620, decomp_step = decomp_step)FILEPATH 

  ## Done !
  cat("Draws complete!\n")
  write.csv(my_parent, paste0(draws_dir, "FILEPATH", my_loc, ".csv"), row.names = F)

}