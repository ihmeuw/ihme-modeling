# NTDS: Onchocerciasis
# Purpose: apply geographic restrictions
#
### ======================= BOILERPLATE ======================= ###
rm(list = ls())
code_root <- "FILEPATH"
data_root <- "FILEPATH"

# Toggle (Prod Arg Parsing VS. Interactive Dev) Common   IO Paths
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

# Source relevant libraries
library(data.table)
source("FILEPATH/get_demographics.R")
source("FILEPATH/get_age_metadata.R")
source("FILEPATH/get_location_metadata.R")
source("FILEPATH/get_bundle_version.R")
source("FILEPATH/get_location_metadata.R")
source(paste0(code_root, "FILEPATH"))

run_file <- fread(paste0(data_root, 'FILEPATH'))
run_path <- run_file[nrow(run_file), run_folder_path]

params_dir <- paste0("FILEPATH")
draws_dir <- paste0(run_path, 'FILEPATH')
interms_dir <- paste0(run_path, 'FILEPATH')
logs_dir <- paste0(run_path, 'FILEPATH')

release_id = ADDRESS
meids <- c(ADDRESS)
study_dems <- get_demographics(gbd_team = "ADDRESS", release_id = release_id)

locs <- get_location_metadata(location_set_id=35, release_id=release_id)
nig_sn <- locs[parent_id == ADDRESS, location_id]
eth_sn <- locs[parent_id == ADDRESS, location_id]

endemic_locations <- get_bundle_version(bundle_version_id = 'ADDRESS')
endemic_locations <- unique(endemic_locations[,location_id])
endemic_locations <- c(endemic_locations, nig_sn, IDS, eth_sn)

locs <- locs[most_detailed == 1, location_id]
locations_to_zero <- setdiff(locs, endemic_locations)
  
zeros <- gen_zero_draws(model_id= ADDRESS, location_id = NA, measure_id = c(5), metric_id = 3, release_id = release_id, team = 'ADDRESS')

i <- 0

for (loc in locations_to_zero){
  zeros[, location_id := loc]
  for (meid in meids){
    zeros[, model_id:= as.integer(meid)]
    if (!(meid %in% c(ADDRESS) & loc == ADDRESS)){
      fwrite(zeros, paste0(draws_dir, 'FILEPATH'))
    }}
  i <- i + 1
  cat(paste0("\n Done with: ", i, " of ", length(locations_to_zero)))
}

for (loc in endemic_locations){
  draw <- fread(paste0(draws_dir, 'FILEPATH'))
  draw[age_group_id %in% c(ADDRESS), paste0('draw_', 0:999) := 0]
  fwrite(draw, paste0(draws_dir, 'FILEPATH'))
}
