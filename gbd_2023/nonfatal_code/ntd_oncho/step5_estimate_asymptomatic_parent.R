# NTDS: Onchocerciasis
# Purpose: The code is taking all oncho prevalence - the various final sequelas, if <0 then it 0's out
# Note: Requries the post-squeezed vision impairment meids to be bested
#
### ======================= BOILERPLATE ======================= ###
rm(list = ls())
code_root <-"FILEPATH"
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
library(stringr)
source("/FILEPATH/get_bundle_version.R")
source("FILEPATH/get_draws.R")
source("FILEPATH/get_location_metadata.R")
source("FILEPATH/get_population.R")
library(argparse)
source("FILEPATH")

#'[ new run dir]
source(paste0(code_root, "FILEPATH"))

params_dir <- paste0(data_root, "FILEPATH")
run_file <- fread(paste0(params_dir, 'FILEPATH'))
run_folder_path <- run_file[nrow(run_file), run_folder_path]
draws_dir <- paste0(run_folder_path, "FILEPATH")
interms_dir <- paste0(run_folder_path, "FILEPATH")

code_dir   <- paste0(code_root, "FILEPATH")

source(paste0(code_root, "FILEPATH"))
my_shell <- paste0(code_root, "FILEPATH")

release_id <- ADDRESS
### ======================= MAIN ======================= ###
 
output_directory <- paste0(draws_dir, "/FILEPATH/")
ifelse(!dir.exists(paste0(output_directory)), dir.create(paste0(output_directory)), FALSE)
study_dems <- get_demographics("ADDRESS", release_id = gbd_rourelease_idnd_id)
locs <-  study_dems$location_id
no_file <- c()

for (loc in locs){
  if(!(file.exists(paste0(output_directory, loc, ".csv")))) { no_file <- c(no_file, loc)}
}

locs <- no_file
param_map <- data.table(location_id = locs,release_id = release_id)

# grs
location_md <- get_location_metadata(location_set_id=ADDRESS, release_id=release_id)
nig_sn <- location_md[parent_id == ADDRESS, location_id]
eth_sn <- location_md[parent_id == ADDRESS, location_id]

endemic_locations <- get_bundle_version(bundle_version_id = 'ADDRESS')
endemic_locations <- unique(endemic_locations[,location_id])
endemic_locations <- c(endemic_locations, nig_sn, IDS, eth_sn)
endemic_locations_table <- data.table(endemic = 1, location_id = endemic_locations)

locs <- locs[most_detailed == 1, location_id]
locations_to_zero <- setdiff(locs, endemic_locations)
locations_to_zero_table <- data.table(endemic = 0, location_id = locations_to_zero)

gr_table <- rbind(locations_to_zero_table, endemic_locations_table)

param_map <- merge(param_map, gr_table, by = 'location_id', all.x = TRUE)

num_jobs  <- nrow(param_map)
fwrite(param_map, paste0(interms_dir, "FILEPATH"), row.names = F)

