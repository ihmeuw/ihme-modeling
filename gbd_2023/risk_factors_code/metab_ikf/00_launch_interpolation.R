#--------------------------------------------------------------
# Project: KD exposure interpolation
# Purpose: Launch and save KD exposure estimates
#--------------------------------------------------------------
rm(list = ls())

user <- Sys.info()["user"]
code_dir <- paste0("FILEPATH", user,"FILEPATH")
out_dir <- "FILEPATH"

# package requirements -------------------
library(tidyr)
source(paste0(code_dir,"general_func_lib.R"))
invisible(sapply(list.files("FILEPATH", full.names = T), source))

# Directories:
path_to_map <- "FILEPATH/annual_exposure_mapping.csv" #FIRST manually update year_end, release_id, crosswalk_version_id in this csv
release_id <- 16

### 01: Run Interpolation ---------------------------------------------------------------------------

# Get locations to parallelize
locations <- get_demographics(gbd_team = "epi", release_id = release_id)$location_id

script_01 <- paste0(code_dir, "kd/01a_interpolate_exposure.R")

# sbatch settings
q <- "long.q"
mem_free <- '8G'
threads <- 1
runtime <- "00:20:00"

# This launches a job for each location
for (loc in locations) {
  # create sbatch
  job_n <- paste0("kd_interpolate_exposure_loc_", loc)
  pass_args <- list(loc, path_to_map, release_id, out_dir)
  construct_sbatch(
    memory = mem_free,
    threads = threads,
    runtime = runtime,
    script = script_01,
    job_name = job_n,
    partition = q,
    pass = pass_args,
    submit = TRUE
  )
}


### Validations ---------------------------------------------------------------------
mapping <- read.csv(path_to_map)

# This will check each folder for correct number of files
for (i in unique(mapping$target_me_id)){
  print(paste("Checking MEID: ", i))
  draws_dir <- paste0(out_dir, i, "/")
  print(draws_dir)
  
  files_lst <- dir(draws_dir)
  print(paste("Number of files:", length(files_lst)))
  
  if (length(files_lst) == length(locations)) {
    message("Correct number of files found!")
  } else {
    stop("Incorrect number of files found - please check before saving results.")
  }
}


### 02: Save Results --------------------------------------------

mapping <- read.csv(path_to_map)
script_02 <- paste0(code_dir, "kd/01b_interpolate_exposure.R")

# sbatch settings
q <- "long.q"
mem_free <- '80G'
threads <- 15
runtime <- "24:00:00"

# create sbatch and launch
for (i in unique(mapping$target_me_id)){
  # create sbatch
  job_n <- paste0("kd_save_results_me_", i)
  pass_args <- list(i, path_to_map, release_id, out_dir)
  construct_sbatch(
    memory = mem_free,
    threads = threads,
    runtime = runtime,
    script = script_02,
    job_name = job_n,
    partition = q,
    pass = pass_args,
    submit = TRUE
  )
}


