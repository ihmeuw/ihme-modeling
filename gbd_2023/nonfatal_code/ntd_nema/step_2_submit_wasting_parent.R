# Purpose: Pulls in child script for calculating wasting estimates
#
#############################################################################################
###'                                [General Set-Up]                                      ###
#############################################################################################

rm(list = ls())
code_root <- "FILEPATH"
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
}

# packages

source("FILEPATH/get_draws.R")
source("FILEPATH/get_model_results.R")
source("FILEPATH/get_location_metadata.R")
source("FILEPATH/get_demographics.R")
source("FILEPATH/get_population.R")
library(data.table)
library(stringr)
library(readstata13)
library(dplyr)
library(argparse)
source(paste0(code_root, '/FILEPATH/sbatch.R'))
source(paste0(code_root, "/FILEPATH/processing.R"))
shell  <- "/FILEPATH.sh"

run_file <- fread(paste0(data_root, "/FILEPATH/run_file.csv"))
run_dir <- run_file[nrow(run_file), run_folder_path]
crosswalks_dir    <- paste0(run_dir, "/crosswalks/")
draws_dir <- paste0(run_dir, "/draws/")
interms_dir <- paste0(run_dir, "/interms/")

params_dir <- paste0(data_root, "/FILEPATH/")
code_dir   <- paste0(code_root, "/FILEPATH/")

release_id <- ADDRESS

#############################################################################################
###'                                     [Sbatch Prep]                                    ###
#############################################################################################

ifelse(!dir.exists(paste0(draws_dir, "ADDRESS/")), dir.create(paste0(draws_dir, "ADDRESS/")), FALSE)
ifelse(!dir.exists(paste0(draws_dir, "ADDRESS/")), dir.create(paste0(draws_dir, "ADDRESS/")), FALSE)
ifelse(!dir.exists(paste0(draws_dir, "ADDRESS/")), dir.create(paste0(draws_dir, "ADDRESS/")), FALSE)

demos       <- get_demographics(gbd_team = "epi", release_id = release_id)
locs        <- demos$location_id
loc_ids     <- data.table(location_id = locs)

#' add new locations to grs
grs_hook <- fread(paste0(params_dir, 'FILEPATH.csv'))
grs_ascar <- fread(paste0(params_dir, 'FILEPATH.csv'))
grs_trichur <- fread(paste0(params_dir, 'FILEPATH.csv'))

grs_hook <- subset(grs_hook, most_detailed == 1 & year_start == 2022)
grs_hook$restrict_hook <- ifelse(grs_hook$value_endemicity == 0, 1, 0)
grs_hook <- subset(grs_hook, select = c('location_id','restrict_hook'))

grs_ascar <- subset(grs_ascar, most_detailed == 1 & year_start == 2022)
grs_ascar$restrict_ascar <- ifelse(grs_ascar$value_endemicity == 0, 1, 0)
grs_ascar <- subset(grs_ascar, select = c('location_id','restrict_ascar'))

grs_trichur <- subset(grs_trichur, most_detailed == 1 & year_start == 2022)
grs_trichur$restrict_trichur <- ifelse(grs_trichur$value_endemicity == 0, 1, 0)
grs_trichur <- subset(grs_trichur, select = c('location_id','restrict_trichur'))

param_map <- merge(grs_hook,grs_ascar, by = 'location_id')
param_map <- merge(param_map, grs_trichur, by = 'location_id')
param_map$release_id <- release_id

num_jobs  <- nrow(param_map)
fwrite(param_map, paste0(interms_dir, "FILEPATH.csv"), row.names = F)

#############################################################################################
###'                                        [sbatch]                                      ###
#############################################################################################
fthreads <- 3
mem_alloc <- 10
time_alloc <- "00:20:00"
script <- paste0(code_dir, "FILEPATH.R")
arg_name <- list("--param_path", paste0(interms_dir, "FILEPATH.csv"))

sbatch(job_name = "COMMENT",
       shell    = shell,
       code     = script,
       output   = "/FILEPATH/output/%x.o%j",
       error    = "/FILEPATH/errors/%x.e%j",
       args     = arg_name,
       project  = "ADDRESS",
       num_jobs = num_jobs,
       threads  = fthreads,
       memory   = mem_alloc,
       time     = time_alloc,
       queue    = "ADDRESS")
