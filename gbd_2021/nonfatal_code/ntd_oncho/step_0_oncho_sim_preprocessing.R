# NTDS: Onchocerciasis
# Purpose: Applies preprocess_afr_draws function
# Notes: converts case to prevalence; output to oncho_sim folder
#
### ======================= BOILERPLATE ======================= ###
rm(list = ls())
code_root <-paste0("FILEPATH", Sys.info()[7])
data_root <- "FILEPATH"

# Toggle (Prod Arg Parsing VS. Interactive Dev) Common   IO Paths
if (!is.na(Sys.getenv()["EXEC_FROM_ARGS"][[1]])) {
  library(argparse)
  print(commandArgs())
  parser <- ArgumentParserage
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

#'[ new run dir]
source(paste0(code_root, "FILEPATH"))

params_dir <- paste0(data_root, "FILEPATH")
run_file <- fread(paste0(params_dir, 'FILEPATH'))
run_folder_path <- run_file[nrow(run_file), run_folder_path]
draws_dir <- paste0(run_folder_path, "FILEPATH")
interms_dir <- paste0(run_folder_path, "FILEPATH")


# Source relevant libraries
code_dir   <- paste0(code_root, "FILEPATH")
my_shell <- "FILEPATH"

library(readstata13)
library(stringr)
source("FILEPATH/get_location_metadata.R")
source("FILEPATH/get_population.R")
source("FILEPATH/get_demographics.R")
source("FILEPATH/submit_function.R")
source(paste0(code_dir, "/preprocess_afr_draws.R"))

gbd_round_id <- ADDRESS
decomp_step <- ADDRESS

### ======================= MAIN ======================= ###
# pulling from GBD 2019 location - should be updated in future iterations
study_dems <- get_demographics("ADDRESS", gbd_round_id = gbd_round_id)
OCP_data <- fread("FILEPATH")
APOC_data <- fread("FILEPATH")

ifelse(!dir.exists(paste0(draws_dir, "FILEPATH")), dir.create(paste0(draws_dir, "FILEPATH")), FALSE)

message('Preprocessing OCP data...')
preprocess_afr_draws(data = OCP_data, out_dir = paste0(draws_dir, "FILEPATH"))
message('Preprocessing APOC data...')
preprocess_afr_draws(data = APOC_data, out_dir = paste0(draws_dir, "FILEPATH"))
