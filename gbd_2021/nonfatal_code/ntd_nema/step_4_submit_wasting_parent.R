# Purpose: Pulls in child script for calculating wasting estimates
#
#############################################################################################
###'                                [General Set-Up]                                      ###
#############################################################################################

rm(list = ls())
code_root <-paste0("FILEPATH", Sys.info()[7])
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
  location_id <- 214
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

# set-up run directory

run_file <- fread(paste0(data_root, "FILEPATH"))
run_dir <- run_file[nrow(run_file), run_folder_path]
crosswalks_dir    <- paste0(run_dir, "FILEPATH")
draws_dir <- paste0(run_dir, "FILEPATH")
interms_dir <- paste0(run_dir, "FILEPATH")

params_dir <- paste0(data_root, "FILEPATH")
code_dir   <- paste0(code_root, "FILEPATH")

# parallelize helpers 

source(paste0(code_root, "FILEPATH"))
my_shell <- paste0(code_root, "FILEPATH")
source(paste0(code_root, "FILEPATH"))

decomp_step <- ADDRESS
gbd_round_id <- ADDRESS

#############################################################################################
###'                                     [Launch Prep]                                      ###
#############################################################################################

ifelse(!dir.exists(paste0(draws_dir, "FILEPATH")), dir.create(paste0(draws_dir, "FILEPATH")), FALSE)
ifelse(!dir.exists(paste0(draws_dir, "FILEPATH")), dir.create(paste0(draws_dir, "FILEPATH")), FALSE)
ifelse(!dir.exists(paste0(draws_dir, "FILEPATH")), dir.create(paste0(draws_dir, "FILEPATH")), FALSE)

demos       <- get_demographics(gbd_team = ADDRESS, gbd_round_id = gbd_round_id)
locs        <- demos$location_id

all_locs <- copy(locs)
 loc_ids     <- data.table(location_id = locs)

#' add new locations to grs
grs_hook            <- fread("FILEPATH")
grs_ascar           <- fread("FILEPATH")
grs_trichur         <- fread("FILEPATH")

grs_hook[, restrict_hook := 1]
grs_ascar[, restrict_ascar := 1]           
grs_trichur[, restrict_trichur := 1]    

grs_hook <- grs_hook[, .(location_id, restrict_hook)]
grs_ascar <- grs_ascar[, .(location_id, restrict_ascar)]        
grs_trichur <- grs_trichur[, .(location_id, restrict_trichur)]    

grs_hook    <- distinct(grs_hook)
grs_ascar   <- distinct(grs_ascar)
grs_trichur <- distinct(grs_trichur)  

param_map <- merge(loc_ids, grs_hook, by = "location_id", all.x = TRUE)
param_map <- merge(param_map, grs_ascar, by = "location_id", all.x = TRUE)
param_map <- merge(param_map, grs_trichur, by = "location_id", all.x = TRUE)

param_map[is.na(restrict_trichur), restrict_trichur := 0 ]
param_map[is.na(restrict_ascar), restrict_ascar := 0 ]
param_map[is.na(restrict_hook), restrict_hook := 0 ]

param_map[, decomp_step := decomp_step]
param_map[, gbd_round_id := gbd_round_id]

num_jobs  <- nrow(param_map)
fwrite(param_map, paste0(interms_dir, "FILEPATH"), row.names = F)

#############################################################################################
###'                                        [Launch]                                        ###
#############################################################################################

LAUNCH(job_name = "STH_Wasting",
     shell    = my_shell,
     code     = paste0(code_dir, "FILEPATH"),
     args     = list("--param_path", paste0(interms_dir, "FILEPATH")),
     project  = ADDRESS,
     m_mem_free = "10G",
     fthread = "3",
     archive = NULL,
     h_rt = "00:00:25:00",
     queue = "long.q",
     num_jobs = num_jobs)
