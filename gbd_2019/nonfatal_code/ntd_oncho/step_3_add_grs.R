
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
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
library(data.table)
library(foreign)
library(maptools) 
library(rgeos) 
library(rgdal) 
require(raster)
library(sp)
library(stats)
library(data.table) 

run_file <- fread(paste0(data_root, 'FILEPATH'))
run_path <- run_file[nrow(run_file), run_folder_path]

params_dir <- paste0("FILEPATH")
draws_dir <- paste0(run_path, 'FILEPATH')
interms_dir <- paste0(run_path, 'FILEPATH')
logs_dir <- paste0(run_path, 'FILEPATH')

code_dir   <- paste0(code_root, "FILEPATH")
gbd_round_id <- ADDRESS
decomp_step <- 'step1'
meids <- c(ADDRESS1, ADDRESS2)
study_dems <- get_demographics(gbd_team = "epi", gbd_round_id = gbd_round_id)
gbdlocs <- study_dems$location_id

numextract <- function(string){ 
  str_extract(string, "\\-*\\d+")
} 

### ======================= MAIN EXECUTION ======================= ###

locs <- study_dems$location_id

files <- list.files(paste0(draws_dir, 'ADDRESS'))
locs_done <- as.numeric(numextract(files))
add_zeroes <- locs[!(locs %in% locs_done)]
zeros <- gen_zero_draws(modelable_entity_id = ADDRESS1, location_id = NA, measure_id = c(5), metric_id = 3, gbd_round_id = gbd_round_id, team = 'epi')
i <- 0

for (loc in add_zeroes){
  zeros[, location_id := loc]
  for (meid in meids){
    if (!(meid %in% c(ADDRESS1, ADDRESS2) & loc == 157)){
      # do not zero yemen for ADDRESS1 or ADDRESS2
      zeros[, modelable_entity_id := as.integer(modelable_entity_id)]
      fwrite(zeros, paste0(draws_dir, "/ILEPATH/", loc, ".csv"))
    }}
  i <- i + 1
  cat(paste0("\n Done with: ", i, " of ", length(add_zeroes)))
}