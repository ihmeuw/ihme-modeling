################################################################################
## This script applies a correction to Chagas results. We don't have subnational
## migration data  for the GBD subnat countries (except US)
################################################################################

rm(list=ls())
data_root <- "FILEPATH"
cause <- "ntd_chagas"
run_date <- "ADDRESS"

## Define paths 
# Toggle btwn production arg parsing vs interactive development
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
  params_dir  <- paste0(data_root, FILEPATH)
  draws_dir   <- paste0(data_root, FILEPATH)
  interms_dir <- paste0(data_root, FILEPATH)
  logs_dir    <- paste0(data_root, FILEPATH)
}

source("FILEPATH/get_model_results.R")
source("FILEPATH/get_location_metadata.R")
source("FILEPATH/get_population.R")
source("FILEPATH/get_age_metadata.R")
source("FILEPATH/get_covariate_estimates.R")
source("FILEPATH/get_outputs.R")
source("FILEPATH/get_draws.R")
source("FILEPATH/interpolate.R")
library(data.table)
library(dplyr)
library(ggplot2)
library(openxlsx)
library(readxl)
library(tidyr)
library(foreign)
library(haven)
################################################################################
## set paths, read in params
path <- paste0(data_root, FILEPATH)
param_path <- paste0(path, FILEPATH)
input_path <- paste0(path, FILEPATH)

me_ids <- c(ADDRESS)

# subnat location param file
subnats <- fread(paste0(param_path,  FILEPATH))
#drop nats for which files aren't produced (these don't have LA migrants)
subnats <- subnats[!(subnats$parent_id %in% c(51,62,67,142,165,6,11,16,179,180,196,214,163)),]
nat_locs <- unique(subnats$parent_id)

################################################################################
## loop through each subnat location
for (me in me_ids){
  message("Now processing me_id: ", me)
  for (loc in nat_locs){
    message("Now processing location: ", loc)

    # set location params to just subnationals of the national location being processed
    sub_loc <- subnats[subnats$parent_id == loc,]
    
    # read in parent df
    parent_df <-  fread(paste0(input_path, FILEPATH))
    
    for (child_loc in unique(sub_loc$location_id)){
      df <- parent_df
      df$location_id <- child_loc
      
      write.csv(df, paste0(input_path, FILEPATH),  row.names = FALSE)
    }
    
  }
}
