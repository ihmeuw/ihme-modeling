#' [Title: FBT Apply GRs Child
#' [Notes: Writes 0's for non-endemic locations, pass through estimate for endemic locations

#############################################################################################
###'                                [General Set-Up]                                      ###
#############################################################################################

rm(list = ls())

# root path

code_root <- paste0("FILEPATH")
data_root <- "FILEPATH"

# Toggle (Prod Arg Parsing VS. Interactive Dev) Common   IO Paths
if (!is.na(Sys.getenv()["EXEC_FROM_ARGS"][[1]])) {
  library(argparse, lib.loc = "FILEPATH")
  print(commandArgs())
  parser <- ArgumentParser()
  parser$add_argument("--params_dir", type = "character")
  parser$add_argument("--draws_dir", type = "character")
  parser$add_argument("--interms_dir", type = "character")
  parser$add_argument("--logs_dir", type = "character")
  parser$add_argument("--location_id", type = "character")
  args <- parser$parse_args()
  print(args)
  list2env(args, environment()); rm(args)
  sessionInfo()
} else {
  params_dir <- paste0(data_root, "FILEPATH")
  draws_dir <- paste0(data_root, "FILEPATH/draws")
  interms_dir <- paste0(data_root, "FILEPATH/interms")
  logs_dir <- paste0(data_root, "FILEPATH/logs")
  #location_id <- 68
}
unloadNamespace("argparse")

# packages

library(data.table)
library(stringr)

source("FILEPATH/get_draws.R")
source("FILEPATH/get_model_results.R")
source("FILEPATH/get_location_metadata.R")
source("FILEPATH/get_demographics.R")
source(paste0(code_root, 'FILEPATH/processing.R'))
library(argparse, lib.loc= "FILEPATH")
detach("package:argparse", unload=TRUE)

gbd_round_id <- ADDRESS
decomp_step <- ADDRESS

#############################################################################################
###'                              [Estimate meIDs]                                        ###
#############################################################################################

study_dems <- readRDS(paste0(data_root, 'FILEPATH', gbd_round_id, '.rds'))
years <- study_dems$year_id
locs <- study_dems$location_id

est_locs       <- fread(paste0(params_dir, "/fbt_grs.csv"))
zero_draw_file <- gen_zero_draws(model_id= NA, location_id = NA, measure_id = 5, year_id = years, gbd_round_id = gbd_round_id, team = ADDRESS)

meids < (ADDRESS)

for (element in meids){
  parent_meid <- element[[1]]
  limit_meid  <- element[[2]]
  
  est_locs_meid <- est_locs[meid == parent_meid, location_id]


    if (location_id %in% est_locs_meid){
      draws <- get_draws(gbd_id_type = ADDRESS,
                         gbd_id = parent_meid,
                         source = ADDRESS,
                         measure_id = c(5),
                         year_id = years,
                         location_id = location_id,
                         status = "ADDRESS",
                         sex_id = c(1,2),
                         decomp_step = decomp_step,
                         gbd_round_id = gbd_round_id)

      
      draws[age_group_id <= 5 | age_group_id %in% c(388,389,238), paste0("draw_", 0:999) := 0]
      draws[, model_id:= limit_meid]
      draws[, metric_id := 3]
      write.csv(draws, paste0(draws_dir, '/', limit_meid, "/", location_id, ".csv"), row.names = FALSE)

  } else {
    
    draws <- copy(zero_draw_file)
    draws[, location_id := location_id]
    draws[, model_id:= limit_meid]
    draws[, metric_id := 3]
    
    write.csv(draws, paste0(draws_dir, '/', limit_meid, "/", location_id, ".csv"), row.names = FALSE)
    
  }
  
}
