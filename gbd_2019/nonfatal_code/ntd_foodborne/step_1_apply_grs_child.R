#' [Title: FBT Apply GRs Child
#' [Notes: Writes 0's for non-endemic locations, pass through estimate for endemic locations

#############################################################################################
###'                                [General Set-Up]                                      ###
#############################################################################################

rm(list = ls())

# root path
code_root <- "ADDRESS"
data_root <- "ADDRESS"

# Toggle (Prod Arg Parsing VS. Interactive Dev) Common IHME IO Paths
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
  params_dir <- paste0(data_root, "/FILEPATH")
  draws_dir <- paste0(data_root, "/FILEPATH")
  interms_dir <- paste0(data_root, "/FILEPATH")
  logs_dir <- paste0(data_root, "/FILEPATH")
  location_id <- 68
}

# packages
library(data.table)
library(argparse, lib.loc= "/FILEPATH")
library(stringr)
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
source(paste0(code_root, 'FILEPATH'))

gbd_round_id <- 7
decomp_step <- 'step2'

#############################################################################################
###'                              [Estimate meIDs]                                        ###
#############################################################################################

study_dems <- readRDS(paste0(data_root, '/FILEPATH'))    # demographics
years <- study_dems$year_id
est_locs       <- fread(paste0(params_dir, "/FILEPATH"))
zero_draw_file <- gen_zero_draws(modelable_entity_id = NA,  location_id = NA, 
                                 measure_id = 5,  year_id = years, 
                                 gbd_round_id = gbd_round_id,  team = 'epi')

meids <- list(list(ADDRESS1, ADDRESS1NEW), 
              list(ADDRESS2, ADDRESS2NEW), 
              list(ADDRESS3, ADDRESS3NEW), 
              list(ADDRESS4, ADDRESS4NEW), 
              list(ADDRESS5, ADDRESS5NEW))


for (element in meids){
    parent_meid <- element[[1]]
    limit_meid  <- element[[2]]
    est_locs_meid <- est_locs[meid == parent_meid, location_id]

    if (location_id %in% est_locs_meid){
        draws <- get_draws(gbd_id_type = "modelable_entity_id",
                           gbd_id = parent_meid,
                           source = "epi",
                           measure_id = c(5),
                           year_id = years,
                           location_id = location_id,
                           status = "best",
                           sex_id = c(1,2),
                           decomp_step = decomp_step,
                           gbd_round_id = gbd_round_id)

    location_id[age_group_id <= 5 | age_group_id %in% c(388,389,238), paste0("draw_", 0:999) := 0]
    draws[, modelable_entity_id := limit_meid]
    draws[, metric_id := 3]
    write.csv(draws, "FILEPATH", row.names = FALSE)
    
    } else {
    
    draws <- copy(zero_draw_file)
    draws[, location_id := location_id]
    draws[, modelable_entity_id := limit_meid]
    draws[, metric_id := 3]
    
    write.csv(draws, "FILEPATH", row.names = FALSE)
    }
}
