# apply gr child script
rm(list = ls())

library(data.table)
library(argparse, lib.loc= "FILEPATH")
library(stringr)
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")

## Parse Arguments
print(commandArgs())
parser <- ArgumentParser()
parser$add_argument("--param_path", help = "param path", default = "ADDRESS", type = "character")

args <- parser$parse_args()
print(args)
list2env(args, environment()); rm(args)
sessionInfo()

task_id <- as.integer(Sys.getenv("SGE_TASK_ID"))
params  <- fread(param_path)

my_loc <- params[task_id, location_id]
end <- params[task_id, value_endemicity]
mv_id <- params[task_id, mv_id]

run_file <- fread(paste0("FILEPATH"))
run_dir <- run_file[nrow(run_file), run_folder_path]
crosswalks_dir    <- paste0(run_dir, "FILEPATH")
draws_dir    <- paste0(run_dir, "FILEPATH")
interms_dir    <- paste0(run_dir, "FILEPATH")

subnats <- fread("FILEPATH")
locs <- get_location_metadata(35)

if (end == 0){
  draws <- fread("FILEPATH")
  draws[, location_id := my_loc]
  draws[, modelable_entity_id := ADDRESS]
  write.csv(draws, paste0(draws_dir, ADDRESS, "/", my_loc, ".csv"), row.names = FALSE)
}

if (my_loc %in% subnats[,location_id] & end == 1){
  parent_id <- locs[location_id == my_loc, parent_id]
  
  draws <- get_draws(gbd_id_type = "modelable_entity_id",
                     gbd_id = ADDRESS,
                     gbd_round_id = ADDRESS,
                     source = "epi",
                     measure_id = c(5,6),
                     year_id = c(1990, 1995, 2000, 2005, 2010, 2015, 2017, 2019),
                     location_id = parent_id,
                     version_id = mv_id,
                     decomp_step = "iterative",
                     sex_id = c(1,2)
  )
  
  subnat_frac <- subnats[location_id == my_loc, draw_0]
  draws[, paste0("draw_", 0:999) := lapply(0:999, function(x) get(paste0("draw_", x)) * subnat_frac)]
  draws[, location_id := my_loc]
  draws[, modelable_entity_id := ADDRESS]
  write.csv(draws, paste0(draws_dir, ADDRESS, "/", my_loc, ".csv"), row.names = FALSE)
}

if (end == 1 & !(my_loc %in% subnats[,location_id])){

  draws <- get_draws(gbd_id_type = "modelable_entity_id",
                     gbd_id = ADDRESS,
                     gbd_round_id = ADDRESS,
                     source = "epi",
                     measure_id = c(5,6),
                     year_id = c(1990, 1995, 2000, 2005, 2010, 2015, 2017, 2019),
                     location_id = my_loc,
                     version_id = mv_id,
                     decomp_step = "iterative",
                     sex_id = c(1,2)
  )
  draws[, modelable_entity_id := ADDRESS]
  write.csv(draws, paste0(draws_dir, ADDRESS, "/", my_loc, ".csv"), row.names = FALSE)
}