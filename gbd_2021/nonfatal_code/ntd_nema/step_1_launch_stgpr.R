# Purpose: launch ST-GPR from crosswalk version dataset
#
### ======================= BOILERPLATE ======================= ###
rm(list = ls())

## Load functions and packages
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

## ST-GPR functions / packages
central_root <- 'FILEPATH'
setwd(central_root)

library(data.table)
source('FILEPATH')
source('FILEPATH')
source(paste0(code_root, 'FILEPATH'))

### Run dir
gen_rundir(data_root = data_root, acause = 'ntd_nema', message = ADDRESS)
run_file <- fread(paste0(data_root, "FILEPATH"))
run_dir <- run_file[nrow(run_file), run_folder_path]
interms_dir <- paste0(run_dir, "FILEPATH")

model_index_id <- ADDRESS
run_id_table <- data.table('run_id' = integer(), 'model_index_id' = integer(), 'worm' = as.character())

for (worm in c('trichuriasis' , 'hookworm', 'ascariasis')){
  run_id <- register_stgpr_model(paste0(data_root, "FILEPATH", worm , "FILEPATH"), model_index_id = model_index_id)
  stgpr_sendoff(run_id, 'proj_ntds')
  row <- data.table('run_id' = run_id, 'model_index_id' = model_index_id, 'worm' = worm)
  run_id_table <- rbind(run_id_table, row)
}

fwrite(run_id_table, paste0(interms_dir, 'FILEPATH'))