# Aggregates vl data from bundle to national level 
# Applies underreporting model and writes out for stgpr
# Launch stgpr model, write out run id to interms
# Best specs from final gbd 2019 

### ======================= BOILERPLATE ======================= ###
rm(list = ls())
code_root <-paste0("FILEPATH", Sys.info()[7])
data_root <- "FILEPATH"

# Toggle (Prod Arg Parsing VS. Interactive Dev) Common IO Paths
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

## ST-GPR functions
central_root <- 'FILEPATH'
setwd(central_root)
source('FILEPATH')
source('FILEPATH')

source(paste0(code_root, 'FILEPATH'))
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
source("FILEPATH")
library(data.table)
library(stringr)

# set-up run directory

run_file <- fread(paste0(data_root, "FILEPATH"))
run_dir <- run_file[nrow(run_file), run_folder_path]
crosswalks_dir    <- paste0(run_dir, "FILEPATH")
draws_dir <- paste0(run_dir, "FILEPATH")
interms_dir <- paste0(run_dir, "FILEPATH")

# parrallelize helpers 

source("FILEPATH")
my_shell <- "FILEPATH"

gbd_round_id <- ADDRESS
decomp_step <- 'step2'
model_index_id <- 1
### ======================= Main Execution ======================= ###

# register and launch stgpr model from config file in params
run_id <- register_stgpr_model(paste0(data_root, 'FILEPATH'), model_index_id = model_index_id)
stgpr_sendoff(run_id, 'ADDRESS')

# write out run_id and model index id
stgpr_run_id_table <- data.table(run_id = run_id)
fwrite(stgpr_run_id_table, paste0(interms_dir, 'FILEPATH'))

model_index_id_table <- data.table(model_index_id = model_index_id)
fwrite(model_index_id_table, paste0(interms_dir, 'FILEPATH'))
