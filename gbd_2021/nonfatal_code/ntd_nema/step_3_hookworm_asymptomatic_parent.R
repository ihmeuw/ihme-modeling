# Purpose: calculate hookworm asymptomatic meid 3111
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

## Load functions and packages
library(data.table)
source(paste0("FILEPATH/save_results_epi.R"))
source(paste0("FILEPATH/get_demographics.R"))
source(paste0("FILEPATH/get_model_results.R"))
source(paste0("FILEPATH/get_location_metadata.R"))

## Set-up run directory
run_file <- fread(paste0(data_root, "FILEPATH"))
run_dir <- run_file[nrow(run_file), run_folder_path]

draws_dir    <- paste0(run_dir, "FILEPATH")
interms_dir  <- paste0(run_dir, "FILEPATH")
logs_dir     <- paste0(run_dir, "FILEPATH")

#parrallelize helpers 
source("FILEPATH")
my_shell <- paste0("FILEPATH")

gbd_round_id <- ADDRESS
decomp_step <- ADDRESS

#############################################################################################
###                                     Main Script                                       ###
#############################################################################################

study_dems <- readRDS(paste0(data_root, "FILEPATH"))
meid_dir             <- paste0(params_dir, "FILEPATH")
meids                <- fread(meid_dir)


restrict_loc <- fread('FILEPATH')
restrict_loc <- unique(restrict_loc[value_endemicity == 0, location_id])
# zero poland and italy
locs <- get_location_metadata(ADDRESS, gbd_round_id = gbd_round_id)
restrict <- locs[parent_id %in% c(86, 51), location_id]
restrict_loc <- c(restrict_loc, restrict)
my_locs <- unique(restrict_loc)
 
 param_map <- expand.grid(location_id = my_locs)
 
 num_jobs  <- nrow(param_map)
 fwrite(param_map, paste0(interms_dir, "FILEPATH"), row.names = F)
 
 LAUNCH(job_name = "hookworm_asymp",
      shell    = my_shell,
      code     = paste0(code_dir, "FILEPATH"),
      args     = list("--param_path", paste0(interms_dir, "FILEPATH")),
      project  = ADDRESS,
      m_mem_free = "5G",
      fthread = "1",
      archive = NULL,
      h_rt = "00:00:20:00",
      queue = "long.q",
      num_jobs = num_jobs)
 

 ## Wait for jobs to complete
  for (loc in my_locs) {
    while (!file.exists(paste0(draws_dir, my_worm,  "FILEPATH"))) {
      cat(paste0("Waiting for file: ", draws_dir, asymp, "/", loc, ".csv -- ", Sys.time(), "\n"))
      Sys.sleep(60)
    }
  }

# Upload
   my_description <- paste0(ADDRESS)
   #
   asymp <- 3139
   ADDRESS(input_dir = paste0(draws_dir, asymp, "/"), input_file_pattern = "{location_id}.csv",
                    model_id= asymp, measure_id = 5, year_id = anemia_years,
                    metric_id = 3,
                    description = my_description, mark_best = TRUE,
                    decomp_step = ADDRESS)


