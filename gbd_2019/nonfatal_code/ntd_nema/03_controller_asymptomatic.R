## Empty the environment
rm(list = ls())

## Set up focal drives
os <- .Platform$OS.type
if (os=="windows") {
  ADDRESS <- FILEPATH
  ADDRESS <- FILEPATH
} else {
  ADDRESS <- FILEPATH
  ADDRESS <- paste0(FILEPATH, Sys.info()[7], "/")
}

## Load functions and packages
library(data.table)
library(argparse, lib.loc = paste0(FILEPATH)) 
library(stringr)
source(paste0(FILEPATH))
source(paste0(FILEPATH))
source(paste0(FILEPATH))
source(paste0(FILEPATH))


## Set-up run directory
run_file <- fread(paste0(FILEPATH))
run_dir <- run_file[nrow(run_file), run_folder_path]

draws_dir    <- paste0(run_dir, FILEPATH)
interms_dir  <- paste0(run_dir, FILEPATH)
logs_dir     <- paste0(run_dir, FILEPATH)
params_dir <- paste0(FILEPATH)
code_dir <- paste0(ADDRESS, FILEPATH)

source(paste0(code_dir, FILEPATH))

#parrallelize helpers 
source(FILEPATH)
my_shell <- paste0(FILEPATH)

gbd_round_id <- 6
#############################################################################################
###                                     Main Script                                       ###
#############################################################################################
study_dems <- get_demographics(gbd_team = 'epi', gbd_round_id = gbd_round_id)
## HELPER VECTORS
worm_list <- list(list("ascariasis", ADDRESS), list("hookworm", ADDRESS), list("trichuriasis", ADDRESS))

worm_element <- worm_list[[2]]

my_worm      <- worm_element[[1]]

cat(paste0("Estimating asymp for ", my_worm))

meid_dir             <- paste0(params_dir, FILEPATH)
meids                <- fread(meid_dir)

meids   <- meids[worm == my_worm]
asymp   <- meids[model == "asymptomatic"]$meid
if (my_worm == "hookworm") {asymp <- ADDRESS}
# # 
# # # Submit jobs to compute asymptomatic cases
ifelse(!dir.exists(paste0(draws_dir, my_worm, "/")), dir.create(paste0(draws_dir, my_worm, "/")), FALSE)
ifelse(!dir.exists(paste0(draws_dir, my_worm, "/", asymp, "/")), dir.create(paste0(draws_dir, my_worm,  "/", asymp, "/")), FALSE)

my_locs <- study_dems$location_id

param_map_asymp <- expand.grid(location_ids = my_locs, my_worm = my_worm,
                               draws_directory = paste0(draws_dir, my_worm, "/", asymp ,"/"),
                               meids_directory = meid_dir, params_directory = params_dir)
# 
num_jobs  <- nrow(param_map_asymp)
fwrite(param_map_asymp, paste0(interms_dir, my_worm, FILEPATH), row.names = F)
# 
 qsub(job_name = paste0("Asymp_Sequelae_Splits_", my_worm),
      shell    = my_shell,
      code     = paste0(code_dir, FILEPATH),
      args     = list("--param_path_asymp", paste0(interms_dir, my_worm, FILEPATH)),
      project  = ADDRESS,
      m_mem_free = "5G",
      fthread = "1",
      archive = NULL,
      h_rt = "00:00:20:00",
      queue = ADDRESS,
      num_jobs = num_jobs)


## Wait for jobs to complete 
i                <- 0
time_to_resubmit <- 0
resubmits        <- 0

for (loc in my_locs) {

  while (!file.exists(paste0(draws_dir, my_worm, "/", asymp, "/", loc, ".csv"))) {
    cat(paste0("Waiting for file: ", draws_dir, my_worm, "/", asymp, "/", loc, ".csv -- ", Sys.time(), "\n"))
    cat(paste0(i, " of 990 completed and accounted for."))
    Sys.sleep(30)
    time_to_resubmit <- time_to_resubmit + 30
    cat(paste0("\n resubmit timer at: ", time_to_resubmit, ". Resubmitting array jobs of missing locations in ", 2000 - time_to_resubmit, " (takes longer to run so more time before resubmit -- needs more resubmits due to complexity also)\n"))

    if (time_to_resubmit >= 2000){
      time_to_resubmit <- 0
      cat("Resubmitting Asymp Array Job Now.")
      root <- FILEPATH
      resubmit_asymp(my_worm = my_worm, model_ver = model_ver, all_locs = all_locs)
      cat("Resubmitted Asymp Array Job. Sleeping for 5 minutes")
      Sys.sleep(300)
      time_to_resubmit <- 0
      resubmits        <- resubmits + 1
    }

    if (resubmits > 3){
      cat("Too many asymp resubmits something is wrong.")
      break
    }

  }
  if (file.exists(paste0(draws_dir, my_worm,  "/", asymp, "/", loc, ".csv"))){
    i <- i + 1
  }}

cat(paste0("\n All asymp draws for ", my_worm, " completed. \n" ))

## Upload
my_description <- paste0("GBD 2019 Resubmission Estimates Asymptomatic cases are the difference between the parent model and the sum",
                          "of heavy and mild cases.")
#
save_results_epi(input_dir = paste0(draws_dir, my_worm,  "/", asymp, "/"), input_file_pattern = "{location_id}.csv",
                 modelable_entity_id = asymp, measure_id = 5, metric_id = 3, description = my_description, mark_best = TRUE,
                 decomp_step = "step4")

#############################################################################################
###                            Hookworm exclusivity adjusment                             ###
#############################################################################################

#############################################################################################
###                            Post-Anemia, needs to be an array job                      ###
#############################################################################################

## Submit jobs to compute asymptomatic cases
if (my_worm == "hookworm"){

 asymp <- ADDRESS
 ifelse(!dir.exists(paste0(draws_dir, asymp, "/")), dir.create(paste0(draws_dir, asymp, "/")), FALSE)
 ifelse(!dir.exists(paste0(draws_dir, asymp, FILEPATH)), dir.create(paste0(draws_dir, asymp, FILEPATH)), FALSE)

 my_locs <- study_dems$location_id
  all_locs <- copy(my_locs)
  
     no_file <- c()
  
   for (loc in all_locs) {
     if (!file.exists(paste0(draws_dir, FILEPATH, loc, ".csv"))) {
       no_file <- c(no_file, loc)
      }}
my_locs <- no_file

restrict_loc <- fread(FILEPATH)
restrict_loc <- unique(restrict_loc[value_endemicity == 0, location_id])
# zero poland and italy
locs <- get_location_metadata(35, gbd_round_id = gbd_round_id)
restrict <- locs[parent_id %in% c(86, 51), location_id]
restrict_loc <- c(restrict_loc, restrict)
my_locs <- unique(restrict_loc)
 
param_map <- expand.grid(location_id = my_locs)
 
num_jobs  <- nrow(param_map)
fwrite(param_map, paste0(interms_dir, FILEPATH), row.names = F)
 
qsub(job_name = "hookworm_asymp",
     shell    = my_shell,
     code     = paste0(code_dir, FILEPATH),
     args     = list("--param_path", paste0(interms_dir, FILEPATH)),
     project  = ADDRESS,
     m_mem_free = "5G",
     fthread = "1",
     archive = NULL,
     h_rt = "00:00:20:00",
     queue = ADDRESS,
     num_jobs = num_jobs)
 
for (loc in my_locs){
  qsub_seq(job_name = paste0("hookworm_exclusivity_adj_location.id_", loc),
           shell   = my_shell,
           code    = p,
           project = ADDRESS,
           args  = list("--my_loc",  loc,
                        "--draws_directory", paste0(draws_dir, asymp, "/"),
                        "--restrict_loc", restrict_loc),
           m_mem_free = "10G",
           fthread = "5",
           archive = NULL,
           h_rt = "00:00:20:00",
           queue = ADDRESS)
    }
  }

 ## Wait for jobs to complete
  for (loc in my_locs) {
    while (!file.exists(paste0(draws_dir, my_worm,  "/", asymp, "/"))) {
      cat(paste0("Waiting for file: ", draws_dir, asymp, "/", loc, ".csv -- ", Sys.time(), "\n"))
      Sys.sleep(60)
    }
  }

# Upload
   my_description <- paste0("GBD 2019 Decomp 4; To derive the adjustment, we subtract all the hookworm-anemia models from the asymptomatic hookwrom model ",
                             "with anemia.")
   #
   asymp <- ADDRESS
   save_results_epi(input_dir = paste0(draws_dir, asymp, "/"), input_file_pattern = "{location_id}.csv",
                    modelable_entity_id = asymp, measure_id = 5, year_id = c(1990, 1995, 2000, 2005, 2010, 2015, 2017, 2019),
                    metric_id = 3,
                    description = my_description, mark_best = TRUE,
                    decomp_step = "step4")