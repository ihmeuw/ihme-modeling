#--------------------------------------------------------------
# Name: Alton Lu
# Date: April 2020
# Project: IKF exposure interpolation
# Purpose: Launch and save IKF expsoure estimates
#--------------------------------------------------------------
rm(list = ls())

code_dir <- "/ihme/code/qwr/ckd_qwr/"
ckd_repo <- "/ihme/code/qwr/ckd_qwr/kd/code/"
shell_script <- '/share/singularity-images/health_fin/forecasting/shells/health_fin_forecasting_shell_singularity.sh'
errors <- "/ihme/temp/sgeoutput/qwr/annual_exposures/errors/" # whatever you want it to be
outputs <- "/ihme/temp/sgeoutput/qwr/annual_exposures/output/"

# package requirements -------------------
library(tidyr)
source(paste0(code_dir,"general_func_lib.R"))
source(paste0(code_dir,"squeeze_split_save_functions.R"))
source_shared_functions("get_demographics")

# Directories:
path_to_map <- paste0(ckd_repo, "annual_exposure_mapping.csv")
print(path_to_map)
step <- 'iterative'
round <- 7

user <- Sys.info()["user"]
project <- "proj_yld"


q <- "long.q"
slots <- 1
mem_free <- '8'
threads <- 1
runtime <- "0:20:00"


### 01: Run Interpolation ---------------------------------------------------------------------------

# Get locations to parallelize
locations <- get_demographics(gbd_team = "epi", gbd_round_id = round)$location_id
#locations <- locations[1:10] # subset for testing purposes ###

script_01 <- paste0(ckd_repo, "01_interpolate_exposure.R")

# Important to check path_to_map and code_dir are correct in error log
pass <- c(path_to_map, code_dir, step, round) 

# This launches a job for each location
job.array.master(tester = F, 
                 paramlist = locations, # pass all locations
                 username = user, 
                 project = project,
                 threads = threads,
                 mem_free = mem_free,
                 runtime = runtime,
                 q = q,
                 jobname = "kd_interpolation",
                 output = outputs, 
                 errors = errors,
                 childscript = script_01,
                 shell = shell_script,
                 args = pass)

### Validations ---------------------------------------------------------------------
mapping <- read.csv(path_to_map)

# This will check each folder for missing locations
for (i in unique(mapping$target_me_id)){
  print(paste("Checking MEID: ", i))
  draws_dir <-   dir <- paste0(mapping$directory_to_save[1], i, "/")
  print(draws_dir)
  check_missing_locs(indir = draws_dir, filepattern = "{location_id}.csv", team = "epi",
                     round = 7, step = 'iterative')
}

### Check to see if all estimation years and sex are in location 33 ---------------------
# for (i in unique(mapping$target_me_id)) {
#   dir <- paste0(mapping$directory_to_save[1], i, "/")
#   file_name <- paste0(draws_dir, "33.csv")
#   check_years <- read.csv(file_name)
#   print("Years by sex ---------------------")
#   print(table(check_years$year_id, check_years$sex_id))
#   
# }

### 02: Save Results --------------------------------------------

# suggest putting thread/memory/runtime very low to test
# for full save:
#  threads = 30
#  memory = 60
#  runtime = "24:00:00"

save_results_function(path_to_errors = errors, 
                      path_to_settings = path_to_map,
                      description = "annual_exposure_interpolated_1990:2022", 
                      threads = 30, memory = 60, runtime = "24:00:00", q = "long.q",
                      project = project, 
                      best = T,
                      code_directory = "/ihme/code/qwr/ckd_qwr/squeeze_split/functions/",
                      list_of_job_names = "no_job_holds",
                      shell = shell_script, 
                      cod_or_epi = "epi", 
                      file_extension = ".csv",
                      year = c(1990:2022)) # had to go into the 02 save results and change years from estimation 
# years to the 1990:2022 there, that is not sustainable so figure out what's going on 



