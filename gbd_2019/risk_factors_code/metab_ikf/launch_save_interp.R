#--------------------------------------------------------------
# Name: USERNAME
# Date: 2018-07-18
# Project: IKF exposure interpolation
# Purpose: Launch and save IKF expsoure estimates
# 
# Updated: 2019-08-05
# Name: USERNAME
# Purpose: Improving comments and documentation
#--------------------------------------------------------------


# Quick Summary:

# To run this code:
# 1. Ensure the following directories are correct
#   a. Directory to the xlsx file of ME IDs
#   b. Directory to where the following code are
#     b1. 01_interpolate_exposure.R
#     b2. save_interp.R
#   c. Directory to where you want the output draws to go
# 2. Get an R singularity imgae


# setup -------------------------------------------------------

# package requirements -------------------
require(data.table)
require(openxlsx)
library(tidyverse)


# specify the path to your map of input to output mes. should have the following
# columns/corresonding values. must be an .xlsx file 
# input_me: me id of expsoure DisMod model
# output_me: me id for annualized expsoure after interpolation
# year_start: the min year you want your interpolated output dataset to contain
# year_end: the max year you want your interpolated output dataset to contain
# annual: T/F, do you want to save annualized results or only estimation years 
#   (must be specified in the excel file as a logical, i.e. =TRUE)
# measure_ids: measure_ids you want to save
# file_pattern: string describing how files are organized – 
#   ie ‘{measure_id}/{location_id}.csv’ 
# sex_ids: comma separated string of sex_ids for which you need results 
# best: T/F, should this model be marked best when save, must be logical
# descript: description for annualized model upload
# gbd_rnd: gbd round id
# interpolated_exp_dir: directory where interpolated draws should be saved

# Parameters to change:

setwd("FILEPATH")

# what step is it? step1, step2, step3, step4, step5
step <- 'step4'
round <- "6"

# Setting the repository for ckd and the file for
ckd_repo <- "FILEPATH"
map_path <- paste0(ckd_repo,"FILEPATH/me_map_lualton.xlsx")

# path to utility functions library 
func_lib <- paste0(ckd_repo,"function_lib.R")
source(func_lib)

# Get shared function called get_demographics
source_shared_functions("get_demographics")

# Access the ME ID's
map_id <- as.data.table(read.xlsx(map_path))
draws_dir <- paste0(map_id$interpolated_exp_dir[1], step)

# Assign locations for all locations that we calculate in dismod. Stores as array of location ID's ~990
locations <- get_demographics("epi", gbd_round_id = round)$location_id

slots <- 1
mem <- '20G'
threads<-4
runtime <- "5:00:00"
q <- "all.q"
project <- "proj_yld"

shell_script <- 'FILEPATH'

script_to_run <- paste0(ckd_repo, "FILEPATH/01_interpolate_exposure.R")
output_version <- 'FILEPATH'

output <- "FILEPATH"
errors <- "FILEPATH"

### Run Interpolation ---------------------------------------------------------------------------
# Change the inner for loop to limit_locations to test on a single location
limit_locations <- locations[1]

for (i in unique(map_id$input_me)){
  
  print(paste("Starting MEID", i))
  
  for (loc in locations){ # change this line to limit_locations for testing
    # job_name <- paste0("interpolate_exp_", i, "_", loc)
    name <- paste0("loc_", loc, "_interp")
    
    arguments = paste(i, loc, map_path, func_lib, output_version, step, round)
    qsub_arg <- paste0("qsub -cwd -N ", name,  " -q ", q, " -o ", output, " -e ", errors,
                      " -l archive=TRUE -l fthread=", threads, " -l m_mem_free=", mem,  " -l h_rt=", runtime, " -P ", project)
    additional_arg <- paste(shell_script, script_to_run, arguments)
    
    system(paste(qsub_arg, additional_arg))
    print(paste(qsub_arg, additional_arg))
  }
}

### Check for Missing Locations ---------------------------------------------------------------------
# This will check each folder for missing locations
for (i in unique(map_id$input_me)){
  print(paste("Checking MEID: ", i))
  draws_dir <- paste0("FILEPATH", output_version, i)
  print(draws_dir)
  check_missing_locs(indir = draws_dir, filepattern = "{location_id}.csv", team = "epi")
}


check <- check_missing_locs("FILEPATH", 
                            filepattern = "{location_id}.csv", team = "epi",
                            round = 6, step = 'step4')



### Check to see if all estimation years are in the csv ---------------------

for (i in unique(map_id$input_me)) {
  draws_dir <- paste0("FILEPATH", output_version, i, "/")
  file_name <- paste0(draws_dir, "33.csv")
  check_years <- read.csv(file_name)
  print("Years by sex ---------------------")
  print(table(check_years$year_id, check_years$sex_id))
  
}
# Save results using script --------------------------------------------
save_script <- "FILEPATH/test_save_epi.R"

year_start <- 1990
year_end <- 2019
description <- "Resubmission_all_years"

for (i in 1:nrow(map_id)){
  print(paste("Running for MEID: ", map_id$input_me[i], "to", map_id$output_me[i]))
  system(paste("qsub -cwd -N ikf_save -q long.q" , "-o", output, "-e", errors,
                "-l fthread=10 -l m_mem_free=60G -l archive=TRUE -l h_rt=24:00:00 -P proj_yld",
               shell_script, save_script, map_id$input_me[i], map_id$output_me[i], step, output_version, year_start, year_end, description))
  print(paste("qsub -cwd -N ikf_save -q long.q" , "-o", output, "-e", errors,
              "-l fthread=10 -l m_mem_free=60G -l archive=TRUE -l h_rt=24:00:00 -P proj_yld",
              shell_script, save_script, map_id$input_me[i], map_id$output_me[i], step, output_version, year_start, year_end, description))
}


### Try Job Array, if doesn't work, run normally--------------------------------------------------------

## GET PARAMS
message("Starting job launch")
script_to_run <- paste0(ckd_repo, "FILEPATH/01_interpolate_exposure.R")
all_params <- get_demographics(gbd_team = "epi")$location_id
all_params <- all_params[1:20]

user <- "lualton"
project <- "proj_yld"
mem_free <- '20'
pass <- c(map_path, func_lib, output_version, step, round)
params <- as.list(params)
shell <- 'FILEPATH'

errors <- "FILEPATH"

job.array.master(tester = F, paramlist = all_params, username = user, project = project,
                 threads = threads,
                 mem_free = mem_free,
                 runtime = runtime,
                 q = q,
                 jobname = "ikf_interp",
                 errors = errors,
                 childscript = script_to_run,
                 shell = shell,
                 args = pass)



### 03:Interpolate PAFS to get all estimation years ----------------------------------------------------

# This doesn't need to be run unless you need all estimation years. 
# Typically at the end of a GBD round or for EPI Transition Analysis

limit_locations <- locations[1]
script3 <- paste0(ckd_repo, "/03_interpolate_pafs.R")

measure_ids <- c(3,4)

for (measure in measure_ids) {
  
  for (loc in locations){ # change this line to limit_locations for testing
    # job_name <- paste0("interpolate_exp_", i, "_", loc)
    name <- paste0("paf_", measure, "_loc_", loc, "_interp")
    
    arguments = paste(loc, map_path, func_lib, output_version, step, round, measure)
    qsub_arg <- paste0("qsub -cwd -N ", name,  " -q ", q, " -o ", output, " -e ", errors,
                       " -l archive=TRUE -l fthread=", threads, " -l m_mem_free=", mem,  " -l h_rt=", runtime, " -P ", project)
    additional_arg <- paste(shell_script, script3, arguments)
    
    system(paste(qsub_arg, additional_arg))
    print(paste(qsub_arg, additional_arg))
  }
  
}

### This will check each folder for missing locations in paf interp -------------------------------
for (i in unique(map_id$input_me)){
  print(paste("Checking MEID: ", i))
  draws_dir <- paste0("FILEPATH", output_version, i)
  print(draws_dir)
  check_missing_locs(indir = draws_dir, filepattern = "{location_id}.csv", team = "epi")
}


check <- check_missing_locs("FILEPATH", 
                            filepattern = "{location_id}_4.csv", team = "epi",
                            round = 6, step = 'step4')
### 04: Save PAFS ------------------------------------------------------------------------
# Takes a lot of memory. go through cluster R and request login with 100g

source("FILEPATH/save_results_risk.R")
save_results_risk(modelable_entity_id=10943,risk_type = 'paf', 
                  description="Interpolated annual PAFs, GBD 2019", 
                  mark_best=TRUE, input_file_pattern="{measure_id}_{location_id}.csv",year_id=c(1990:2019),
                  input_dir=("FILEPATH"), decomp_step = 'step4')



