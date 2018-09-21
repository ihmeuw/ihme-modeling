##########################################################################
## Author: USERNAME
## Date: DATE
## Purpose: MASTER script for calculating incidence based on events and yearly point prevalence data
##########################################################################

# clear workspace environment
rm(list = ls())

if(Sys.info()[1] == 'Windows') {
  username <- "USERNAME"
  root <- "J:/"
  code_dir <- paste0("FILEPATH")
} else {
  username <- "USERNAME"
  root <- "FILEPATH"
  code_dir <- paste0("FILEPATH", username, "FILEPATH")
}

pacman::p_load(data.table, magrittr)

run_date <- "DATE"

# source the central functions
source("FILEPATH.R")
source("FILEPATH.R")
source("FILEPATH.R")

#############################################################
#############################################################
# SUBMIT THE SUMMARIZING SCRIPT
#############################################################
#############################################################

testing <- FALSE
local <- FALSE

summarize <- FALSE
apply <- TRUE

if(summarize == TRUE){
  
  #############################################################
  # IMPORTANT: SETTINGS OF THIS RUN, LINE UP WITH CHILD SCRIPT
  #############################################################
  
  # define settings for this run
  
  # get shared functions
  source(paste0(root, "FILEPATH.R"))
  source(paste0(root, "FILEPATH.R"))
  
  # get demographic information to pull from
  dems <- get_demographics(gbd_team = "epi")
  location_ids <- dems$location_ids
  sex_ids <- dems$sex_ids
  
  # get location information
  metaloc <- get_location_metadata(location_set_id = 2)
  supers <- metaloc[!is.na(super_region_id), super_region_id] %>% unique %>% as.numeric
  
  out_dir <- paste0("FILEPATH", run_date, "FILEPATH")
  
  # create directories
  directories <- c(paste0(out_dir, "/FILEPATH"), paste0(out_dir, "/FILEPATH"))
  lapply(directories, dir.create, recursive = TRUE)
  
  if(testing){
    
    # running two tasks, one for each sex_id
    n = 1
    
  } else {
    
    n = length(supers)
    
  }
  
  #############################################################
  # SUBMIT THE JOBS IN A JOB ARRAY
  #############################################################
  
  array <- paste0("1:", n)
  jname <- "super_ratios"
  slots <- 24
  sys.sub <- paste0("qsub -cwd -N ", jname, " -P proj_injuries -t ", array, " -pe multi_slot ", slots)
  shell <- "FILEPATH.sh"
  script <- "FILEPATH.R"
  print(paste(sys.sub, shell, script))
  
  # QSUB COMMAND
  system(paste(sys.sub, shell, script))
}

#############################################################
#############################################################
# SUBMIT THE APPLICATION SCRIPT
#############################################################
#############################################################

if(apply == TRUE){
  
  #############################################################
  # IMPORTANT: SETTINGS OF THIS RUN, LINE UP WITH CHILD SCRIPT
  #############################################################
  
  # get shared functions
  source(paste0(root, "FILEPATH.R"))
  
  # get demographic information to pull from
  dems <- get_demographics(gbd_team = "epi")
  location_ids <- dems$location_ids
  sex_ids <- dems$sex_ids
  age_group_ids <- dems$age_group_ids
  year_ids <- dems$year_ids
  
  out_dir <- paste0("FILEPATH")
  
  # create directories
  directories <- c(paste0(FILEPATH), paste0(out_dir, "/FILEPATH"))
  lapply(directories, dir.create, recursive = TRUE)
  
  if(testing){
    
    # running two tasks, one for each sex_id
    n = length(sex_ids)
    
  } else {
    
    # running 1386 jobs
    n = length(sex_ids)*length(location_ids)
    
  }
  
  #############################################################
  # SUBMIT THE JOBS IN A JOB ARRAY
  #############################################################
  
  array <- paste0("1:", n)
  jname <- "apply_ratios"
  slots <- 2
  sys.sub <- paste0("qsub -cwd -N ", jname, " -P proj_injuries_2 -t ", array, " -pe multi_slot ", slots)
  shell <- "FILEPATH.sh"
  script <- "FILEPATH.R"
  print(paste(sys.sub, shell, script))
  
  # QSUB COMMAND
  system(paste(sys.sub, shell, script))
}





