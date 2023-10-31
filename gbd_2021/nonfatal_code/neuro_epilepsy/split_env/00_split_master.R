# ------------------------------------------------------------------------------
# Author: Emma Nichols, updated by Paul Briant
# Purpose: Master script to split and save Epilepsy Splits.
# User instructions:
# 1. Update arguments in the "SET GENERAL FILE PATHS" and "SET QSUB ARGUMENTS" 
# sections.
# 2. Select and run the entire script. 
# ------------------------------------------------------------------------------

rm(list = ls())
user <- Sys.info()[['user']]
date <- gsub("-", "_", Sys.Date())

# ---LOAD LIBRARIES-------------------------------------------------------------

library(chron)
library(data.table)
library(magrittr)
library(mortcore, lib = "/ihme/mortality/shared/r/")

# ---SET GENERAL FILE PATHS-----------------------------------------------------

shared_repo <- paste0("/share/code/", 
                      user, 
                      "/ylds-shared-code/database_utilities/")
share_path <- paste0(
  "/share/code/", user, "/epilepsy/modeling/envelope_splits/")
central <- "/ihme/cc_resources/libraries/current/r/"
# Use a shell script from a maintained singularity image (eventually switch to 
# the image that is updated by the data science and engineering team)

shell_path <- '/ihme/mortality/shared/r_shell_singularity_3501.sh'

# ---SET QSUB ARGUMENTS---------------------------------------------------------

# General qsub arguments 
proj <- "proj_yld"
q <- "all.q"
output_dir <- "/share/scratch/projects/yld_gbd/epilepsy/"
# NOTE: this is just for error files from the save results jobs. Errors from 
# The array jobs will go to "/share/temp/sgeoutput/{user}/errors/". This is
# because the error path for mortcore's array qsub job does not appear to be
# adjustable.
errors <- paste0("/share/temp/sgeoutput/", user, "/errors/epilepsy/")
map_path <- paste0(output_dir, "task_map.csv")
envelope_id <- 2403
primary <- 3025
secondary <- 3026
severe <- 1953
notsevere <- 1952
tnf <- 1951
gbd_round_id <- 7
decomp_step <- "iterative"
location_set_id <- 35
file_pattern <- "{location_id}.csv"
# True: Best the model in epi viz. False: Do not best. NOTE: Custom models 
# currently cannot be manually bested in epi viz meaning that this argument 
# must be set to "T" to best the splits.
mark_best <- T
description <- "Model 550550, Bested for central comp test run of COMO"
# Arguments to pass to the impairment envelope splitting array job after 
# the general qsub arguments. 
split_args <- list(envelope_id, primary, secondary, severe, notsevere, 
                tnf, gbd_round_id, decomp_step, output_dir, central, 
                share_path, map_path)

# Arguments for control flow and testing
# True: test on a single location/task. False: run in production
test_splits <- F
# True: qsub save results jobs for all splits related meids. False end 
# the script after parallelization by location completes
save_splits <- T

# Splits qsub arguments (these have been profiled)
splt_threads <- 1 
splt_mem_free <- 1.9
splt_runtime <- "00:15:00"

# Save results qsub arguments (these have been profiled)
sr_threads <- 10 
sr_mem_free <- 35
sr_runtime <- "05:00:00"

# ---SOURCE FUNCTIONS-----------------------------------------------------------

source(paste0(central, "get_location_metadata.R"))
source(paste0(shared_repo, "get_best_bundle_and_crosswalk_id.R"))

# ---GET PARAMS-----------------------------------------------------------------
# Obtain a list of most detailed locations for a specified location set. These
# locations are then linked to task ids so they can be submitted as part of an 
# array job.
# ------------------------------------------------------------------------------

message("Getting params")
loc_dt <- get_location_metadata(location_set_id = location_set_id, 
                                gbd_round_id = gbd_round_id,
                                decomp_step = decomp_step)
loc_dt <- loc_dt[(is_estimate == 1 & most_detailed == 1)]
params <- data.table(location = loc_dt[, location_id])
params[, task_num := 1:.N]
if (test_splits) {
  params <- params[task_num==1]
}
write.csv(params, map_path, row.names = F)

# ---PREP FOR LOCATION SPECIFIC QSUB--------------------------------------------

# Create one of the splits output directories early for file checking purposes.
# Use TNF output files to check because TNF is the last of the splits to be
# outputted, ensuring all files have been written to disk.
tnf_dir <- paste0(output_dir, tnf, "/")
if (!file.exists(paste0(tnf_dir, date))){dir.create(paste0(tnf_dir, date), 
                                                    recursive = T)}
tnf_date_dir <- paste0(tnf_dir, date, "/")
expected_locs <- loc_dt[, unique(location_id)]

# Convert runtime from hours:minutes:seconds (h:m:s) to minutes because 
# the sleep end argument for assertable::check_files only takes minutes.
time_limit <- 60 * 24 * as.numeric(times(splt_runtime))

# ---SUBMIT JOBS FOR PROPORTION SPLITS------------------------------------------
# For each location, split the impairment envelope into primary and secondary 
# epilepsy and then split primary elipesy into severe, not severe and treated 
# no fits using regression generated proportions for each split.
# ------------------------------------------------------------------------------

while(nrow(as.data.table(params)) != 0) {
  array_qsub(jobname = "split_env",
             shell = shell_path,
             code = paste0(share_path, "01_split_env.R"),
             pass = split_args,
             proj = "proj_yld",
             num_tasks = nrow(params), 
             archive_node = T,
             cores = splt_threads, 
             mem = splt_mem_free, 
             wallclock = splt_runtime,
             log = T, 
             submit = T)
  
  # Check that all location specific output files are present before advancing 
  # past envelope splitting to another section of the code. The code will not 
  # move on until all files have been written to disk.
  assertable::check_files(filenames = paste0(expected_locs, ".csv"), 
                          folder = tnf_date_dir, 
                          continual = T,
                          sleep_time = 60,
                          sleep_end = time_limit,
                          display_pct = 95)
  
  # Identify jobs that have failed and update the parameter mapping file so 
  # that it is composed only of task ids related to those jobs. All failed 
  # jobs will be relaunched when the loop enters another iteration.
  files <- list.files(tnf_date_dir)
  files <- files[grepl("^[0-9]", files)]
  finished_locs <- stringi::stri_extract(files,regex = "[0-9]*\\.csv$")
  finished_locs <- unique(
    stringi::stri_extract(finished_locs, regex = "^[0-9]*"))
  missing <- as.numeric(setdiff(expected_locs, finished_locs))
  params <- data.table(location = missing)
  params[, task_num := 1:.N]
  map_path <- paste0(tnf_date_dir, "task_map.csv")
  write.csv(params, map_path, row.names = F)
}

if (!test_splits & save_splits) {
  
  # ---GET THE ENVELOPE BUNDLE AND CROSSWALK VERSION----------------------------
  # As of GBD 2020, all save_results_epi calls must specify the bundle_id and 
  # crosswalk_version_id used to produce the dismod model serving as a base for 
  # custom model computations. These changes were made to better track source
  # counts.
  # ----------------------------------------------------------------------------
  
  meid_metadata <- get_best_bundle_and_crosswalk_id(envelope_id,
                                                    gbd_round_id, 
                                                    decomp_step)
  
  bundle_id <- meid_metadata$bundle_id
  crosswalk_version_id <- meid_metadata$crosswalk_version_id
  
  # ---SAVE ESTIMATES-----------------------------------------------------------
  
  message("Saving results")
  
  # Switch out spaces with underscores because the individual words get inputted
  # as seperate arguments when qsubbing in R. 
  description <- gsub(" ", "_", description)
  
  for (meid in c(primary, secondary, severe, notsevere, tnf)) {
    
    if (!file.exists(errors)){dir.create(errors, recursive=T)}
    results_dir <- paste0(output_dir, meid, "/", date, "/")
    
    job_name<- paste0("save_results_", meid)
    script <- paste0(share_path, "02_save_splits.R")
    sys_sub<- paste0("qsub -P ", proj, " -N ", job_name ," -e ", errors, 
                     " -l m_mem_free=", sr_mem_free, "G", " -l fthread=", 
                     sr_threads, " -l h_rt=", sr_runtime, " -q ", q, 
                     " -l archive=TRUE")
    system(paste(sys_sub, shell_path, script, meid, gbd_round_id, decomp_step, 
                 results_dir, central, file_pattern, mark_best, bundle_id, 
                 crosswalk_version_id, description))
    print(paste(sys_sub, shell_path, script, meid, gbd_round_id, decomp_step, 
                results_dir, central, file_pattern, mark_best, bundle_id, 
                crosswalk_version_id, description))
  }
}
