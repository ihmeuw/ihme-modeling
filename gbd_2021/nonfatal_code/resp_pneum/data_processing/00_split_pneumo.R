### Pneumo splits
# This script will take cod proportions and use them to split a parent pneumo dismod model
# Dismod pneumo parent MEID: 24988

#### Starting of splits ------------------------------------------------------------

### Function sourcing
source("FILEPATHget_location_metadata.R")
source("FILEPATHget_model_results.R")
source("FILEPATHget_cod_data.R")

source("FILEPATHget_age_metadata.R")
source("FILEPATH/submit_parallel_jobs.R")
library(general.utilities, lib.loc = "FILEPATH")

### coal exclusion percentile
percent_exclude <- .05 # Use if we want to exclude coal values

### Subset locs to 1-10 for testing purposes
locations <- get_location_metadata(location_set_id = 9, gbd_round_id = 7, decomp_step = "step3")
locs <- c(locations$location_id, 20, 189)
# locs <- locs[1:10] # for testing

# Where you save outputs and where the mapping file is
directory_to_save <- "FILEPATH"
path_to_map <- "FILEPATH/pneumo_cod_map.csv"

### standard cluster things
shell = 'FILEPATH'
errors <- "FILEPATH"
user <- "username"
project = "proj_yld "
q = "all.q"

### Split script settings
threads = 3
memory = 10
runtime = "2:00:00"
job_name_1 = "split_pneumos"

# Script
script_01 <- "FILEPATH/01_split_pneumo.R" # Path to script


### Get exclusion_value for coal
cod_data <- get_model_results(gbd_id = 513,
                              gbd_team = "cod",
                              age_group_id = 22,
                              gbd_round_id = 7,
                              decomp_step = 'step3')

exclusion_value <- quantile(cod_data$mean_death_rate, percent_exclude) # find low quantile globally

print('Starting splits -----------------------------------')
# Only need to pass the mapping file to script
arguments_for_script <- c(path_to_map, exclusion_value)

# Submits jobs parallel by location
job.array.master(tester = F,
                 paramlist = locs,
                 username = user,
                 project = project,
                 threads = threads,
                 mem_free = memory,
                 runtime = runtime,
                 errors = errors,
                 q = q,
                 jobname = job_name_1,
                 childscript = script_01,
                 shell = shell,
                 args = arguments_for_script)


### Save results --------------------------------------------------------------
# Can run this immediately and will qhold until splits are finished

# Arguments
# description <- paste0("Using_copd_emr_smoothed_coal_exclude_codrefresh9_exclude", percent_exclude)
description <- "Using_copd_emr_no_coal_exclusions"
# meids <- c(24989, 24990, 24991, 24992) # might be deprecated. MEIDS created for this new split, but saving instead to original meids
meids <- c(24657, 3052, 24656, 24659) # custom coal

# meids <- c(24657, 24658, 24656, 24659) # Original EMR meids.
cause <- c("asbestosis", "coal", "silicosis", "other_pneum")

# Save script settings
threads = 20
memory = 80
runtime = "5:00:00" # if cluster is busy, might need to set 24 hours in case

# Script
save_script <- "FILEPATH/02_save_pneumo.R" # Path to script
mapping <- read.csv(path_to_map, stringsAsFactors = FALSE)

print('Starting saves -----------------------------------')
for(i in 1:length(meids)){
  print(cause[i])
  job_name_2 <- paste0(' save_pneumo_', meids[i], "_", cause[i])
  sys_sub <- paste0('qsub', 
                    ' -P ', project,
                    ' -N ', job_name_2, 
                    ' -e ', errors,
                    ' -hold_jid ', job_name_1, # holding job1 is still running
                    ' -l m_mem_free=', memory, 'G', 
                    ' -l fthread=', threads,
                    ' -l h_rt=', runtime, 
                    ' -q ', q)
  
  directory <- paste0(mapping$directory_to_save[i], mapping$target_me_id[i], "/")
  
  system(paste(sys_sub, shell, save_script, path_to_map, meids[i], directory, description))
  print(paste(sys_sub, shell, save_script, path_to_map, meids[i], directory, description))
}
  
