#####################################INTRO#############################################
#' Author: 
#' 6/14/18
#' Purpose: 
#'          1) Launch jobs to get draws and age-restricted by location in parallel
#'          2) Wait for files to finish saving, relaunching if necessary
#'          3) Run save_results
#'
#' OUTPUTS: Prevalence estimates for females uploaded to the epi database under ME
#'
#####################################INTRO#############################################

library("ihme", lib.loc = "FILEPATH")
setup()

library(rjson)

source_functions(get_best_model_versions = T, get_location_metadata = T, 
                 save_results_epi = T)


# Set up model info -------------------------------------------------------
message(paste(Sys.time(), "Set up script"))

date <- gsub("-", "_", Sys.Date())

# modelable entity ids
me_id        <- 2423  # dismod ME ID for PID envelope model
upload_me_id <- 20419 # custom model ME ID for age-restricted PID envelope

# age range for PID
age_group_start <- 7  # ages 10 - 14
age_group_end   <- 16 # ages 55 - 59

model       <- get_best_model_versions("modelable_entity", me_id)
description <- paste0("PID model ", model$model_version_id, " age-restricted from age groups ", age_group_start, " to ", age_group_end, " on ", Sys.Date())

# grab all locations from location set 35 (model results)
# this will grab ALL locations, including regions, super regions, etc.
# because I'm not sure exactly what level of detail is needed
location_data <- get_location_metadata(35)
locations     <- unique(location_data$location_id)

# Set up directories, creating a new one for each run ---------------------
root_dir <- paste0("ROOT_DIR/")
out_base <- paste0("OUT_BASE/")
out_dir  <- paste0(out_base, "run_", date,"/") 

# Create directories, gives warning if already exists
dir.create(out_base) # base directory
dir.create(out_dir)  # stores draws for the given run


# Function definitions ----------------------------------------------------

# Submit one job for a given location
#'
#'@param location location_id to pass into launched script
#'@param root_dir directory storing the code for the launched script
#'@param out_dir  directory where age-restricted draw files are stored
#'@param code     the name of the launched script
#'
submit_for <- function(location, root_dir, out_dir, code, age_group_start, age_group_end) {
  jobname <- paste0("pid_adj_", location)
  args <- list(out_dir, location, age_group_start, age_group_end)
  
  ihme::qsub(jobname, code = paste0(root_dir, code), pass = args,
             slots = 2, submit = T, proj = "proj_custom_models",
             shell = paste0("SHELL_SCRIPT"))
}

#' recursive method to check if the right number of files has been saved
#' if not, wait x amount of seconds (default 60s)
#' if been sitting at 95% done for repeat_limit number of times,
#' jobs are relaunched. If the relaunch limit has been reach, throws an error
#' 
#' @requires saved csvs in the form of {location_id}.csv
#' @param locations character vector of location_ids to check for
#' @param files_in  file path where csvs are stored
#' @param relaunch_code code to run if jobs fail
#' @param time number > 0 of seconds to wait before checking file counts again.
#'             defaults to 60 s
#' @param relaunch_limit number of times to relaunch missing jobs before throwing an arrow.
#'                       defaults to twice. Not recommended to change this parameter
#' @param repeats Number of times the wait_for loop has repeated, if file count is > 95% complete. 
#'                Default is 0. Not recommended to mess with this parameter unless debugging wait_for function
#' @param repeat_limit Number of repeats of wait_for loops to go through once file count has hit 95% before
#'                     relaunching jobs. Default is 5
#' 
wait_for <- function(locations, files_in, relaunch_code, time = 60, relaunch_limit = 2, repeats = 0, repeat_limit = 5) {
  number    <- length(locations)
  num_files <- length(list.files(files_in))
  
  # if we've been stuck above 95% complete for the limit of repeats (default 5),
  # relaunch the models that haven't finished
  if (repeats == repeat_limit) {
    finished      <-  list.files(files_in)
    finished_locs <- unique(as.numeric(gsub(".csv$", "", finished)))
    relaunch      <- setdiff(locations, finished_locs)
    
    #check if we've hit the relaunch limit yet,
    # if so there's prob a bigger issue and throw error
    if (relaunch_limit == 0) {
      stop(paste0("Relaunch limit reached without results for all locations. Missing ", relaunch,
                  " . Breaking."))
    }
    
    cat(paste0("Relaunching jobs for location ids: ", relaunch, "\n"))
    invisible(lapply(sort(relaunch), submit_for,
                     root_dir = root_dir,
                     out_dir = out_dir,
                     code_dir = relaunch_code))
    
    # recurse again, lowering relaunch limit by 1 and reseting repeats to 0
    wait_for(locations, files_in, relaunch_code = relaunch_code, time = time, 
             relaunch_limit = relaunch_limit - 1, repeats = 0, repeat_limit = repeat_limit)
    
  } else if (num_files < number) {
    if (num_files / number > 0.95) {
      cat(paste0("Over 95% completed. Repeating cycle ", repeat_limit - repeats, " more times before relaunching missing locations\n"))
      repeats <- repeats + 1
    }
    cat(paste0(Sys.time(), " ", num_files, " out of ", number, " (", signif(num_files / number * 100, digits = 3),
               "%) saved. Sleeping another ", time, "s\n"))
    Sys.sleep(time)
    wait_for(locations, files_in, relaunch_code = relaunch_code, time = time, relaunch_limit = relaunch_limit, repeats = repeats, repeat_limit = repeat_limit)
  } else {
    # base case: ends recursion
    cat(paste0(Sys.time(), " Complete\n"))
  }
}


# Launch age-restriction jobs by location ---------------------------------
message(paste(Sys.time(), "Launch jobs to age-restrict PID envelope model by location"))

invisible(parallel::mclapply(sort(locations), submit_for, root_dir = root_dir, out_dir = out_dir, code = "01_restrict_age.R",
                             age_group_start = age_group_start, age_group_end = age_group_end))


# Wait for files to finish ------------------------------------------------
message(paste(Sys.time(), "Waiting for age-restricted draws to save for all locations"))

wait_for(locations, files_in = out_dir, relaunch_code = paste0(root_dir, "01_restrict_age.R"), time = 60, repeat_limit = 15) 


# Upload results to the Epi database --------------------------------------
message(paste(Sys.time(), "Saving results to me_id", upload_me_id))


# try and save, creating message based on success/failure staus
tryCatch({
  save_results_epi(input_dir = out_dir, input_file_pattern = paste0("{location_id}.csv"), measure_id = c(5, 6),
                   sex_id = 2, modelable_entity_id = upload_me_id, description = description,
                   mark_best = TRUE, birth_prevalence = FALSE)
  assign("message", paste0("PID age-restriction uploaded successfully to me_id ", upload_me_id, 
                           " with description: ", description), .GlobalEnv)
}, error = function(e) {
  assign("message", paste0("PID age-restriction upload FAILED. Error: ", e), .GlobalEnv)
})



# Make a message ----------------------------------------------------------
json <- rjson::fromJSON(file = paste0(h_root, "private_url.json"))

post_slack <- function(message, url) {
  system(paste0("curl -X POST -H \'Content-type: application/json' --data \'{\"text\": \"", message, "\"}\' ", url))
}

# post message
post_slack(message = message, url = json$pipeline_updates)










