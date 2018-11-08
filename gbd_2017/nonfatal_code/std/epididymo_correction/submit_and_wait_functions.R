#####################################INTRO#############################################
#' Author: 
#' 5/17/18
#' Purpose: De-coupling functions for submiting jobs by location and wait_for so I can source it from where needed
#'
#####################################INTRO#############################################

library("ihme", lib.loc = "FILEPATH")
setup()


# FUNCTIONS ----------------------------------------------------------------

# Function to submit incidence or prevalence grabbing (02 or 04) depending on
# the me_ids supplied. Uses the me_id_to_string() function below to craft job names
# Can launch BOTH gonorrhea and chlamydia jobs at once or one at a time
#'
#'@require me_id_to_string function that converts an me_id to a string identifier
#'@param location_id location_id to pass into launched script
#'@param code_dir    the name of the launched script
#'@param root_dir    directory storing the code for the launched script
#'@param out_dir     directory where draw files are stored
#'@param me_id       me_id to submit a job for. Allows flexible job submission of gonorrhea, chlamydia, or both
#'@param run         boolean, true iff the job is to be submitted. False otherwise
#'
submit_both <- function(location_id, code_dir, out_dir, me_ids, run = T) {
  for (me in me_ids) {
    ihme::qsub(jobname = paste0(me_id_to_string(me), "_", location_id), 
               code = code_dir, pass = list(location_id, me, out_dir),
               slots = 1, submit = run, proj = "proj_custom_models",
               shell = paste0(j_root, "SHELL"))
  }
}


# Helper method: Coverts an me_id into a short string version in order to create a jobname with an me_id
#'
#'@param me_id modelable_entity_id to be converted into a string representation to put in a job name
#' OPTIONS:
#' 1629: chlamydia, 1635: gonorrhea
#' 20394: Epididymo-orchitis due to chlamydia
#' 20393: Epididymo-orchitis due to gonorrhea
#'
#'@return string representation of the me_id passed in. Returns NULL if me_id does not match one
#'        of the four options above
me_id_to_string <- function(me_id) {
  me_id <- as.character(me_id)
  switch (me_id,
          `1629` = "CT",
          `1635` = "GO",
          `20394` = "CT_up",
          `20393` = "GO_up"
  )
}



# Recursive method to check if the right number of files has been saved
# if not, wait x amount of seconds (default 60s)
# if been sitting at 95% done for repeat_limit number of times,
# jobs are relaunched. If the relaunch limit has been reach, throws an error
#
#' @requires saved csvs in the form of {me_id}_{location_id}.csv
#' @param locations character vector of location_ids to check for
#' @param files_in  file path where csvs are stored
#' @param pattern   csv name pattern to check for; either {me_id} or {me_id_eo}
#' @param me        modelable_entity to check for; defaults to me_id variable in gloabl environment
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
wait_for <- function(locations, files_in, pattern, me = me_id, relaunch_code, time = 60, relaunch_limit = 2, repeats = 0, repeat_limit = 5) {
  number    <- length(locations)
  num_files <- length(grep(me, list.files(files_in)))
  
  # if we've been stuck above 95% complete for the limit of repeats (default 5),
  # relaunch the models that haven't finished
  if (repeats == repeat_limit) {
    finished      <- grep(me, list.files(files_in), value = TRUE)
    finished_locs <- gsub(pattern, "", finished) %>% 
      gsub("\\.csv", "", .) %>% 
      as.numeric() %>% unique()
    relaunch <- setdiff(locations, finished_locs)
    
    #check if we've hit the relaunch limit yet,
    # if so there's prob a bigger issue and throw error
    if (relaunch_limit == 0) {
      stop(paste0("Relaunch limit reached without results for all locations. Missing ", relaunch,
                  " . Breaking."))
    }
    
    cat(paste0("Relaunching jobs for location ids: ", relaunch, "\n"))
    invisible(lapply(sort(relaunch), submit_both,
                     code_dir = relaunch_code,
                     out_dir = main_dir,
                     me_ids = me))
    
    # recurse again, lowering relaunch limit by 1 and reseting repeats to 0
    wait_for(locations, files_in, pattern = pattern, relaunch_code = relaunch_code, time = time, relaunch_limit = relaunch_limit - 1, repeats = 0)
    
  } else if (num_files < number) {
    if (num_files / number > 0.95) {
      cat(paste0("Over 95% completed. Repeating cycle ", repeat_limit - repeats, " more times before relaunching missing locations\n"))
      repeats <- repeats + 1
    }
    cat(paste0(Sys.time(), " ", num_files, " out of ", number, " (", signif(num_files / number * 100, digits = 3),
               "%) saved. Sleeping another ", time, "s\n"))
    Sys.sleep(time)
    wait_for(locations, files_in, pattern = pattern, relaunch_code = relaunch_code, time = time, relaunch_limit = relaunch_limit, repeats = repeats, repeat_limit = repeat_limit)
  } else {
    # base case: ends recursion
    cat(paste0(Sys.time(), " Complete\n"))
  }
}






