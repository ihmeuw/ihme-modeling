#'####################################`INTRO`##########################################
#' @author: 
#' 7/3/18
#' @purpose: De-coupling wait_for and other functions from master script for cleanliness
#' 
#'####################################`INTRO`##########################################




# Recursive method to check if the right number of files has been saved
# if not, wait x amount of seconds (default 60s)
# if been sitting at 95% done for repeat_limit number of times,
# jobs are relaunched. If the relaunch limit has been reach, throws an error
#
#' @requires saved csvs in the form of {me_id}_{location_id}.csv
#' @param number    number of files to check for
#' @param files_in  file path where csvs are stored
#' @param time number > 0 of seconds to wait before checking file counts again.
#'             defaults to 60 s
#' @param relaunch_limit number of times to relaunch missing jobs before throwing an arrow.
#'                       defaults to twice. Not recommended to change this parameter
#' @param repeats Number of times the wait_for loop has repeated, if file count is > 95% complete. 
#'                Default is 0. Not recommended to mess with this parameter unless debugging wait_for function
#' @param repeat_limit Number of repeats of wait_for loops to go through once file count has hit 95% before
#'                     relaunching jobs. Default is 5
#' @param FUN function to run of jobs need to be relaunched
#' @param ... arguments to be passed into @FUN
#' 
wait_for <- function(number, files_in, time = 60, relaunch_limit = 2, repeats = 0, repeat_limit = 5, FUN, ...) {
  num_files <- length(list.files(files_in))
  
  # if we've been stuck above 95% complete for the limit of repeats (default 5),
  # relaunch the models that haven't finished
  if (repeats == repeat_limit) {
    finished <-  list.files(files_in)
    finished_locs <- unique(as.numeric(gsub("_.*_.*$", "", finished)))
    relaunch <- setdiff(locations, finished_locs)
    
    # check if we've hit the relaunch limit yet,
    # if so there's prob a bigger issue and throw error
    if (relaunch_limit == 0) {
      stop(paste0("Relaunch limit reached without results for all locations. Missing ", relaunch,
                  " . Breaking."))
    }
    
    cat(paste0("Relaunching jobs for location ids: ", relaunch, "\n"))
    invisible(lapply(sort(relaunch), FUN, ...))
    
    # recurse again, lowering relaunch limit by 1 and reseting repeats to 0
    wait_for(number, files_in, time = time, relaunch_limit = relaunch_limit - 1, repeats = 0, repeat_limit = repeat_limit)
    
  } else if (num_files < number) {
    if (num_files / number > 0.95) {
      cat(paste0("Over 95% completed. Repeating cycle ", repeat_limit - repeats, " more times before relaunching missing locations\n"))
      repeats <- repeats + 1
    }
    cat(paste0(Sys.time(), ": ", num_files, " out of ", number, " (", signif(num_files / number * 100, digits = 3),
               "%) saved. Sleeping another ", time, "s\n"))
    Sys.sleep(time)
    wait_for(number, files_in, time = time, relaunch_limit = relaunch_limit, repeats = repeats, repeat_limit = repeat_limit)
  } else {
    # base case: ends recursion
    cat(paste0(Sys.time(), ": Complete\n"))
  }
}




#' Safely interpolate when parallelizing by location. 
#' 
#' @param limit     the number of interpolate calls allowed at a time. I think 40 is reasonable
#' @param wait_time number of seconds to wait before rechecking how many interpolate calls are running.
#'                  Default is 15
#' @param dir       directory the temp files txt are saved in. These temp files are used to see if an interpolate call is still running
#' @param unique_id the unique_id used to identify the temp file for this particular call. I suggest location_id
#' @param ...       arguments to be passed in to `interpolate()`
#'
#' @return data.table of interpolated results
#' 
interpolate_safe <- function(limit, wait_time = 120, dir, unique_id, ...) {
  if (length(list.files(dir)) < limit) {                                   # num of interpolate calls is less than limit
    on.exit(system(paste0("rm ", paste0(dir, "/tmp", unique_id, ".txt")))) # call this here in case interpolate (or seomthing else) fails unexpectedly
    write.csv("", paste0(dir, "/tmp", unique_id, ".txt"))
    f <- interpolate(...)
    return(f)
  } else { # too many interpolate calls running right now, must wait
    cat(paste(Sys.time(), "Waiting for other interpolate calls to finish...\n"))
    Sys.sleep(wait_time)
    interpolate_safe(limit, wait_time, dir = dir, unique_id = unique_id, ...)
  }
}










