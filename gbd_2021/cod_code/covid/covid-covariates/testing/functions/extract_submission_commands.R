CLUSTER_NAME
#'
#' Intended to find Rstudio singularity image versions for metadata.  Finds a
#' user-defined string from the submission command you used to start your
#' Rstudio singularity image (or something else you desire).  Extracts this
#' informtion from ALL jobs you currently have active in your squeue.
#'
#' @param squeue_jobname_filter [character|regex] when you run `squeue -u
#'   <username>`, what `NAME` do you want to filter for?
#' @param max_cmd_length [integer] how many characters long is your command?
#'   Increase your default if the command is truncated.  All leading/trailing
#'   whitespace is trimmed.
#' @param string_to_extract [character|regex] what string do you want to extract
#'   after running  `sacct -j <jobid> -o submitline%xxx` using
#'   `stringr::str_extract_all`
#' @param strings_to_ignore [character|regex] if your `string_to_extract`
#'   command finds more strings than you want, this removes all strings with
#'   this pattern anywhere inside using `stringr::str_detect`
#' @param user_name [character] which user's commands to find - defaults to your
#'   own
#'
#' @return [list] all desired submission commands, and specific extracted text
#'   from string_to_extract
extract_submission_commands <- function(
  
  squeue_jobname_filter = "^rst_ide",
  max_cmd_length        = 500L,
  string_to_extract     = "FILEPATH",
  strings_to_ignore     = "jpy",
  user_name             = Sys.info()[["user"]]
  
) {
  
  if (is.null(string_to_extract)) {
    stop("You must specify a string to find and extract from command line submissions")
  }
  
  # command to find user's cluster jobs
  
  job_finder <- function(user = user_name) {
    all_jobs <- system2(SYSTEM_COMMAND), 
      stdout = T
    )
    
    return(all_jobs)
  }
  
  # extract submission information
  
  jobs <- job_finder()
  jobs <- read.table(text = jobs, header = T, sep = "", )
  
  # format table & extract jobids
  names(jobs) <- tolower(names(jobs))
  jobs <- jobs[, c("jobid", "name")]
  rstudio_filter <- grepl(squeue_jobname_filter, jobs[["name"]])
  jobs <- jobs[rstudio_filter, ]
  
  submit_command_list <- list()
  
  # use sacct to extract original rstudio image submission commands
  # only works if rstudio was started from CLI, not from API
  # INTERNAL_COMMENT
  
  for (i in 1:nrow(jobs)) {
    
    job_id <- jobs[i, "jobid"]
    
    command_args <- paste(
      "-j", job_id,
      paste0("-o submitline%", max_cmd_length)
    )
    
    submission_command <- system2(SYSTEM_COMMAND)[[3]]
    
    submission_command <- tolower(trimws(submission_command, which = "both"))
    
    submit_command_list[[i]] <- submission_command
    
  }
  
  extract_command_string <- function (submit_command_element) {
    
    extracted_strings <- stringr::str_extract_all(submit_command_element, string_to_extract)
    
    # str_extract_all produces a list - need to deal with it
    if(!length (extracted_strings) == 1) {
      stop("submit_command_list has more than one element - investigate. 
             You likely have more than one pattern specified")
    }
    
    extracted_strings <- extracted_strings[[1]]
    ignore_filter <- !sapply(extracted_strings, stringr::str_detect, pattern = strings_to_ignore)
    
    # return only strings without jpy language
    return(extracted_strings[ignore_filter])
    
  }
  
  extracted_cmd_strings <- lapply(submit_command_list, extract_command_string)
  
  out_list <- list(
    submission_commands = submit_command_list,
    extracted_cmd_strings = extracted_cmd_strings
  )
  
  return(out_list)
  
}
