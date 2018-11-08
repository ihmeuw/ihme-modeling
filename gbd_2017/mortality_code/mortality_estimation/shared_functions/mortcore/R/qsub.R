#' SGE Job Submission
#'
#' Submit jobs to IHME's SGE computing cluster for scheduling. Builds a qsub string and submits either a normal or array job, inserting the code file, jobs to hold on, arguments to pass, number of slots and memory, logging locations, intel instructions, project to run under, and shell to use.
#' 
#' @param jobname character, Name for job, must start with a letter
#' @param filepath character, Filepath to code file
#' @param hold character, Optional comma-separated string of jobnames to hold the job on i.e. "hold_name_1, hold_name_2, ..."
#' @param pass list (NOT character or numeric vector), arguments to pass on to the recieving script. e.g. list(arg1, arg2, arg3)
#' @param pass_argparse NAMED list, arguments to pass on to the recieving script. Can accept vectors as values for each named list element. Example: list(arg_name1 = arg_value1, arg_name2 = c(1:3), arg_name3 = c("val1", "val2"))
#' @param slots integer, number of computing slots to request.
#' @param mem integer, number of gb to request to be associated with this job. Defaults to 2 * slots (default is calculated in construct_qsub_string function), and 9 * slots if hostgroup = "c2". Specify a non-0 number if you want a specific memory allocation other than 2 * slots.
#' @param submit logical, whether to submit the job to the system. If F, will print out the command to be submitted
#' @param log logical, if T save in "/share/temp/sgeoutput/user/errors" and "/share/temp/sgeoutput/user/output"
#' @param intel logical, if T, add flag to request a node with Intel CPUs
#' @param hostgroup character, whether to submit jobs to a certain hostgroup. Options: "all" and "c2". c2 nodes have a higher memory-to-slot ratio and are most useful for jobs that are memory-bound.
#' @param proj character, project name to submit the job under
#' @param shell character, filepath to a user-defined shell. Default: search for "r_shell.sh", "python_shell.sh", or "stata_shell.sh" within working directory (shell name based on script ending).
#' @param num_tasks integer, for array_qsub only -- number of array tasks to submit
#' @param hold_jid_ad character, for array_qsub only -- optional comma-separated string of jobnames to use array holds on i.e. "hold_name_1, hold_name_2, ..."
#' @return None
#' @import assertthat
#' @examples 
#' \dontrun{
#' hold_jobs <- c("job1", "job2")
#' args <- list(arg1, arg2)
#' qsub(jobname = "my_job", filepath = "code_dir/01_run_model.R", hold = paste(hold_jobs, collapse = ","), pass = args, slots = 10, submit = F, intel = T, log = T, shell = "code_dir/r_shell.sh")
#' }
#' 
#' @name qsub_utils
NULL

#' @rdname qsub_utils
#' @export
qsub <- function(jobname, code, hold = NULL, pass = NULL, pass_argparse = NULL, slots = 1, mem = 0, submit = F, log = T, intel = F, hostgroup = "all", proj = "proj_mortenvelope", shell = NULL) {
  library(assertthat)
  assert_that(is.logical(submit))
  
  qsub_string <- construct_qsub_string(jobname, 
                                       code, 
                                       hold, 
                                       pass, 
                                       pass_argparse, 
                                       slots,
                                       mem, 
                                       log, 
                                       intel, 
                                       hostgroup, 
                                       proj, 
                                       num_tasks = NULL, 
                                       hold_jid_ad = NULL,
                                       shell)
    
  if (submit) {
    submit_qsub(qsub_string) 
  } else {
    cat(paste("\n", qsub_string, "\n\n "))
    flush.console()
  } 
}

# ----- array qsub function ----- # 
# jobname : Name for job, must start with a letter
# code : Filepath to code file
# hold : Optional comma-separated string of job names to hold the job on 
#        i.e. "hold_name_1, hold_name_2, ..."
# pass : Optional list of arguments to pass on to receiving script
# slots : Number of slots to request for each task in the array job
#  Default: 1
# submit : Whether to submit the job to the system or print out the command to be submitted
#   Default: False
# log : Whether this job saves error and output files for jobs in
#   "errors" and "output"
#   Default: True
# intel : Whether to specifically request a node with an Intel CPU from the scheduler
#   Default: False
# proj : A string indicating under which project to submit the job
#   Default: "proj_mortenvelope"
# num_tasks : Number of tasks to run in the job
#   Default: 1
#   Default: 1
# hold_jid_ad : Optional comma-separated string of array job names whose tasks this job's tasks will hold on
# shell : Filepath a user-defined shell file
#   Default: searches for a "r_shell.sh" or "python_shell.sh" or "stata_shell.sh"
#            in the current working directory, relative to the code's file extension (.R, .py, or .do) 

#' @rdname qsub_utils
#' @export
array_qsub <- function(jobname, code, hold = NULL, pass = NULL, pass_argparse = NULL, slots = 1, mem = 0, submit = F, log = T, intel = F, hostgroup = "all", proj = "", num_tasks = 1, hold_jid_ad = NULL, shell = NULL) {
  assertthat::assert_that(is.logical(submit))
  
  qsub_string <- construct_qsub_string(jobname, 
                                       code, 
                                       hold, 
                                       pass, 
                                       pass_argparse, 
                                       slots, 
                                       mem,
                                       log, 
                                       intel, 
                                       hostgroup, 
                                       proj, 
                                       num_tasks, 
                                       hold_jid_ad,
                                       shell)
    
  if (submit) {
    submit_qsub(qsub_string) 
  } else {
    cat(paste("\n", qsub_string, "\n\n "))
    flush.console()
  } 
}


# ----- Helper Functions ----- #
#' @export
construct_qsub_string <- function(jobname, code, hold, pass, pass_argparse, slots, mem, log, intel, hostgroup, proj, num_tasks, hold_jid_ad, shell) {
  is_whole_number <- function(x) {
    if (!is.numeric(x)) {
      return(FALSE)
    }
    return((x == round(x)))
  }
  
  # Test types of arguments passed to function --------------------------- 
  assertthat::assert_that(is.string(jobname))
  assertthat::assert_that(is.string(code))
  assertthat::assert_that(is.string(hold) | is.null(hold))
  assertthat::assert_that(is.list(pass) | is.null(pass))
  assertthat::assert_that(is.list(pass_argparse) | is.null(pass_argparse))

  # check all elements in list of pass
  if (is.list(pass)) {
    lapply(pass, FUN = function(element) {
      if (!(is.character(element) | is.numeric(element) | is.logical(element))) {
        stop(paste0("One or more of the elements in the list given to the 'pass' 
                    option is not of type character, numeric, or logical."))
      }
      })
    }

  if (is.list(pass_argparse)) {
  lapply(pass_argparse, FUN = function(element) {
    if (!(is.character(element) | is.numeric(element) | is.logical(element))) {
      stop(paste0("One or more of the elements in the list given to the 'pass_argparse' 
                  option is not of type character, numeric, or logical."))
    }
    })
  }

  assertthat::assert_that(is.numeric(slots))
  if (!is_whole_number(slots)) {
    stop(paste0("The given number of slots: ", slots, " is not a whole number."))
  } else if (slots < 1) {
    stop(paste0("The given number of slots: ", slots, " is less than 1."))
  }

  assertthat::assert_that(is.numeric(mem))
  if (!is_whole_number(mem)) {
    stop(paste0("The given number of mem: ", mem, " is not a whole number."))
  }

  assertthat::assert_that(is.logical(log))
  assertthat::assert_that(is.logical(intel))
  assertthat::assert_that(is.string(proj))
  assertthat::assert_that(is.string(hostgroup))

  if (!is.null(num_tasks)) {
    if (!is_whole_number(num_tasks)) {
      stop(paste0("The given number of array tasks: ", num_tasks, " is not a whole number."))
    } else if (num_tasks < 1) {
      stop(paste0("The given number of array tasks: ", num_tasks, " is less than 1."))
    }
  }
  
  assertthat::assert_that(is.string(hold_jid_ad) | is.null(hold_jid_ad))
  assertthat::assert_that(is.string(shell) | is.null(shell))
  
  if (!grepl(pattern = "^([a-z]|[A-Z])", jobname, ignore.case = T)) {
    stop(paste0("The name of the job: '", jobname, "' must begin with a letter."))
  }
  
  # Construct String  ---------------------------
  
  qsub_components <- c("qsub")
  
  
  if (log) {
    log_string <- paste0("-e ", user, "/errors -o ", user, "/output")
  } else {
    log_string <- "-e /dev/null -o /dev/null"
  }
  
  qsub_components <- append(qsub_components, log_string)
  
  
  if (proj != "") {
    project_string <- paste0("-P ",proj)
    qsub_components <- append(qsub_components, project_string)
  }
  
  # set up number of slots
  if (slots > 1) { 
    slot_string <- paste0("-pe multi_slot ", slots)
    qsub_components <- append(qsub_components, slot_string)
  } 

  # set up memory
  if (mem == 0) { 
    if(hostgroup != "c2") mem_multiplier <- 2
    if(hostgroup == "c2") mem_multiplier <- 9
    req_mem <- mem_multiplier * slots
  } else {
    req_mem <- mem
  }
  
  mem_string <- paste0("-l mem_free=", req_mem, "G")
  qsub_components <- append(qsub_components, mem_string)
  
  # set up jobs to hold for 
  if (!is.null(hold)) { 
    hold_string <- paste0("-hold_jid \"", hold, "\"")
    qsub_components <- append(qsub_components, hold_string)
  }
  
  # Add job name
  qsub_components <- append(qsub_components, paste0("-N ",jobname))
  
  # Add array arguments if specified
  if (!is.null(num_tasks)) {
    array_job_tasks <- paste0("-t 1:", num_tasks)
    qsub_components <- append(qsub_components, array_job_tasks)
  }
  
  if (!is.null(hold_jid_ad)) {
    array_hold_string <- paste0("-hold_jid_ad \"", hold_jid_ad, "\"")
    qsub_components <- append(qsub_components, array_hold_string)
  }

  # check if code specified exists
  if (!file.exists(code)) {
    stop(paste0("Path to the given code file: '", code, "' does not exist."))
  }
  
  # choose appropriate shell_path script 
  if (is.null(shell)) {
    if (grepl("(R)$", code, perl = T, ignore.case = T)) { 
      shell_path <- "r_shell.sh"     
    } else if (grepl("(py)$", perl = T, code, ignore.case = T)) {
      shell_path <- "python_shell.sh" 
    } else if (grepl(".do", perl = T, code, ignore.case = T)) {
      shell_path <- "stata_shell.sh" 
    } else {
      stop("'code' option must reference path to file with extension '.R', 
           '.py', or '.do', otherwise, pass a path to a user-defined shell script.")
    }
    } else {
      shell_path <- shell
  }
  
  # check if shell_path script exists
  if (!file.exists(shell_path)) {
    stop(paste0("Path to the given shell script: '", shell_path, "' does not exist."))
  }
  
  qsub_components <- append(qsub_components, shell_path)
  qsub_components <- append(qsub_components, code)

  if(!is.null(pass) & !is.null(pass_argparse)) stop("Cannot specify arguments to pass and pass_argparse -- choose one method to send job arguments")

  # set up arguments to pass in 
  pass_string <- c()
  if (!is.null(pass)) { 
    for (ii in pass) {
      pass_string <- append(pass_string, paste0("\"", ii, "\""))
    }
    pass_string <- paste0(pass_string, collapse = " ")
    qsub_components <- append(qsub_components, pass_string)
  }

  pass_argparse_string <- c()
  if (!is.null(pass_argparse)) { 
    for (ii in 1:length(pass_argparse)) {
      if(names(pass_argparse)[[ii]] == "") stop("Names of all pass_argparse list elements must be defined")
      new_string <- paste0("--", names(pass_argparse)[[ii]], " ", paste(pass_argparse[[ii]], collapse = " "))
      pass_argparse_string <- append(pass_argparse_string, new_string)
    }
    pass_argparse_string <- paste0(pass_argparse_string, collapse = " ")
    qsub_components <- append(qsub_components, pass_argparse_string)
  }
  
  qsub_string <- paste(qsub_components, collapse = " ")
  
  return(qsub_string)
  }

#' @export
submit_qsub <- function(qsub_string) {
  assertthat::assert_that(is.string(qsub_string))
  system(qsub_string)
}
