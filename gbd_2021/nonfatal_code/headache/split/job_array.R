##########################################################################
## Author: Marlena Norwood (mnorwood@uw.edu)
## Date: March 15, 2017
## Purpose: Function to submit a job-array in the master script, and function
##          to call in the child script to extract relevant info from master script
## Usage: see 
##########################################################################

library(pacman, lib.loc = paste0("FILEPATH", "package_lib/"))
pacman::p_load(data.table, magrittr)

# job.array.master(): function to submit an array job from the master script

# Inputs -------------------------------------------------
#   tester: should we submit only a subset of the parameters for testing?
#   paramlist: a named list of the *unique* parameters that will be sent to the child script as another argument
#   username: person running for the error output files
#   project: name of IHME project
#   slots: number of slots each child script should use
#   jobname: name of job to submit
#   childscript: name of the childscript in the working directory
#   shell: name of r shell script
#   args: list of additional arguments to pass to child script (must be in list form)
job.array.master <- function(tester = T, paramlist, username = "USERNAME", project = "USERNAME",
                             slots = 4, jobname = "testjob", childscript = "01_child.R", shell = "FILEPATH", args = NULL) {
  
  require(magrittr, data.table)
  
  # create the parameter grid and save as a flat file to pull in in the child script
  folder <- paste0("FILEPATH")
  cat(folder, "\n")
  
  # create the folder
  dir.create(folder)
  filepath <- "FILEPATH"
  cat(filepath, "\n")
  
  params <- expand.grid(paramlist)
  write.csv(params, filepath, row.names = F)
  cat("Wrote CSV", "\n")
  
  # submit the number of jobs for testing
  # OR to the length of the parameters
  
  n <- ifelse(tester, 2, nrow(params))
  
  error <- paste0("FILEPATH")
  output <- paste0("FILEPATH")
  
  # create the qsub command
  sys.sub <- paste0("qsub -cwd -N ", jobname, " -P ", project, " -t 1:", n, " -pe multi_slot ", slots, error, output)
  cat("Sys sub: ", sys.sub, "\n")
  
  # get the additional arguments to pass to the QSUB
  # plus send it the filepath it needs for the parameter grid
  
  args <- list(filepath, paste(args, collapse = " ")) %>% paste(collapse = " ")
  cat("Args: ", args, "\n")
  
  qsub <- paste(sys.sub, shell, childscript, "\"", args, "\"")
  print(paste("QSUB COMMAND:", qsub))
  
  # QSUB COMMAND SUBMIT
  system(paste(qsub))
}


job.array.master.hold <- function(tester = T, paramlist, username = "USERNAME", project = "USERNAME",
                                  slots = 4, jobname = "testjob", childscript = "01_child.R", shell = "FILEPATH", holdjob = NULL, args = NULL) {
  
  require(magrittr, data.table)
  
  # create the parameter grid and save as a flat file to pull in in the child script
  folder <- paste0("FILEPATH")
  cat(folder, "\n")
  
  # create the folder
  dir.create(folder)
  filepath <- paste0("FILEPATH")
  cat(filepath, "\n")
  
  params <- expand.grid(paramlist)
  write.csv(params, filepath, row.names = F)
  cat("Wrote CSV", "\n")
  
  # submit the number of jobs for testing
  # OR to the length of the parameters
  
  n <- ifelse(tester, 2, nrow(params))
  
  error <- paste0("FILEPATH")
  output <- paste0("FILEPATH")
  
  # create the qsub command
  sys.sub <- paste0("qsub -cwd -N ", jobname, " -P ", project, " -t 1:", n, " -hold_jid ", holdjob, " -pe multi_slot ", slots, error, output)
  cat("Sys sub: ", sys.sub, "\n")
  
  # get the additional arguments to pass to the QSUB
  # plus send it the filepath it needs for the parameter grid
  
  args <- list(filepath, paste(args, collapse = " ")) %>% paste(collapse = " ")
  cat("Args: ", args, "\n")
  
  qsub <- paste(sys.sub, shell, childscript, "\"", args, "\"")
  print(paste("QSUB COMMAND:", qsub))
  
  # QSUB COMMAND SUBMIT
  system(paste(qsub))
}

# job.array.child(): function to pull information from master script into the child script

# Returns ------------------------------------------------
# element [[1]]: list of the desired parameters based on task_id pulled from SGE
# element [[2]]: list of additional arguments that are passed on from the master script in the "args" argument
#                 if no additional arguments are specified, returns NA

job.array.child <- function(){
  
  require(magrittr, data.table)
  
  # grab the task_id from the SGE environment
  task_id <- Sys.getenv("SGE_TASK_ID") %>% as.numeric
  cat("This task_id is ", task_id)
  
  # read in the file path that is passed from the parent script
  # and save the parameters that correspond to this task id
  params <- fread(commandArgs()[3])[task_id,]
  
  # get the additional arguments
  # if you don't have any additional arguments passed from the master script,
  #   don't need to pull job.array.child()[[2]]
  args <- commandArgs()[3:length(commandArgs())]
  
  return(list(params, args))
}
