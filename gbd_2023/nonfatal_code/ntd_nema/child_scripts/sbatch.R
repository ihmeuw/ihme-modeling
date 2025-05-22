#############################################################################################
###                                     SET UP FUNCTION                                   ###
#############################################################################################

sbatch <- function(job_name = "array_job", project, num_jobs, shell, code, output = NULL, error = NULL, args = NULL, threads = 1, memory = 2, time = "00:30:00", archive = F, queue = "ADDRESS"){
  
  if (is.null(num_jobs)) stop("Did not specify number of jobs, please insert before running again")
  if (is.null(shell))    stop("Did not specify a shell script, please provide one before running again")
  if (is.null(code))     stop("Did not specify a computation script (code parameter), please provide one before running again")
  
  if (is.null(threads))  warning("Did not specify any threads, setting to 1")
  if (is.null(memory))   warning("Did not specify any memory, setting to 2")
  if (is.null(time))     warning("Did not specify time, setting time at 30 minutes")
  
  my_job    <- paste0("-J ", job_name, " ")
  my_num    <- paste0("-a 1-", as.character(num_jobs), "%100 ")
  my_shell  <- paste0(shell, " ")
  my_code   <- paste0("-s ",code, " ")
  
  my_thread <- paste0("-c ", threads, " ")
  my_mem    <- paste0("--mem=", memory, "G ")
  my_time   <- paste0("-t ", time, " ")
  my_queue  <- paste0("-p ", queue, ".q ")
  ver_r     <- paste0("-i /FILEPATH.img ")
  
  if (archive == T)      my_archive <- paste0("-C archive ")    else my_archive <- ""
  if (!is.null(project)) my_project <- paste0("-A ", project, " ")   else my_project <- ""
  if (!is.null(output))  my_output  <- paste0(" -o ", output)        else my_output  <- ""
  if (!is.null(error))   my_error   <- paste0(" -e ", error, " ")    else my_error   <- ""
  if (!is.null(args))    my_args    <- paste0(" ", paste(args, collapse = " ")) else my_args <- ""
  
  my_sbatch <- paste0("sbatch ", my_project, my_job, my_output, my_error, my_thread, my_mem, my_time, my_archive, my_queue, my_num, my_shell, ver_r, my_code, my_args)
  print(my_sbatch)
  system(my_sbatch)
  
}
