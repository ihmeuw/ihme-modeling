#####################################################################################################################################################################################
#####################################################################################################################################################################################
#' @Author: 
#' @DateUpdated: 12/17/2018
#' @Description: This helper function creates and submits a qsub command.
#' 
#' @param job_name -N name The name of the job.
#' @param project -P project_name Specifies the project to which this  job  is  assigned.
#' @param slots -pe multi_slot The number of slots requested for the job in a parallel environment.
#' @param mem -l mem_free= Specifies the amount of memory requested for the job.
#' @param output -o The path used for the standard output stream of the job.
#' @param error -e Defines or redefines the path  used  for  the  standard error  stream of the job.
#' @param shell R shell file
#' @param script The job's scriptfile.
#' @param args Arguments to the job.
#'
#' @return NULL
#' 
#' @Example: qsub(job_name = job_name, project = "proj_custom_models", slots = slots, mem = mem, shell_file = shell_file, script = parallel_script, args = args)
#####################################################################################################################################################################################
#####################################################################################################################################################################################

qsub <- function(job_name = "array_job", project, tasks = NULL, slots = 1, mem = NULL, holds = NULL, output = "filepath", error = "filepath", shell_file, script, args = NULL) {
  if (is.null(shell_file))   stop("Did not specify a shell script, please provide one before running again")
  if (is.null(script))       stop("Did not specify a computation script (script parameter), please provide one before running again")
  
  my_job                            <- paste("-N", job_name)
  if (!is.null(project)) my_project <- paste("-P", project)           else my_project <- ""
  if (!is.null(tasks))   my_tasks   <- paste("-t", tasks)             else my_tasks   <- ""
  if (slots > 1)         my_slots   <- paste("-pe multi_slot", slots) else my_slots   <- ""
  if (!is.null(holds))   my_holds   <- paste("-hold_jid", holds)      else my_holds   <- "" 
  if (!is.null(output))  my_output  <- paste("-o", output)            else my_output  <- ""
  if (!is.null(error))   my_error   <- paste("-e", error)             else my_error   <- ""
  my_shell                          <- paste(shell_file)
  my_code                           <- paste(script)
  if (!is.null(args))    my_args    <- paste(args)                    else my_args    <- ""
  
  my_qsub <- paste("qsub", my_job, my_project, my_tasks, my_slots, my_holds, my_output, my_error, my_shell, my_code, my_args)
  print(my_qsub)
  system(my_qsub)
}