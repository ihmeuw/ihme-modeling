#####################################################################################################################################################################################
#####################################################################################################################################################################################
#' @Author: 
#' @DateUpdated: 3/1/2019
#' @Description: This helper function creates and submits a qsub command for the new cluster
#' 
#' @param job_name -N name The name of the job.
#' @param project -P project_name Specifies the project to which this job is assigned.
#' @param tasks -t number of jobs within array job
#' @param mem (new cluster) -l m_mem_free = Memory usage from QPID + 10%
#' @param fthread (new cluster) -l fthread = pe_multislot value
#' @param time new cluster -l h_rt = Time your job took on a legacy cluster + 10-30%
#' @param holds -hold_jid list of job names to wait for
#' @param output -o The path used for the standard output stream of the job
#' @param error -e Defines or redefines the path  used  for  the  standard error  stream of the job.
#' @param shell R shell file
#' @param script The job's scriptfile.
#' @param j_archive (new cluster) -l archive = If you need acess to the J:Drive (snfs1)
#' @param args Arguments to the job.
#'
#' @return NULL
#' 
#' @example: qsub(job_name = job_name, project = "proj_custom_models", slots = slots, mem = mem, shell_file = shell_file, script = parallel_script, args = args)
#####################################################################################################################################################################################
#####################################################################################################################################################################################

qsub <- function(job_name = "array_job", 
                     project, 
                     tasks = NULL, 
                     mem = "1G", 
                     fthread = NULL, 
                     time = NULL, 
                     queue = "all.q",
                     holds = NULL, 
                     output = "filepath", 
                     error = "filepath", 
                     shell_file, 
                     script, 
                     j_archive = 1,
                     args = NULL) {
  # check for required arguments
  if (is.null(shell_file)) stop("Did not specify a shell script, please provide one before running again")
  if (is.null(script))     stop("Did not specify a computation script (script parameter), please provide one before running again")
  if (is.null(mem))        stop("Did not specify requested memory, please provide a value for this argument")
  if (is.null(fthread))    stop("Did not specify number of slots, please provide a value for this argument")
  if (is.null(time))       stop("Did not specify requested runtime, please provide a value for this argument")
  
  # format qsub arguments
  my_job                            <- paste("-N", job_name)
  if (!is.null(project)) my_project <- paste("-P", project)           else my_project <- ""
  if (!is.null(tasks))   my_tasks   <- paste("-t", tasks, "-tc 500")  else my_tasks   <- ""
  my_mem                            <- paste0("-l m_mem_free=", mem)
  my_fthread                        <- paste0("-l fthread=", fthread)
  my_time                           <- paste0("-l h_rt=", time)
  if (!is.null(holds))   my_holds   <- paste("-hold_jid", holds)      else my_holds   <- "" 
  if (!is.null(output))  my_output  <- paste("-o", output)            else my_output  <- ""
  if (!is.null(error))   my_error   <- paste("-e", error)             else my_error   <- ""
  my_queue                          <- paste("-q", queue)
  if (j_archive == 1)    my_archive <- paste("-l archive=TRUE")       else my_archive <- ""
  my_shell                          <- paste(shell_file)
  my_code                           <- paste(script)
  if (!is.null(args))    my_args    <- paste(args)                    else my_args    <- ""
  
  my_qsub <- paste("qsub", my_job, my_project, my_tasks, my_mem, my_fthread, my_time, my_holds, my_output, my_error, my_queue, my_archive, my_shell, my_code, my_args)
  print(my_qsub)
  
  # submit job
  system(my_qsub)
}