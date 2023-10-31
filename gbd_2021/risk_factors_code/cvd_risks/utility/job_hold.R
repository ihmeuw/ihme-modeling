## Purpose: holds second job until the first one finishes

job_hold <- function(job_name, file_list=NULL, obj=NULL) {
  
  ## Give it a sec to launch
  Sys.sleep(5)
  
  ## Start timer
  start.time <- proc.time()
  
  ## Wait for job to finish
  flag <-  0
  while (flag == 0) {
    ## Check if job is done
    if (system(paste0("qstat -r | grep ", job_name, "|wc -l"), intern=T) == 0) {
      ## If so, set flag to 1
      flag <- 1
    } else {
      Sys.sleep(5)
    }
  }
  
  ## End Timer
  job.runtime <- proc.time() - start.time
  job.runtime <- job.runtime[3]
  
  ## Give it another sec
  Sys.sleep(10)
  
  
  ## Check for the file list
  if (!is.null(file_list)) {
    missing_list <- NULL
    for (file in file_list) {
      ## Ensure that all files are there
      if (!file.exists(file)) {
        missing_list <- rbind(missing_list, file)
        ## Check obj if hdf
      } else {
        if (grepl(".h5", file_list[1])) {
          if (!(obj %in% h5ls(file_list)$name)) {
            missing_list <- rbind(missing_list, file)
          }
        }
      }
    }
    
    ## If missing_list > 0, break
    if (length(missing_list) > 0) {
      stop(paste0("Job failed: ", job_name,
                  "\nTime elapsed: ", job.runtime,
                  "\nYou are missing the following files: ", toString(missing_list)))
    }
  }
  
  ## Complete
  print(paste0("Job ", job_name, " has completed. Time elapsed: ", job.runtime))
  
}