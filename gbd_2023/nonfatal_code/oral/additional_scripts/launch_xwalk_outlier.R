## ******************************************************************************
##
## Purpose: Launch code which applies crosswalk betas and outliers for specified bundles, and creates
##          an new crosswalk version
## Input:   Bundle ID(s)
## Output:  Launches two qsubs for each specified bundle, one for incidence and one
##          for prevalence. Errors are saved to your sgeoutput folder on the cluster
##
## To use: - Source the function in an R terminal on the fair cluster.
##           Example: source("FILEPATH")
##         - Call launch_agesex_split and enter eiher a single bundle ID, or a vector of bundle IDs
##           using standard R syntax. 
##           Example: launch_agesex_split(c(75,74))
##
##           If you want to launch a job for every bundle in the MNCH map,
##           you can also enter "all" as the parameter.
##
## REQUIRED: If you save your repos somewhere other than FILEPATH,
##           update the filepath on line 54 to point to your repo. Also open
##           FILEPATH and update the filepath on line
##           55 and 56. 
##
## ******************************************************************************


launch_xwalk_outlier <- function(bundles, username = 'USERNAME'){

  
  library(data.table)
  
  
  bundles <- bundles
  measures <- c("prevalence")
  gbd_round_id <- 7
  decomp_step <- 'step2'
  save_dir <- 'FILEPATH'
  out_dir <- 'FILEPATH'
  covariate <- "cv_atchloss4ormore"
  
  #Job specifications
  username <- Sys.getenv("USER")
  m_mem_free <- "-l m_mem_free=100G"
  fthread <- "-l fthread=4"
  runtime_flag <- "-l h_rt=08:00:00"
  jdrive_flag <- "-l archive"
  queue_flag <- "-q long.q"
  shell_script <- "-cwd FILEPATH"
  
  
  ### change to your own repo path if necessary
  script <- paste0(h, "FILEPATH")
  errors_flag <- paste0("-e FILEPATH", username, "FILEPATH")
  outputs_flag <- paste0("-o FILEPATH", username, "FILEPATH")
  
  for (bundle in bundles){
    for (measure in measures){
      job_name <- paste0("-N", " bundle_", bundle, "_", measure, '')
      job <- paste("qsub", m_mem_free, fthread, runtime_flag,
                   jdrive_flag, queue_flag,
                   job_name, "-P proj_nch", jdrive_flag, outputs_flag, 
                   errors_flag, shell_script, script, bundle, measure, covariate,
                   gbd_round_id, decomp_step, save_dir, out_dir)
      
      system(job)
      print(job_name)
    }
  }
}