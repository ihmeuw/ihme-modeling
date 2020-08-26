## ******************************************************************************
##
## Purpose: Launch match mrbrt code for specified bundles
## Input:   Bundle ID(s)
## Output:  Launches a qsubs for each specified bundle. Errors are saved to your sgeoutput folder on the cluster
## Last Update: 5/12/19
##
## To use: - Source the function in an R terminal on the fair cluster.
##         - Call launch_match_mrbrt and enter eiher a single bundle ID, or a vector of bundle IDs
##           using standard R syntax. 
##           Example: launch_match_mrbrt(c(75,74))
##
##           If you want to launch a job for every bundle in the MNCH map,
##           you can also enter "all" as the parameter.
##
##
## ******************************************************************************

launch_match_mrbrt <- function(bundles){
  
  os <- .Platform$OS.type
  if (os == "windows") {
    j <- "J:/"
    h <- "H:/"
  } else {
    j <- "FILEPATH"
    h <- paste0("USERNAME")
  }
  
  library(data.table)
  
  if (bundles == "all") {
    map <- fread("USERNAME")
    bundles <- map$bundle_id
  }
  
  if (bundles == "congenital") {
    map <- fread("USERNAME")
    bundles <- map[type == "congenital", bundle_id]
  }
  
  measures <- c('prevalence')
  trim <- 'trim' # trim or no_trim
  trim_percent <- 0.05
  
  #Job specifications
  username <- Sys.getenv("USER")
    m_mem_free <- "-l m_mem_free=20G"
  fthread <- "-l fthread=2"
  runtime_flag <- "-l h_rt=01:00:00"
  jdrive_flag <- "-l archive"
  queue_flag <- "-q all.q"
  shell_script <- "FILEPATH"
  
  ### change to your own repo path if necessary
  script <- paste0("FILEPATH")
  errors_flag <- paste0("FILEPATH")
  outputs_flag <- paste0("FILEPATH")
  
  for (bundle in bundles){
    for (measure in measures){
      job_name <- paste0("-N", " bundle_", bundle, "_", measure)
      job <- paste("qsub", m_mem_free, fthread, runtime_flag,
                   jdrive_flag, queue_flag,
                   job_name, "-P proj_custom_models", outputs_flag, errors_flag, shell_script, script, bundle, measure, trim, trim_percent)
      
      system(job)
      print(job_name)
    }
  }
}
