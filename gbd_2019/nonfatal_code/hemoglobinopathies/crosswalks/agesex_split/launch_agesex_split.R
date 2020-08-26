## ******************************************************************************
##
## Purpose: Launch age/sex splitting code for specified bundles
## Input:   Bundle ID(s)
## Output:  Launches two qsubs for each specified bundle, one for incidence and one
##          for prevalence. Errors are saved to your sgeoutput folder on the cluster
##
## To use: - Source the function in an R terminal on the fair cluster.
##         - Call launch_agesex_split and enter eiher a single bundle ID, or a vector of bundle IDs
##           using standard R syntax. 
##           Example: launch_agesex_split(c(75,74))
##
##           If you want to launch a job for every bundle in the MNCH map,
##           you can also enter "all" as the parameter.
##
##
## ******************************************************************************

launch_agesex_split <- function(bundles, username = 'USERNAME'){
  
  os <- .Platform$OS.type
  if (os == "windows") {
    j <- "J:/"
    h <- "H:/"
  } else {
    j <- "FILEPATH"
    h <- paste0("FILEPATH")
  }
  
  library(data.table)
  
  if (bundles == "all") {
    map <- fread("FILEPATH")
    bundles <- map$bundle_id
  }
  
  if (bundles == "congenital") {
    map <- fread("FILEPATH")
    bundles <- map[type == "congenital", bundle_id]
  }
  
  measures <- c("prevalence", "incidence")
  
  #Job specifications
  #username <- Sys.getenv("USER")
  m_mem_free <- "-l m_mem_free=45G"
  fthread <- "-l fthread=4"
  runtime_flag <- "-l h_rt=01:00:00"
  jdrive_flag <- "-l archive"
  queue_flag <- "-q long.q"
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
                    job_name, "-P proj_nch", jdrive_flag, outputs_flag, errors_flag, shell_script, script, bundle, measure)
      
      system(job)
      print(job_name)
    }
  }
}
    



  
