## ******************************************************************************
##
## Purpose: Launch age/sex plotting code for specified bundles
## Input:   Bundle ID(s)
## Output:  Launches two qsubs for each specified bundle, one for incidence and one
##          for prevalence. Errors are saved to your sgeoutput folder on the cluster
## Last Update: 5/3/19
##
## To use: - Source the function in an R terminal on the fair cluster.
##         - Call launch_agesex_dx_plot and enter eiher a single bundle ID, or a vector of bundle IDs
##           using standard R syntax. 
##           Example: launch_agesex_dx_split(c(75,74))
##
##           You can also enter "all" or "congenital" in place of a list of bundle IDs
##
##
## ******************************************************************************

launch_save_get_bundle <- function(bundles){
  
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
    map <- fread("FILEPATH")
    bundles <- map$bundle_id
  }
  
  if (bundles == "congenital") {
    map <- fread("FILEPATH")
    bundles <- map[type == "congenital", bundle_id]
  }
  
  
  #Job specifications
  username <- Sys.getenv("USER")
  m_mem_free <- "-l m_mem_free=4G"
  fthread <- "-l fthread=2"
  runtime_flag <- "-l h_rt=00:20:00"
  jdrive_flag <- "-l archive"
  queue_flag <- "-q all.q"
  shell_script <- "FILEPATH"
  
  ### change to your own repo path if necessary
  script <- paste0("FILEPATH")
  errors_flag <- paste0("FILEPATH")
  outputs_flag <- paste0("FILEPATH")
  
  for (bundle in bundles){
    job_name <- paste0("-N", " bundle_", bundle)
    job <- paste("qsub", m_mem_free, fthread, runtime_flag,
                  jdrive_flag, queue_flag,
                  job_name, "-P proj_custom_models", outputs_flag, errors_flag, shell_script, script, bundle)
      
    system(job)
    print(job_name)
  }
  
}
    
    


  
