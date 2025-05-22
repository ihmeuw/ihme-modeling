## ******************************************************************************
## Purpose: Launch age/sex splitting code for specified bundles
## Input:   Bundle ID(s)
## Output:  Launches a qsub for every specified bundle, measure, and bundle version.
## ******************************************************************************

launch_agesex_split <- function(bundles, cascade){
  
  os <- .Platform$OS.type
  if (os == "windows") {
    j <- "FILEPATH"
    h <- "FILEPATH"
  } else {
    j <- "FILEPATH"
    h <- "FILEPATH"
  }
  
  library(data.table)
  library(dplyr)
  '%ni%' <- Negate('%in%')
  
  #bundles <- c(338,261)
  #bundles <- 'congenital'
  bundles <- bundles
  
  if (bundles == "all") {
    map <- fread("FILEPATH")
    bundles <- map$bundle_id
  }
  
  if (bundles == "congenital") {
    map <- fread("FILEPATH")
    map <- map[bundle_id %ni% c(439, 2972, 2975, 3776, 3779, 2978)]
    bundles <- map[type == "congenital", bundle_id]
  }
  
  measures <- c("prevalence")
  step <- 'iterative'
  types <- 'dismod'
  inputs <- data.table(bundle_id = bundles, measure = measures, type = types)
  
  
  #Job specifications
  username <- Sys.getenv("USER")
  m_mem_free <- "-l m_mem_free=50G"
  fthread <- "-l fthread=4"
  runtime_flag <- "-l h_rt=04:00:00"
  jdrive_flag <- "-l archive"
  queue_flag <- "-q all.q"
  shell_script <- "-cwd FILEPATH"
  
  ### change to your own repo path if necessary
  script <- paste0(h, "FILEPATH", "do_agesex_split.R")
  errors_flag <- paste0("-e FILEPATH")
  outputs_flag <- paste0("-o FILEPATH")
  
  
  for (row_i in 1:nrow(inputs)) {
    bundle <- inputs[row_i, bundle_id]
    measure <- inputs[row_i, measure]
    bv <- fread(paste0("FILEPATH")) %>% as.data.table
    bv_id <- bv$bundle_version_id
    type <- inputs[row_i, type]
    
    job_name <- paste0("-N", " log_", bundle, "_", measure)
    job <- paste("qsub", m_mem_free, fthread, runtime_flag,
                 jdrive_flag, queue_flag,
                 job_name, "-P proj_nch", outputs_flag, errors_flag, shell_script, script,
                 bundle, measure, bv_id, type, cascade)
    
    
    system(job)
    print(job_name)
  }
}





