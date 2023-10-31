## ******************************************************************************
## Purpose: Launch script to apply crosswalk ratios and upload data 
## Input:   Bundle ID(s)
## Output:  Launches a qsubs for each specified bundle. Errors are saved to your sgeoutput folder on the cluster
## ******************************************************************************

launch_ratio_application <- function(bun_ids, step="iterative", model_description){
  
  os <- .Platform$OS.type
  if (os == "windows") {
    j <- "FILEPATH"
    h <- "FILEPATH"
  } else {
    j <- "FILEPATH"
    h <- paste0("FILEPATH", Sys.getenv("USER"),"/")
  }
  
  library(data.table)
  require(DBI, lib.loc="FILEPATH/rlibs")
  require(RMySQL)
  library(openxlsx)
  library(scales)
  library("jsonlite")
  
  
  map <- fread("FILEPATH/all_me_bundle.csv")
  map <- map[type=="congenital"]

  #Job specifications
  username <- Sys.getenv("USER")
  m_mem_free <- "-l m_mem_free=300G"
  fthread <- "-l fthread=2"
  runtime_flag <- "-l h_rt=10:00:00"
  jdrive_flag <- "-l archive"
  queue_flag <- "-q long.q"
  shell_script <- "-cwd FILEPATH/r_shell_ide.sh"
  
  ### change to your own repo path if necessary
  script <- paste0("FILEPATH/apply_ratios_upload_xwalk_version.R")
  errors_flag <- paste0("-e FILEPATH", Sys.getenv("USER"), "/errors")
  outputs_flag <- paste0("-o FILEPATH", Sys.getenv("USER"), "/output")
  
  for (bun_id in bun_ids){
      job_name <- paste0("-N", " ratio_bundle_", bun_id, step)
      job <- paste("qsub", m_mem_free, fthread, runtime_flag,
                   jdrive_flag, queue_flag,
                   job_name, "-P proj_nch", outputs_flag, errors_flag, shell_script, script, bun_id, step, model_description)
      
      system(job)
      print(job_name)
    }
  }



