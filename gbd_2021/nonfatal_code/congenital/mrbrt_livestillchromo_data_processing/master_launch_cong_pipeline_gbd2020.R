## ******************************************************************************
##
## Purpose: Master launch script for congenital pipeline
## Input:   Bundle ID(s), 
## Output:  Launches a qsubs for entire congenital pipeline post-registry processing
##
##
## ******************************************************************************


launch_congenital_pipeline <- function(bun_ids, step = "iterative", bun_ver_creation = FALSE, run_match_mrbrt = FALSE, model_description, gbd_round_id= 7, crosswalked=TRUE){
  
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
  
  #Job specifications
  username <- Sys.getenv("USER")
  m_mem_free <- "-l m_mem_free=50G"
  fthread <- "-l fthread=2"
  runtime_flag <- "-l h_rt=:30:00"
  jdrive_flag <- "-l archive"
  queue_flag <- "-q all.q"
  shell_script <- "-cwd FILEPATH/r_shell_ide.sh"
  
  ### change to your own repo path if necessary
  script <- paste0(h, "FILEPATH/3_save_bun_ver_and_launch.R")
  errors_flag <- paste0("-e FILEPATH", Sys.getenv("USER"), "/errors")
  outputs_flag <- paste0("-o FILEPATH", Sys.getenv("USER"), "/output")
  
  for (bun_id in bun_ids){
    job_name <- paste0("-N", " cong_pipeline_master_", bun_id)
    job <- paste("qsub", m_mem_free, fthread, runtime_flag,
                 jdrive_flag, queue_flag,
                 job_name, "-P proj_nch", outputs_flag, errors_flag, shell_script, script, bun_id, step, bun_ver_creation, run_match_mrbrt, model_description, gbd_round_id, crosswalked)
    
    system(job)
    print(job_name)
  }
}