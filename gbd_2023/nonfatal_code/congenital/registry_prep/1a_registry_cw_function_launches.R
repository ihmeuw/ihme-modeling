##' ***************************************************************************
##' Title: 1a_registry_cw_function_launches.R
##' Purpose: Various qsubs for the registry crosswalk functions
##' To use: This script can be sourced by 1_full_registry_process.R (numbers correspond with the numbered step in 1_full_registry_process.R,
##'         or can be used to launch qsubs directly from this script.  
##' ***************************************************************************

#1
launch_prep_raw_bundles <- function(raw_registries_to_prep){
  
  os <- .Platform$OS.type
  if (os == "windows") {
    j <- "FILEPATH"
    h <- "FILEPATH"
  } else {
    j <- "FILEPATH"
    h <- "FILEPATH"
  }
  
  library(data.table)
  library(ggplot2)
  library(openxlsx)
  library(dplyr)
  library(stringr)
  library(readxl)
  
  
  #Job specifications
  username <- Sys.getenv("USER")
  m_mem_free <- "-l m_mem_free=15G"
  fthread <- "-l fthread=2"
  runtime_flag <- "-l h_rt=10:00:00"
  jdrive_flag <- "-l archive"
  queue_flag <- "-q long.q"
  shell_script <- "-cwd FILEPATH -s "
  
  
  ### change to your own repo path if necessary
  script <- paste0(h, "FILEPATH")
  errors_flag <- paste0("-e FILEPATH")
  outputs_flag <- paste0("-o FILEPATH")
  
  
  
  for (registry in raw_registries_to_prep){
    job_name <- paste0("-N", " prep_raw_", registry)
    job <- paste("qsub", m_mem_free, fthread, runtime_flag,
                 jdrive_flag, queue_flag,
                 job_name, "-P proj_nch", outputs_flag, errors_flag, shell_script, script, registry)
    
    system(job)
    print(job_name)
  }
}


#1
launch_prep_raw_nzl <- function(){
  
  os <- .Platform$OS.type
  if (os == "windows") {
    j <- "FILEPATH"
    h <- "FILEPATH"
  } else {
    j <- "FILEPATH"
    h <- "FILEPATH"
  }
  
  library(data.table)
  library(ggplot2)
  library(openxlsx)
  library(dplyr)
  library(stringr)
  library(readxl)
  
  
  #Job specifications
  username <- Sys.getenv("USER")
  m_mem_free <- "-l m_mem_free=15G"
  fthread <- "-l fthread=2"
  runtime_flag <- "-l h_rt=10:00:00"
  jdrive_flag <- "-l archive"
  queue_flag <- "-q long.q"
  shell_script <- "-cwd FILEPATH -s "
  
  
  ### change to your own repo path if necessary
  script <- paste0(h, "FILEPATH", "/process_nzl.R")
  errors_flag <- paste0("-e FILEPATH")
  outputs_flag <- paste0("-o FILEPATH")
  
  
  job_name <- paste0("-N", " prep_raw_nzl")
  job <- paste("qsub", m_mem_free, fthread, runtime_flag,
               jdrive_flag, queue_flag,
               job_name, "-P proj_nch", outputs_flag, errors_flag, shell_script, script)
  
  system(job)
  print(job_name)
  
}



#3a
launch_prep_for_cw <- function(source_registries){
  
  os <- .Platform$OS.type
  if (os == "windows") {
    j <- "FILEPATH"
    h <- "FILEPATH"
  } else {
    j <- "FILEPATH"
    h <- "FILEPATH"
  }
  
  library(data.table)
  library(ggplot2)
  library(openxlsx)
  library(dplyr)
  library(stringr)
  library(readxl)
  
  
  #Job specifications
  username <- Sys.getenv("USER")
  m_mem_free <- "-l m_mem_free=5G"
  fthread <- "-l fthread=2"
  runtime_flag <- "-l h_rt=10:00:00"
  jdrive_flag <- "-l archive"
  queue_flag <- "-q long.q"
  shell_script <- "-cwd FILEPATH -s "
  
  
  ### change to your own repo path if necessary
  script <- paste0(h, "FILEPATH")
  errors_flag <- paste0("-e FILEPATH")
  outputs_flag <- paste0("-o FILEPATH")
  functions_flag <- 'prep_for_cw'
  
  
  for (registry in source_registries){
    job_name <- paste0("-N", " prep_for_cw", registry)
    job <- paste("qsub", m_mem_free, fthread, runtime_flag,
                 jdrive_flag, queue_flag,
                 job_name, "-P proj_nch", outputs_flag, errors_flag, shell_script, script, functions_flag, registry)
    
    system(job)
    print(job_name)
  }
}

# 3a 
launch_agg_dummy <- function(source_registries){
  
  os <- .Platform$OS.type
  if (os == "windows") {
    j <- "FILEPATH"
    h <- "FILEPATH"
  } else {
    j <- "FILEPATH"
    h <- "FILEPATH"
  }
  
  library(data.table)
  library(ggplot2)
  library(openxlsx)
  library(dplyr)
  library(stringr)
  library(readxl)
  
  
  #Job specifications
  username <- Sys.getenv("USER")
  m_mem_free <- "-l m_mem_free=5G"
  fthread <- "-l fthread=2"
  runtime_flag <- "-l h_rt=10:00:00"
  jdrive_flag <- "-l archive"
  queue_flag <- "-q long.q"
  shell_script <- "-cwd FILEPATH -s "
  
  ### change to your own repo path if necessary
  script <- paste0(h, "FILEPATH")
  errors_flag <- paste0("-e FILEPATH")
  outputs_flag <- paste0("-o FILEPATH")
  functions_flag <- 'agg_dummy'
  
  
  for (registry in source_registries){
    job_name <- paste0("-N", " agg_", registry)
    job <- paste("qsub", m_mem_free, fthread, runtime_flag,
                 jdrive_flag, queue_flag,
                 job_name, "-P proj_nch", outputs_flag, errors_flag, shell_script, script, functions_flag, registry)
    
    system(job)
    print(job_name)
  }
}

#3b
launch_prep_singapore_for_cw <- function(){
  
  os <- .Platform$OS.type
  if (os == "windows") {
    j <- "FILEPATH"
    h <- "FILEPATH"
  } else {
    j <- "FILEPATH"
    h <- "FILEPATH"
  }
  
  library(data.table)
  library(ggplot2)
  library(openxlsx)
  library(dplyr)
  library(stringr)
  library(readxl)
  
  
  #Job specifications
  username <- Sys.getenv("USER")
  m_mem_free <- "-l m_mem_free=10G"
  fthread <- "-l fthread=2"
  runtime_flag <- "-l h_rt=10:00:00"
  jdrive_flag <- "-l archive"
  queue_flag <- "-q long.q"
  shell_script <- "-cwd FILEPATH -s "
  
  
  ### change to your own repo path if necessary
  script <- paste0(h, "FILEPATH")
  errors_flag <- paste0("-e FILEPATH")
  outputs_flag <- paste0("-o FILEPATH")
  functions_flag <- 'prep_singapore_for_cw'
  
  
  job_name <- paste0("-N", " prep_singapore_for_cw")
  job <- paste("qsub", m_mem_free, fthread, runtime_flag,
               jdrive_flag, queue_flag,
               job_name, "-P proj_nch", outputs_flag, errors_flag, shell_script, script, functions_flag)
  
  system(job)
  print(job_name)
  
}

#3b
launch_aggregate_singapore <- function(){
  
  os <- .Platform$OS.type
  if (os == "windows") {
    j <- "FILEPATH"
    h <- "FILEPATH"
  } else {
    j <- "FILEPATH"
    h <- "FILEPATH"
  }
  
  library(data.table)
  library(ggplot2)
  library(openxlsx)
  library(dplyr)
  library(stringr)
  library(readxl)
  
  
  #Job specifications
  username <- Sys.getenv("USER")
  m_mem_free <- "-l m_mem_free=10G"
  fthread <- "-l fthread=2"
  runtime_flag <- "-l h_rt=10:00:00"
  jdrive_flag <- "-l archive"
  queue_flag <- "-q long.q"
  shell_script <- "-cwd FILEPATH -s "
  
  
  ### change to your own repo path if necessary
  script <- paste0(h, "FILEPATH")
  errors_flag <- paste0("-e FILEPATH")
  outputs_flag <- paste0("-o FILEPATH")
  functions_flag <- 'aggregate_singapore'
  
  
  job_name <- paste0("-N", " agg_sinapore")
  job <- paste("qsub", m_mem_free, fthread, runtime_flag,
               jdrive_flag, queue_flag,
               job_name, "-P proj_nch", outputs_flag, errors_flag, shell_script, script, functions_flag)
  
  system(job)
  print(job_name)
  
}

#4a
launch_crosswalks <- function(target_registries){
  
  os <- .Platform$OS.type
  if (os == "windows") {
    j <- "FILEPATH"
    h <- "FILEPATH"
  } else {
    j <- "FILEPATH"
    h <- "FILEPATH"
  }
  
  library(data.table)
  library(ggplot2)
  library(openxlsx)
  library(dplyr)
  library(stringr)
  library(readxl)
  
  library(reticulate)
  library(crosswalk, lib.loc = "FILEPATH")
  
  
  #Job specifications
  username <- Sys.getenv("USER")
  m_mem_free <- "-l m_mem_free=50G"
  fthread <- "-l fthread=2"
  runtime_flag <- "-l h_rt=10:00:00"
  jdrive_flag <- "-l archive"
  queue_flag <- "-q long.q"
  shell_script <- "-cwd FILEPATH -s "
  
  ### change to your own repo path if necessary
  script <- paste0(h, "FILEPATH")
  errors_flag <- paste0("-e FILEPATH")
  outputs_flag <- paste0("-o FILEPATH")
  functions_flag <- 'crosswalk'
  
  for (registry in target_registries){
    job_name <- paste0("-N", " crosswalk_", registry)
    job <- paste("qsub", m_mem_free, fthread, runtime_flag,
                 jdrive_flag, queue_flag,
                 job_name, "-P proj_nch", outputs_flag, errors_flag, shell_script, script, functions_flag, registry)
    
    system(job)
    print(job_name)
  }
}

#5
launch_aggregate_raw <- function(congenital_registries){
  
  os <- .Platform$OS.type
  if (os == "windows") {
    j <- "FILEPATH"
    h <- "FILEPATH"
  } else {
    j <- "FILEPATH"
    h <- "FILEPATH"
  }
  
  library(data.table)
  library(ggplot2)
  library(openxlsx)
  library(dplyr)
  library(stringr)
  library(readxl)
  
  
  #Job specifications
  username <- Sys.getenv("USER")
  m_mem_free <- "-l m_mem_free=10G"
  fthread <- "-l fthread=2"
  runtime_flag <- "-l h_rt=10:00:00"
  jdrive_flag <- "-l archive"
  queue_flag <- "-q long.q"
  shell_script <- "-cwd FILEPATH -s "
  
  ### change to your own repo path if necessary
  script <- paste0(h, "FILEPATH")
  errors_flag <- paste0("-e FILEPATH")
  outputs_flag <- paste0("-o FILEPATH")
  functions_flag <- 'aggregate_raw'
  
  
  for (registry in congenital_registries){
    job_name <- paste0("-N", " aggregate_raw_", registry)
    job <- paste("qsub", m_mem_free, fthread, runtime_flag,
                 jdrive_flag, queue_flag,
                 job_name, "-P proj_nch", outputs_flag, errors_flag, shell_script, script, functions_flag, registry)
    
    system(job)
    print(job_name)
  }
}

#5
launch_aggregate_sing_lvl2 <- function(){
  
  os <- .Platform$OS.type
  if (os == "windows") {
    j <- "FILEPATH"
    h <- "FILEPATH"
  } else {
    j <- "FILEPATH"
    h <- "FILEPATH"
  }
  
  library(data.table)
  library(ggplot2)
  library(openxlsx)
  library(dplyr)
  library(stringr)
  library(readxl)
  
  
  #Job specifications
  username <- Sys.getenv("USER")
  m_mem_free <- "-l m_mem_free=10G"
  fthread <- "-l fthread=2"
  runtime_flag <- "-l h_rt=10:00:00"
  jdrive_flag <- "-l archive"
  queue_flag <- "-q long.q"
  shell_script <- "-cwd FILEPATH -s "
  
  ### change to your own repo path if necessary
  script <- paste0(h, "FILEPATH")
  errors_flag <- paste0("-e FILEPATH")
  outputs_flag <- paste0("-o FILEPATH")
  functions_flag <- 'aggregate_sing_lvl2'
  
  
  job_name <- paste0("-N", " aggregate_sing_lvl2")
  job <- paste("qsub", m_mem_free, fthread, runtime_flag,
               jdrive_flag, queue_flag,
               job_name, "-P proj_nch", outputs_flag, errors_flag, shell_script, script, functions_flag)
  
  system(job)
  print(job_name)
}

#6
launch_apply_betas <- function(congenital_registries, registry_bundles){
  
  os <- .Platform$OS.type
  if (os == "windows") {
    j <- "FILEPATH"
    h <- "FILEPATH"
  } else {
    j <- "FILEPATH"
    h <- "FILEPATH"
  }
  
  library(data.table)
  library(ggplot2)
  library(openxlsx)
  library(dplyr)
  library(stringr)
  library(readxl)
  
  #Job specifications
  username <- Sys.getenv("USER")
  m_mem_free <- "-l m_mem_free=10G"
  fthread <- "-l fthread=2"
  runtime_flag <- "-l h_rt=10:00:00"
  jdrive_flag <- "-l archive"
  queue_flag <- "-q long.q"
  shell_script <- "-cwd FILEPATH -s "
  
  ### change to your own repo path if necessary
  script <- paste0(h, "FILEPATH")
  errors_flag <- paste0("-e FILEPATH")
  outputs_flag <- paste0("-o FILEPATH")
  functions_flag <- 'apply_betas'
  
  
  for (registry in congenital_registries){
    job_name <- paste0("-N", " apply_betas_", registry)
    job <- paste("qsub", m_mem_free, fthread, runtime_flag,
                 jdrive_flag, queue_flag,
                 job_name, "-P proj_nch", outputs_flag, errors_flag, shell_script, script, functions_flag, registry, registry_bundles)
    
    system(job)
    print(job_name)
  }
}

#6
launch_assemble_bundles <- function(congenital_registries, bundles){
  
  os <- .Platform$OS.type
  if (os == "windows") {
    j <- "FILEPATH"
    h <- "FILEPATH"
  } else {
    j <- "FILEPATH"
    h <- "FILEPATH"
  }
  
  library(data.table)
  library(ggplot2)
  library(openxlsx)
  library(dplyr)
  library(stringr)
  library(readxl)
  
  
  
  #Job specifications
  username <- Sys.getenv("USER")
  m_mem_free <- "-l m_mem_free=10G"
  fthread <- "-l fthread=2"
  runtime_flag <- "-l h_rt=10:00:00"
  jdrive_flag <- "-l archive"
  queue_flag <- "-q long.q"
  shell_script <- "-cwd FILEPATH -s "
  
  ### change to your own repo path if necessary
  script <- paste0(h, "FILEPATH")
  errors_flag <- paste0("-e FILEPATH")
  outputs_flag <- paste0("-o FILEPATH")
  functions_flag <- 'assemble_bundles'
  
  
  for (bun in bundles){
    job_name <- paste0("-N", " assemble_bundle_", bun)
    job <- paste("qsub", m_mem_free, fthread, runtime_flag,
                 jdrive_flag, queue_flag,
                 job_name, "-P proj_nch", outputs_flag, errors_flag, shell_script, script, functions_flag, congenital_registries, bun)
    
    system(job)
    print(job_name)
  }
}

#7
launch_pdf_diagnostics <- function(bundles){
  
  os <- .Platform$OS.type
  if (os == "windows") {
    j <- "FILEPATH"
    h <- "FILEPATH"
  } else {
    j <- "FILEPATH"
    h <- "FILEPATH"
  }
  
  library(data.table)
  library(ggplot2)
  library(openxlsx)
  library(dplyr)
  library(stringr)
  library(readxl)
  
  
  
  #Job specifications
  username <- Sys.getenv("USER")
  m_mem_free <- "-l m_mem_free=10G"
  fthread <- "-l fthread=2"
  runtime_flag <- "-l h_rt=10:00:00"
  jdrive_flag <- "-l archive"
  queue_flag <- "-q long.q"
  shell_script <- "-cwd FILEPATH -s "
  
  ### change to your own repo path if necessary
  script <- paste0(h, "FILEPATH")
  errors_flag <- paste0("-e FILEPATH")
  outputs_flag <- paste0("-o FILEPATH")
  functions_flag <- 'pdf_diagnostics'
  
  
  for (pdf_bun in bundles){
    job_name <- paste0("-N", " pdf_", pdf_bun)
    job <- paste("qsub", m_mem_free, fthread, runtime_flag,
                 jdrive_flag, queue_flag,
                 job_name, "-P proj_nch", outputs_flag, errors_flag, shell_script, script, functions_flag, pdf_bun)
    
    system(job)
    print(job_name)
  }
}


#8
launch_wipe_bundle_data <- function(bundles, archive, decomp_step, gbd_round_id){

  os <- .Platform$OS.type
  if (os == "windows") {
    j <- "FILEPATH"
    h <- "FILEPATH"
  } else {
    j <- "FILEPATH"
    h <- "FILEPATH"
  }
  
  library(data.table)
  library(ggplot2)
  library(openxlsx)
  library(dplyr)
  library(stringr)
  library(readxl)
  
  source("FILEPATH")
  
  #Job specifications
  username <- Sys.getenv("USER")
  m_mem_free <- "-l m_mem_free=15G"
  fthread <- "-l fthread=2"
  runtime_flag <- "-l h_rt=10:00:00"
  jdrive_flag <- "-l archive"
  queue_flag <- "-q long.q"
  shell_script <- "-cwd FILEPATH -s "
  
  ### change to your own repo path if necessary
  script <- paste0(h, "FILEPATH")
  errors_flag <- paste0("-e FILEPATH")
  outputs_flag <- paste0("-o FILEPATH")
  functions_flag <- 'wipe_bundle_data'
  
  
  for (bun in bundles){
    job_name <- paste0("-N", " wipe_", bun)
    job <- paste("qsub", m_mem_free, fthread, runtime_flag,
                 jdrive_flag, queue_flag,
                 job_name, "-P proj_nch", outputs_flag, errors_flag, shell_script, script, functions_flag, bun, archive, decomp_step, gbd_round_id)
    
    system(job)
    print(job_name)
  }
}





