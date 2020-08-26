## ******************************************************************************
##
## Purpose: Launch age/sex splitting code for specified bundles
## Input:   Bundle ID(s)
## Output:  Launches two qsubs for each specified bundle, one for incidence and one
##          for prevalence. Errors are saved to your sgeoutput folder on the cluster
## Output:  Launches a qsub for every specified bundle, measure, and bundle version.
##          Errors are saved to your sgeoutput folder on the cluster
## Author:  NAME and NAME
## Last Update: DATE
## Last Update: DATE
##
## To use: - Source the function in an R terminal on the cluster.
##           Example: source("FILEPATH")
##         - Call launch_agesex_split and enter either a single bundle ID, or a vector of bundle IDs
##           using standard R syntax. 
##           Example: launch_agesex_split(c(bundle_id,bundle_id))
## To use:   Enter either a single bundle ID, or a vector of bundle IDs
##
##           If you want to launch a job for every bundle in the MNCH map,
##           you can also enter "all" as the parameter.
##
## REQUIRED: If you save your repos somewhere other than FILEPATH,
##           update the filepath on line 54 to point to your repo. Also open
##          FILEPATH and update the filepath on line
##           55 and 56. 
##           update the filepath on line 61 to point to your repo. Also open
##           FILEPATH and update the repo filepath there.
##
## ******************************************************************************
rm(list=ls())
launch_agesex_split <- function(bundles){
  
  os <- .Platform$OS.type
  if (os == "windows") {
    j <- "J:/"
    h <- "H:/"
  } else {
    j <- "/home/j/"
    h <- paste0("/homes/", Sys.getenv("USER"),"/")
  }
  
  library(data.table)
  
  if (bundles == "all") {
    map <- fread("FILEPATH")
    bundles <- map$bundle_id
  }
  
  if (bundles == "cause") {
    map <- fread("FILEPATH")
    bundles <- map[type == "cause", bundle_id]
  }
  
  measures <- c("prevalence")
  # measures <- c("proportion")
  #measures <- c("prevalence", "incidence")
  
  #Job specifications
  username <- Sys.getenv("USER")
  m_mem_free <- "-l m_mem_free=60G"
  fthread <- "-l fthread=4"
  runtime_flag <- "-l h_rt=04:00:00"
  jdrive_flag <- "-l archive"
  queue_flag <- "-q long.q"
  shell_script <- "-cwd FILEPATH"
  
  ### change to your own repo path if necessary
  #script <- paste0(h, FILEPATH")
  script <- paste0(h, "FILEPATH")
  errors_flag <- paste0("-e FILEPATH", Sys.getenv("USER"), "FILEPATH")
  outputs_flag <- paste0("-o FILEPATH", Sys.getenv("USER"), "FILEPATH")
  
  bundles <- c(bundel_id)
  bv_id <- bundle_version_id

os <- .Platform$OS.type
if (os == "windows") {
  j <- "J:/"
  h <- "H:/"
} else {
  j <- "/home/j/"
  h <- paste0("/homes/", Sys.getenv("USER"),"/")
}

library(data.table)

bundles <- c(bundle_id, bundle_id)

if (bundles == "all") {
  map <- fread("FILEPATH")
  bundles <- map$bundle_id
}

if (bundles == "cause") {
  map <- fread("FILEPATH")
  bundles <- map[type == "cause", bundle_id]
}

measures <- c("prevalence")
bvs <- c(bundle_version_id, bundle_version_id)
types <- 'dismod'
inputs <- data.table(bundle_id = bundles, measure = measures, bv_id = bvs, type = types)


#Job specifications
username <- Sys.getenv("USER")
m_mem_free <- "-l m_mem_free=40G"
fthread <- "-l fthread=4"
runtime_flag <- "-l h_rt=04:00:00"
jdrive_flag <- "-l archive"
queue_flag <- "-q long.q"
shell_script <- "-cwd FILEPATH"

### change to your own repo path if necessary
script <- paste0(h, "FILEPATH")
errors_flag <- paste0("-e FILEPATH", Sys.getenv("USER"), "FILEPATH")
outputs_flag <- paste0("-o FILEPATH", Sys.getenv("USER"), "FILEPATH")


for (row_i in 1:nrow(inputs)) {
  bundle <- inputs[row_i, bundle_id]
  measure <- inputs[row_i, measure]
  bv_id <- inputs[row_i, bv_id]
  type <- inputs[row_i, type]
  
  bundles <- c(bundle_id)
  bv_id <- bundle_version_id
  job_name <- paste0("-N", " log_", bundle, "_", measure)
  job <- paste("qsub", m_mem_free, fthread, runtime_flag,
               jdrive_flag, queue_flag,
               job_name, "-P PROJECT", outputs_flag, errors_flag, shell_script, script, 
               bundle, measure, bv_id, type)
  
  bundles <- c(bundle_id)
  bv_id <- bundle_version_id

  for (bundle in bundles){
    for (measure in measures){
      job_name <- paste0("-N", " log_", bundle, "_", measure)
      job <- paste("qsub", m_mem_free, fthread, runtime_flag,
                    jdrive_flag, queue_flag,
                    job_name, "-P PROJECT", outputs_flag, errors_flag, shell_script, script, bundle, measure, bv_id)
      
      system(job)
      print(job_name)
    }
  }
  system(job)
  print(job_name)
}
