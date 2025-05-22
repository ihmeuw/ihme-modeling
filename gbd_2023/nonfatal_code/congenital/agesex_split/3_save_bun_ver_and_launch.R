## ******************************************************************************
## Purpose: The purpose of this script is to: 
##    1. create bundle versions or pull existing bundle versions
##    2. launch all subsequent scripts in pipeline
## ******************************************************************************


launch_diagnostics <- function(bun_id, crosswalked, hold_string){
  
  os <- .Platform$OS.type
  if (os == "windows") {
    j <- "FILEPATH"
    h <- "FILEPATH"
  } else {
    j <- "FILEPATH"
    h <- "FILEPATH"
  }
  
  #Job specifications
  username <- Sys.getenv("USER")
  m_mem_free <- "-l m_mem_free=30G"
  fthread <- "-l fthread=2"
  runtime_flag <- "-l h_rt=10:00:00"
  jdrive_flag <- "-l archive"
  queue_flag <- "-q long.q"
  shell_script <- "-cwd FILEPATH"
  
  ### change to your own repo path if necessary
  script <- paste0(h, "FILEPATH")
  errors_flag <- paste0("-e FILEPATH")
  outputs_flag <- paste0("-o FILEPATH")
  hold_flag <- ""
  if (!missing(hold_string)){
    hold_flag <- paste0(" -hold_jid ",hold_string, " ")
  }
  
  job_name <- paste0("-N", " diagnostics_", bun_id)
  job <- paste("qsub", m_mem_free, fthread, runtime_flag,
               jdrive_flag, queue_flag, hold_flag,
               job_name, "-P proj_nch", outputs_flag, errors_flag, shell_script, script, bun_id, crosswalked)
  system(job)
}

launch_pdf_append <- function(arg_a, arg_b, hold_string){
  
  os <- .Platform$OS.type
  if (os == "windows") {
    j <- "FILEPATH"
    h <- "FILEPATH"
  } else {
    j <- "FILEPATH"
    h <- "FILEPATH"
  }
  
  #Job specifications
  username <- Sys.getenv("USER")
  m_mem_free <- "-l m_mem_free=30G"
  fthread <- "-l fthread=2"
  runtime_flag <- "-l h_rt=10:00:00"
  jdrive_flag <- "-l archive"
  queue_flag <- "-q long.q"
  shell_script <- "-cwd FILEPATH"
  
  ### change to your own repo path if necessary
  script <- paste0(h, "FILEPATH")
  errors_flag <- paste0("-e FILEPATH")
  outputs_flag <- paste0("-o FILEPATH")
  hold_flag <- ""
  if (!missing(hold_string)){
    hold_flag <- paste0(" -hold_jid ",hold_string, " ")
  }
  
  job_name <- paste0("-N", " pdf_append")
  job <- paste("qsub", m_mem_free, fthread, runtime_flag,
               jdrive_flag, queue_flag, hold_flag,
               job_name, "-P proj_nch", outputs_flag, errors_flag, shell_script, script, arg_a, arg_b)
  system(job)
}
#Script content
Sys.umask(mode = 002)

os <- .Platform$OS.type
if (os == "windows") {
  j <- "FILEPATH"
  h <- "FILEPATH"
} else {
  j <- "FILEPATH"
  h <- "FILEPATH"
}

source("FILEPATH")

library(data.table)
library(openxlsx)
library(readxl)
args <- commandArgs(trailingOnly = TRUE)
bun_id <- args[1]
step <- args[2]
bun_ver_creation <- args[3]
run_match_mrbrt <- args[4]
model_description <- args[5]
gbd_round_id <- args[6]
crosswalked <- args[7]

measure_name <- "prevalence"
measure <- "prevalence"

invisible(lapply(list.files("FILEPATH", full.names = T), function(x){  source(x)  }))


## EUROCAT NIDs: 
nids<-list(159930, 159937, 128835 , 159941, 163937, 159924, 159938, 159927, 
           159926, 159927, 159928, 159929, 159931, 159932, 159933, 159934, 
           159935, 159936, 159939, 159940, 159942, 163938, 163939, 159925, 128835)
heart_buns <- c(628,630,632,634,636)
################################################################################################################################################################################
# load all original bundle data

if(bun_ver_creation==TRUE){
  print(bun_id)
  bun_ver <- save_bundle_version(bundle_id=bun_id, decomp_step = step, gbd_round_id = 7, include_clinical = c('inpatient', 'claims'))
  b_v <- bun_ver$bundle_version_id
  bv <- data.frame(bundle_id=bun_id, bundle_version_id=b_v)
  write.csv(bv, paste0("FILEPATH", step, "_", bun_id, "_", Sys.Date(), ".csv"))
  write.csv(bv, paste0("FILEPATH", step, "_", bun_id, "_CURRENT.csv"))
}

if(bun_ver_creation==FALSE){
  print("Age sex splitting will query latest bundle version that exists")
}


#######################################################################
# Step 1: Launch age sex splitting on bundle data: 
source(paste0(h, "/repos/crosswalks/agesex_split/launch_agesex_split.R"))
message("sourced age sex split")
# Apply under 1 aggregation cascade for bundles on rotation.
if(bun_id %in% heart_buns){
  launch_agesex_split(bundles = bun_id, cascade = TRUE)} else{
    launch_agesex_split(bundles = bun_id, cascade = FALSE)
  }


# Input: bundle version, which it pulls from the CSV pulled above
# Output: Flat file of age sex split data
# Note: This automatically launches the diagnostics 

#######################################################################
agesexsplit_job_names <- paste0("'log_", bun_id, "_*'")
mrbrt_job_names <- ""
ratio_job_name <- paste0("'ratio_bundle_", bun_id, "*'")
diagnostics_job_name <- paste0("diagnostics_", bun_id)

#######################################################################
# Step 2: If we want to rerun crosswalks, run this section: 
# Cannot run this in a qsub due to the fact that the crosswalk packages requires interactive running#
if(run_match_mrbrt==TRUE){
  launch_match_mrbrt_congenital(bun_id, "cv_livestill", agesexsplit_job_names)
  mrbrt_job_names <- paste0("'cong_mrbrt_bundle_", bun_id, "_*'")
  # Input: age sex split flat file 
  # Output: Crosswalk beta files
}
if(run_match_mrbrt==FALSE){
  message("Proceed")
  mrbrt_job_names <- agesexsplit_job_names
}

# Step 3: Now that we have crosswalks, run application of ratios/outliers
# this will also upload a crosswalk version 
source(paste0(h, "FILEPATH"))
for (bun_id in bundles){
  launch_ratio_application(bun_ids = bun_id, step=step, 
                           model_description = model_description, gbd_round_id = gbd_round_id, hold_string = mrbrt_job_names)
}





