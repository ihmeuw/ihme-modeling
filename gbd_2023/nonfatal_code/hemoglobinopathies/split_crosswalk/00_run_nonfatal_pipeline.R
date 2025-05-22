##' *************************************************************************************
##' Title: 00_run_nonfatal_pipeline.R
##' Purpose: Age/sex split and crosswalk nonfatal hemoglobinopathy causes.
##' *************************************************************************************

os <- .Platform$OS.type
if (os == "windows") {
  j <- "FILEPATH"
  h <- "FILEPATH"
} else {
  j <- "FILEPATH"
  h <- "FILEPATH"
}


library('openxlsx')
library('tidyr')
library('dplyr')
library('data.table')
library('matrixStats')

## Source all shared functions
invisible(lapply(list.files("FILEPATH", full.names = T), function(x){  source(x)  }))

'%ni%' <- Negate('%in%')
## Set params
release_id <- 9 
old_release_id <- 9 
gbd_round_id <- 7


save_bv <- TRUE 
agesex <- TRUE
apply_outliers <- TRUE
save_cw <- TRUE


bundles <- c(206, 207, 208, 209, 210, 211, 212)
save_dir <- "FILEPATH"
upload_filepath <- "FILEPATH"

date <- gsub('-', '_', Sys.Date())
cw_description <- paste0(date, '_description')

##' **************************************************************************************************************
##' 1. Upload bundle data
##' **************************************************************************************************************

for (bun_id in bundles){

  upload_bundle_data(bundle_id = bun_id, filepath = upload_filepath)
  
}

##' **************************************************************************************************************
##' 2. Save bundle versions
##' **************************************************************************************************************

if (save_bv == T){
  for (bun_id in bundles){
    print(bun_id)
    
    if(bun_id == 208){
      new_version <- save_bundle_version(bundle_id = bun_id, 
                                         include_clinical = c("claims"))
    } 
    if(bun_id == 212){
      new_version <- save_bundle_version(bundle_id = bun_id)
    } 
    
    if (bun_id == 3011){
      new_version <- save_bundle_version(bundle_id = bun_id, 
                                         include_clinical = c("inpatient", "claims"))
    }
    if (bun_id %ni% c(208, 212, 3011)){
      new_version <- save_bundle_version(bundle_id = bun_id, 
                                         include_clinical = c("inpatient", "claims"))
    }
  }
  
}
##' **************************************************************************************************************
##' 3. Age-sex splitting
##' **************************************************************************************************************
if (agesex == T){
  
  bvs <- c()
  for (bun_id in bundles){
    version_map <- read.csv(paste0("FILEPATH"))
    bun_version <- version_map$bundle_version_id
    bvs <- c(bvs, bun_version)
  }
  
  inputs <- data.table(bundle_id = bundles, bv_id = bvs)
  
  
  #Job specifications
  username <- Sys.getenv("USER")
  m_mem_free <- "-l m_mem_free=40G"
  fthread <- "-l fthread=4"
  runtime_flag <- "-l h_rt=04:00:00"
  jdrive_flag <- "-l archive"
  queue_flag <- "-q long.q"
  shell_script <- "-cwd FILEPATH -s "
  
  ### change to your own repo path if necessary
  script <- paste0("FILEPATH/01_do_agesex_split.R")
  errors_flag <- paste0("-e FILEPATH")
  outputs_flag <- paste0("-o FILEPATH")
  
  measure_name <- 'prevalence'
  type <- 'dismod'
  release_id <- release_id
  cascade <- FALSE
  lit_cascade <- FALSE
  old_release_id<- old_release_id
  
  for (row_i in 1:nrow(inputs)) {
    
    bun_id <- inputs[row_i, bundle_id]
    bv_id <- inputs[row_i, bv_id]
    outdir <- paste0("FILEPATH") 
    
    job_name <- paste0("-N", " logspl_", bun_id)
    job <- paste("qsub", m_mem_free, fthread, runtime_flag, jdrive_flag, queue_flag, job_name, "-P proj_nch",
                 outputs_flag, errors_flag, shell_script, script, 
                 bv_id, measure_name, type, release_id, cascade, lit_cascade, outdir, old_release_id)
    system(job)
    print(job_name)
  }
  
}
##' **************************************************************************************************************
##' 4. Crosswalking
##' **************************************************************************************************************

bun_id <- 212
trim_percent <- .1
out_dir <- "FILEPATH"

source(paste0(h, "FILEPATH/02_crosswalk.R"))

crosswalk_and_adjust(bun_id, measure_name = 'prevalence', trim_percent, out_dir, offset = 1e-7)


##' **************************************************************************************************************
##' 5. Apply outliers
##' **************************************************************************************************************

if (apply_outliers == TRUE){
  #Job specifications
  username <- Sys.getenv("USER")
  m_mem_free <- "-l m_mem_free=40G"
  fthread <- "-l fthread=4"
  runtime_flag <- "-l h_rt=04:00:00"
  jdrive_flag <- "-l archive"
  queue_flag <- "-q long.q"
  shell_script <- "-cwd FILEPATH -s "
  
  ### change to your own repo path if necessary
  script <- paste0("FILEPATH/03_apply_outliers.R")
  errors_flag <- paste0("-e FILEPATH")
  outputs_flag <- paste0("-o FILEPATH")
  
  outlier_save_dir <- "FILEPATH"
  for (bun_id in bundles){
    job_name <- paste0("-N", " apply_outliers_", bun_id)
    job <- paste("qsub", m_mem_free, fthread, runtime_flag, jdrive_flag, queue_flag, job_name, "-P proj_nch",
                 outputs_flag, errors_flag, shell_script, script, bun_id, release_id, outlier_save_dir)
    system(job)
    print(job_name)
  }
}


##' **************************************************************************************************************
##' 6. Save crosswalk version
##' **************************************************************************************************************
date <- gsub('-', '_', Sys.Date())
cw_description <- paste0(date, '_description') # description of crosswalk to save

if (save_cw == TRUE){
  #Job specifications
  username <- Sys.getenv("USER")
  m_mem_free <- "-l m_mem_free=40G"
  fthread <- "-l fthread=4"
  runtime_flag <- "-l h_rt=04:00:00"
  jdrive_flag <- "-l archive"
  queue_flag <- "-q long.q"
  shell_script <- "-cwd FILEPATH -s "
  
  ### change to your own repo path if necessary
  script <- paste0("FILEPATH/04_save_crosswalk.R")
  errors_flag <- paste0("-e FILEPATH")
  outputs_flag <- paste0("-o FILEPATH")
  
  
  for (bun_id in bundles){
    job_name <- paste0("-N", " save_cw_", bun_id)
    job <- paste("qsub", m_mem_free, fthread, runtime_flag, jdrive_flag, queue_flag, job_name, "-P proj_nch",
                 outputs_flag, errors_flag, shell_script, script, bun_id, release_id, save_dir, cw_description)
    system(job)
    print(job_name)
  }
}

##' **************************************************************************************************************
##' 6b. Boxplot diagnostics
##' **************************************************************************************************************


crosswalked <- TRUE
by_super_region <- TRUE
location_set_id <- 35

#Job specifications
username <- Sys.getenv("USER")
m_mem_free <- "-l m_mem_free=40G"
fthread <- "-l fthread=4"
runtime_flag <- "-l h_rt=04:00:00"
jdrive_flag <- "-l archive"
queue_flag <- "-q long.q"
shell_script <- "-cwd FILEPATH -s "

### change to your own repo path if necessary
script <- paste0("FILEPATH/05_diagnostic_boxplots.R")
errors_flag <- paste0("-e FILEPATH")
outputs_flag <- paste0("-o FILEPATH")

for (bun_id in bundles){  
  job_name <- paste0("-N", " boxplot_", bun_id)
  job <- paste("qsub", m_mem_free, fthread, runtime_flag, jdrive_flag, queue_flag, job_name, "-P proj_nch",
               outputs_flag, errors_flag, shell_script, script, 
               bun_id, crosswalked, by_super_region, release_id, location_set_id)
  system(job)
  print(job_name)
}



