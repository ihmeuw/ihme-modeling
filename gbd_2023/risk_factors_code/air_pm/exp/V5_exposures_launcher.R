rm(list=ls())

# Libraries
library(data.table)
library(tidyverse)
library(dplyr)
library(haven) # for reading in dta files
library(argparse)
library(stringr)

# Set up
user <- 'USER'
project <- "-"
GBD <- 'GBD2022'
sge.output.dir <- paste0("FILEPATH")
rshell <- "/ihme/singularity-images/rstudio/shells/execRscript.sh -s"
draws.required <- 250 
grid_version <- 'v5_4' # v5

# set flags for running exposures and PAF setup
run_exposures <- FALSE
run_PAF_setup <- TRUE # FALSE

######## EXPOSURES ########

if (run_exposures) {
  
  worker_script <- 'V5_exposures_worker.R'
  code_loc <- paste0('FILEPATH')
  worker_path <- paste0(code_loc, worker_script)
  
  ## Outputs
  new_wd <- paste0('FILEPATH')
  # setwd(new_wd)
  output_path <- paste0(new_wd, '/', grid_version, '/draws/')
  output_summary_path <- paste0('FILEPATH/results/summary/', grid_version)
  
  # create directory if it doesn't exist:
  if (!dir.exists(output_path)) {
    dir.create(output_path, recursive = TRUE)
  }
  
  if (!dir.exists(output_summary_path)) {
    dir.create(output_summary_path, recursive = TRUE)
  }
  
  years <- c(1990, 1995, 2000:2022) # 1990, 1995, 2000-2005 will use GBD 2021 inputs and will get rescaled afterwards
  
  memory <- 100 # GB
  cores.provided <- 1
  args <- paste(draws.required, GBD, output_path, output_summary_path)
  mem <- paste0("--mem=",memory,"G")
  fthread <- paste0("-c ",cores.provided)
  runtime <- "-t 08:00:00"
  archive <- "-C archive"
  jname <- "-J v5_exp_"
  q <- "-p long.q"

  
  # loop over years and launch jobs
  for (year in years) {
    
    system(paste("sbatch",paste0(jname,year),mem,fthread,runtime,archive,project,q,sge.output.dir,rshell,worker_path,args,year))
    
  }
  
}

#### check if there are any missing locations/years
v5_2_gridded <- list.files('FILEPATH')
new_gridded <- list.files('FILEPATH')

missing_in_new <- as.data.table(setdiff(v5_2_gridded, new_gridded))


######## PAF calc exposure prep #########

if (run_PAF_setup) {

  worker_script <- 'PAF_calc_exposure_prep.R'
  code_loc <- paste0('FILEPATH')
  worker_path <- paste0(code_loc, worker_script)
  input_path <- paste0("FILEPATH/draws_final/") # new draws directory that incorporates rescaling and smoothing
  
  memory <- 10 # GB
  cores.provided <- 1
  # args <- paste(draws.required, cores.provided, GBD, output_path)
  mem <- paste0("--mem=",memory,"G")
  fthread <- paste0("-c ",cores.provided)
  runtime <- "-t 01:00:00"
  archive <- "-C archive"
  jname <- "-J v5_paf_prep_"
  q <- "-p all.q"
  
  
  # loop over each file in the directory and launch jobs
  
  files <- list.files(input_path, pattern = ".fst", full.names = FALSE)
  
  for (file in files) {
    
    # year id is the four digits preceding the file extension
    year <- str_extract(file, "[0-9]{4}(?=\\.fst)")
    
    # loc_id are the first numbers before the underscode in the file name
    loc_id <- str_extract(file, "^[0-9]+(?=_)")
    
    args <- paste(loc_id, year, grid_version, draws.required, GBD)
    # browser()
    system(paste("sbatch",paste0(jname,file),mem,fthread,runtime,archive,project,q,sge.output.dir,rshell,worker_path,args))
  }

}
