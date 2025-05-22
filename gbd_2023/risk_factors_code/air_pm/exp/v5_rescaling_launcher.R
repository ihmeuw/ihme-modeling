rm(list=ls())

# Libraries
library(data.table)
library(tidyverse)
library(dplyr)
library(haven) # for reading in dta files
library(argparse)
library(stringr)

central_lib <- "FILEPATH"
source(file.path(central_lib,"FILEPATH/get_location_metadata.R"))
locs <- get_location_metadata(35,release_id=16)

# Set up
user <- "USER"
project <- "-A proj_erf"
GBD <- 'GBD2022'
sge.output.dir <- paste0("FILEPATH")
rshell <- "/ihme/singularity-images/rstudio/shells/execRscript.sh -s"
draws.required <- 250
grid_version <- 'v5_4' # v5

# # set flags for running exposures and PAF setup
run_exposures_rescale <- TRUE # FALSE

if (run_exposures_rescale) {
  
  run_rescale <- function(location_id) {
  
    worker_script <- 'v5_rescaling_1990_1995_2000-2005_exposures.R'
    code_loc <- paste0('FILEPATH/air_pm/exp/')
    worker_path <- paste0(code_loc, worker_script)
    
    memory <- 75 # 30 # GB
    cores.provided <- 1
    args <- paste(location_id, draws.required)
    mem <- paste0("--mem=",memory,"G")
    fthread <- paste0("-c ",cores.provided)
    runtime <- "-t 01:00:00"
    archive <- ""
    jname <- "-J v5_rescale_"
    q <- "-p all.q"
    #hold <- "-d 47131829"
    
    system(paste("sbatch",paste0(jname, location_id),mem,fthread,runtime,archive,project,q,sge.output.dir,rshell,worker_path,args))
    
  }
  
  pblapply(62, run_rescale)
  
}


# #------------------------------------------------------------------------------------------
# Once the rescaling and smoothing is done, the code below will help check to make sure all locs/years are accounted for.
# More importantly, this code will compile all of the relevant files into a single directory.

#### clean up files

v5_4.dir <- file.path("FILEPATH")
gbd2021_w2005_rescaled.dir <- file.path("FILEPATH")
gbd2021_rolling_avg.dir <- file.path("FILEPATH")

v5_4_files <- list.files(v5_4.dir, full.names = FALSE)

v5_files_2009 <- as.data.table(v5_4_files[grep("2009|2010|2011|2012|2013|2014|2015|2016|2017|2018|2019|2020|2021|2022", v5_4_files)])

# for v5_4, only want files for 2009-2022 (and most_detailed location)
v5_4_files_dt <- v5_files_2009 %>%
  mutate(loc = gsub("^(\\d+)_.*", "\\1", V1)) %>%
  mutate(loc = as.numeric(loc))

most_detailed_locs <- filter(locs, most_detailed == 1)


v5_4_files_clean <- paste0("FILEPATH/air_pm/exp/gridded/v5_4/draws/", v5_4_files_dt$V1)

# these are the files we're replacing/adding
gbd2021_w2005_rescaled_files <- list.files(gbd2021_w2005_rescaled.dir, full.names = TRUE)
gbd2021_rolling_avg_files <- list.files(gbd2021_rolling_avg.dir, full.names = TRUE)

# final list
final_file_list <- c(v5_4_files_clean, gbd2021_w2005_rescaled_files, gbd2021_rolling_avg_files)

