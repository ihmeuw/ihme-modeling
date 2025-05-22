# This code launches location & year specific jobs to convert temperature and
# population rasters to flat files with person-days of exposure to given
# daily mean temperatures.
#
# This is a pretty stock launch script that's undergone some refinement over
# time, but nothing dramatic.
#
# Packages: data.table


# Start with a clean slate
rm(list=ls())

# Define directories
SHARED_FUN_DIR <- 'FILEPATH'
OUT_DIR <- 'FILEPATH'
CODE_DIR <- 'FILEPATH'

# Define run parameters
RELEASE <- 16
N_DRAWS <- 1000
THREADS <- 8
YEARS <- 1990:2023
PROJECT <- 'PROJECT'
R_SHELL <- 'FILEPATH'
QUEUE <- 'long'

user <- Sys.getenv('USER')

# Load packages and custom functions
library('data.table')

source('FILEPATH/submit_jobs.R')
source(file.path(SHARED_FUN_DIR, 'get_cause_metadata.R'))
source(file.path(SHARED_FUN_DIR, 'get_demographics.R'))
source(file.path(SHARED_FUN_DIR, 'get_location_metadata.R'))

# Get location metadata so we have a list of locations to run
loc_meta <- get_location_metadata(location_set_id = 35, release_id = RELEASE)
loc_ids <- loc_meta[(is_estimate == 1 & most_detailed == 1) | level == 3 , location_id]
loc_ids <- loc_ids[order(loc_ids)]

# Set up arguments for the run in a list
arg_list <- list(loc_id = loc_ids, year = YEARS, release = RELEASE,
                 n_draws = N_DRAWS, threads = THREADS, outdir = OUT_DIR)

name_stub <- 'melt'
name_args <- c('loc_id', 'year')

# Set up log directory
log_dir <- paste0(' -o FILEPATH', user,
                  '/output/%x.o%j.out -e FILEPATH', user,
                  '/errors/%x.e%j.err ')

# Set up memory and slots
if (length(name_args) == 1) {
  MEMORY <- '250G'
} else {
  MEMORY <- '150G'
}

## LAUNCH JOBS -----------------------------------------------------------------
submit_jobs(arg_list = arg_list,
            name_stub = name_stub,
            name_args = name_args,
            log_dir = log_dir,
            memory = MEMORY,
            slots = THREADS,
            worker_script = file.path(CODE_DIR, 'era_raster_to_person_time_table.R'),
            r_shell = R_SHELL,
            project = PROJECT,
            queue = QUEUE,
            outfile_dir = OUT_DIR,
            outfile_suffix = '.csv',
            run_time = '20:00:00')


## CHECK OUTPUTS ---------------------------------------------------------------
# Check to see if jobs completed successfully by checking for output files
status <- data.table(expand.grid(location_id = loc_ids, year_id = YEARS))
status[, complete := file.exists(file.path(OUT_DIR, paste0('melt_',
                                                           location_id,
                                                           '_',
                                                           year_id,
                                                           '.csv')))]
table(status$complete, useNA = 'ifany')