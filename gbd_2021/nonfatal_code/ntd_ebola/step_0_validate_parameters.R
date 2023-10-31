# Purpose: create parameter draws for ebola
# Notes: create/validate parameters and their draws for acute, chronic, and UR using functions in utils script
#
### ======================= BOILERPLATE ======================= ###
rm(list = ls())
code_root <-"FILEPATH"
data_root <- "FILEPATH"

# Source relevant libraries
library(data.table)
library(stringr)
library(argparse)
my_shell <- "FILEPATH/r_shell.sh"
source(paste0("FILEPATH/get_demographics.R"))
source(paste0(code_root, 'FILEPATH/utils.R'))
source(paste0(code_root, 'FILEPATH/processing.R'))

run_file <- fread(paste0(data_root, 'FILEPATH'))
run_path <- run_file[nrow(run_file), run_folder_path]

params_dir <- paste0("FILEPATH/params")
draws_dir <- paste0(run_path, '/draws')

# seed
set.seed(42) 

gbd_round_id <- ADDRESS

data_dir   <- paste0(params_dir, "/data")                            

#[ ======================= MAIN EXECUTION ======================= ###

#' [Create meids and death folders]

ifelse(!dir.exists(paste0(draws_dir, "/ADDRESS")), dir.create(paste0(draws_dir, "/ADDRESS")), FALSE)
ifelse(!dir.exists(paste0(draws_dir, "/ADDRESS")), dir.create(paste0(draws_dir, "/ADDRESS")), FALSE)
ifelse(!dir.exists(paste0(draws_dir, "/ADDRESS")), dir.create(paste0(draws_dir, "/ADDRESS")), FALSE)

####################################################################
#' [Create Ebola parameters and their draws if absent and save them]
####################################################################

# -----------------------------------------------------------------------
#' [Validate acute draws] 

ifelse(!dir.exists(paste0(draws_dir, "/acute_draws")), dir.create(paste0(draws_dir, "/acute_draws")), FALSE)

acute_duration_present <- list.files(path=paste0(draws_dir, "/acute_draws"), pattern = "evd_acute_duration_draws")
  
  if(length(acute_duration_present) == 0){ 
    make_acute_draws(scalar_file = paste0(data_dir, "/acute_dur.csv"), draws_dir = draws_dir)
  }

# -----------------------------------------------------------------------
#' [Validate chronic draws] #bootstrap across parameter values, not predicted values

ifelse(!dir.exists(paste0(draws_dir, "/chronic_draws")), dir.create(paste0(draws_dir, "/chronic_draws")), FALSE)
chronic_duration_present <- list.files(path=paste0(draws_dir, "/chronic_draws"), pattern = "evd_chronic_duration_draws")

if(!("evd_chronic_duration_draws_year1.csv" %in% chronic_duration_present)){  #if it is not then make it
  make_chronic_draws(scalar_file = paste0(data_dir, "/chronic_dur.csv"), draws_dir) 
}

# -----------------------------------------------------------------------
#' [Validate underreporting draws]

ifelse(!dir.exists(paste0(draws_dir, "/underreporting_draws")), dir.create(paste0(draws_dir, "/underreporting_draws")), FALSE)

underreporting_cases_present <- list.files(path=paste0(draws_dir, "/underreporting_draws"), pattern = "evd_underreporting_cases_draws")

if(length(underreporting_cases_present) == 0){
  make_underreport_draws(scalar_file = paste0(data_dir, "/UR.csv"), draws_dir = draws_dir) 
}

# -----------------------------------------------------------------------
