##########################################################################
## Author: USERNAME
## Date: March 15, 2017
## Purpose: MASTER FOR SEXUAL VIOLENCE MODEL
##########################################################################

# clear workspace environment
rm(list = ls())

if(Sys.info()[1] == 'Windows') {
  username <- "USERNAME"
  root <- "FILEPATH"
  code_dir <- paste0("FILEPATH")
} else {
  username <- "USERNAME"
  root <- "FILEPATH"
  code_dir <- paste0("FILEPATH")
  code_dir <- "FILEPATH"
}

pacman::p_load(data.table, magrittr)

# run date
date <- Sys.Date()

# are you testing?
test = F

# what script do you want to run?
prep = F
inj = F
mental = F
prev = T

# source the central functions
source("FILEPATH.R")
source("FILEPATH.R")
source("FILEPATH.R")

# source job array function
source("FILEPATH.R")

# define directories to use

out_dir <- "FILEPATH"
dir.create(out_dir, recursive = T)
data_dir <- "FILEPATH"

## ------------------ SUBMIT THE PREP SCRIPT ------------------------------- ##

files <- list.files(data_dir)

if(prep) job.array.master(tester = test, paramlist = files, jobname = "marketscan",
                                 childscript = "01_prep.R", slots = 11, args = list(out_dir, data_dir))

## ------------ INJURIES ANALYSIS FOR SEXUAL VIOLENCE ---------------------- ##

if(inj) source("FILEPATH.R")


## ------------ MENTAL HEALTH ANALYSIS FOR SEXUAL VIOLENCE ----------------- ##

if(mental) source("FILEPATH.R")

## ---------------- APPLY PROPS TO DISMOD OUTPUT --------------------------- ##

dems <- get_demographics(gbd_team = "epi")
print(dems)

demographics <- list(dems$sex_ids, dems$location_ids, dems$year_ids)

in_dir <- paste0(out_dir, "/FILEPATH")
out_dir_prev <- paste0("FILEPATH", date, "/FILEPATH")
dir.create(out_dir_prev, recursive = T)

date <- "2017_06_21"
como_dir <- paste0("FILEPATH")
results_dirs <- c(paste0(como_dir, "/FILEPATH"), paste0(como_dir, "/FILEPATH"))
lapply(results_dirs, dir.create, recursive = T)

ylds_dir <- paste0(como_dir, "/FILEPATH/", c("E", "N"))
ylds_dir_E <- paste0(ylds_dir[1], "/FILEPATH")
dir.create(ylds_dir_E, recursive = T)

prev_dir_E <- paste0(como_dir, "/FILEPATH")
dir.create(prev_dir_E, recursive = T)

if(prev) job.array.master(tester = test, paramlist = demographics, jobname = "prevalence", project = "proj_injuries_2",
                          childscript = "04_prev.R", slots = 2, args = list(in_dir, out_dir_prev, como_dir, date))

