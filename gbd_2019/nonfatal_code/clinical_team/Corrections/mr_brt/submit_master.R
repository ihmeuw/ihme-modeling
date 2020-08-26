#############################################################
##Date: 3/4/2019
##Purpose: Submit job to submit Master script to prep data and submit MR-BRT models
##Notes: ***NOT currently used to run CFs. This script makes testing/ad-hoc runs easier, but script cflauncher.py includes checks & pre-processing not used here.
##Updates: 
##Updates: 
##Updates: 
###########################################################

rm(list = ls())

user <- Sys.info()[["user"]]
args <- commandArgs(trailingOnly = TRUE)
run <- args[[1]][1]
estimate <- args[[1]][2]
bundle_file <- args[[1]][3]

# Variables for testing -- comment out unless running from here
# estimate <- 'custom'
# bundle_file <- paste0('test2bundles.csv')
# run <- 'run_3'

# Validate input args
stopifnot(!is.na(run) & !is.na(estimate))
if(estimate=='custom'){
  stopifnot(!is.na(bundle_file))
}

# Prep CFs from input data -----
source(paste0(FILEPATH))
prep_cfs(run)

# Send qsub to run MR-BRT master-----
passvars <- paste0(c(run, estimate, bundle_file), collapse = ',')

qsub <- QSUB
system(qsub)
