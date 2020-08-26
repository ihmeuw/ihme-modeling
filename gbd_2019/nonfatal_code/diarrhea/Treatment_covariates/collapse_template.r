###########################################################
### Date: 2016
### Project: ubCov
### Purpose: Collapse ubcov extraction output
###########################################################

###################
### Setting up ####
###################
rm(list=ls())
os <- .Platform$OS.type
if (os == "windows") {
  j <- "ADDRESS"
  h <- "ADDRESS"
  ## Load Packages
  pacman::p_load(data.table, haven, dplyr, survey, Hmisc)
} else {
  j <- "ADDRESS"
  user <- Sys.info()[["user"]]
  h <- paste0("ADDRESS")
}

## Load Functions
ubcov_central <-  "ADDRESS"
setwd(ubcov_central)
source(paste0(ubcov_central,"FILEPATH"))

######################################################################################################################

## Settings

topic <- "diarrhea_antibiotics" ## Subset config.csv
config.path <- paste0(h,"FILEPATH") ## Path to config.csv. Note: Parallel runs will not work if there are spaces in this file path.
parallel <- FALSE ## Run in parallel?
cluster_project <- 'proj_hiv' ## You must enter a cluster project in order to run in parallel
slots <- 1 ## How many slots per job (used in mclapply) | Set to 1 if running on desktop
logs <- NA ## Path to logs

## Launch collapse
df <- collapse.launch(topic=topic, config.path=config.path, parallel=parallel, cluster_project=cluster_project, logs=logs, central.root=ubcov_central)
