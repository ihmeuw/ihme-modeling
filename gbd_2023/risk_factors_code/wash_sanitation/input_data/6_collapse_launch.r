###########################################################

### Purpose: Collapse ubcov extraction output
###########################################################

###################
### Setting up ####
###################

rm(list=ls())
os <- .Platform$OS.type
if (os == "windows") {
  j <- "J:/"
  h <- "H:/"
  l <- "L:/"
  ## Load Packages
  pacman::p_load(data.table, haven, dplyr, survey, Hmisc)
} else {
  j <- "FILEPATH"
  user <- Sys.info()[["user"]]
  h <- paste0("FILEPATH", user, "/")
  l <- "FILEPATH"
}

## Load Functions
ubcov_central <- "FILEPATH"
setwd(ubcov_central)
source("FILEPATH/launch.r")

######################################################################################################################

## Settings

topic <- "wash"
config.path <- "FILEPATH/wash_collapse_config_sm.csv"
parallel <- F
cluster_project <- "proj_erf"
fthreads <- 3 ## How many threads per job (used in mclapply) | Set to 1 if running on desktop
m_mem_free <- 25 ## How many GBs of RAM to use per job
h_rt <- "10:00:00"  ## How much run time to request per job | format is "HH:MM:SS"
logs <- "FILEPATH"

## Launch collapse

df <- collapse.launch(topic=topic, config.path=config.path, parallel=parallel, cluster_project=cluster_project, 
                      fthreads=fthreads, m_mem_free=m_mem_free, h_rt=h_rt, logs=logs, central.root=ubcov_central)
