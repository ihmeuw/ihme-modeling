## ---------------------------------------------------------------------------------------------------- ##
## Launch jobs for calculating mean_age
##
## Author: USERNAME
## ---------------------------------------------------------------------------------------------------- ##


## SET UP ENVIRONMENT --------------------------------------------------------------------------------- ##

rm(list=ls())

if (Sys.info()["sysname"] == "Linux") {
  j <- "FILEPATH"
  h <- "FILEPATH"
} else {
  j <- "FILEPATH"
  h <- "FILEPATH"
}

library(data.table)
date <- gsub("-", "_", Sys.Date())

## SET OBJECTS ---------------------------------------------------------------------------------------- ##

# args <- commandArgs(trailingOnly = TRUE)
# filename <- args[1]
filename <- paste0("FILEPATH")

file_path <- paste0("FILEPATH")

calc_age <- fread(filename)
nloops <- ceiling(nrow(calc_age)/5)

for (loop in 1:nloops) {
  # qsub arguments:
  threads <- 2
  proj <- "PROJECT"
  shell_path <- "ADDRESS"
  error_path <- "FILEPATH"
  logs <- "FILEPATH"
  rscript <- paste0("FILEPATH", "calc_age_distribution.R")
  
  command <- paste("qsub -P ", proj,
                   " -l m_mem_free=4G",
                   paste0("-l fthread=", threads),
                   "-q all.q ",
                   "-l archive ",
                   " -N ",paste0("split_", loop),
                   paste(" -e ", error_path, " -o ",logs,
                         " ", shell_path, " ", rscript),
                   " ",loop,
                   " ",filename)
  system(command)
  

  Sys.sleep(1)
}