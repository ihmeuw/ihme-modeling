# Created by: alee91
# Created on: 3/13/18

args <- commandArgs(trailingOnly = TRUE)
date <- args[1]
rei <- args[2]

library(ini)
library(DBI)
source("FILEPATH/launch_paf.R")

print(.libPaths())

main <- function(date, rei) {

      if (rei == 370) {
            launch_paf(rei_id=370, save_results=TRUE, cluster_proj="proj_ensemble", slots=20, log=FALSE, year_id=c(1990, 2005, 2017))

            f <- file(paste0("FILEPATH", date, "/step_10_adult.txt"))
            writeLines("PAFs successfully completed", f)
            close(f)
      } else {
            launch_paf(rei_id=371, save_results=TRUE, cluster_proj="proj_ensemble", log=FALSE, year_id=c(1990, 2005, 2017))

            f <- file(paste0("FILEPATH", date, "/step_10_child.txt"))
            writeLines("PAFs successfully completed", f)
            close(f)
      }
}

 main(date=date, rei=rei)
