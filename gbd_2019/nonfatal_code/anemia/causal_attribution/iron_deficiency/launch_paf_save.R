rm(list=ls())
os <- .Platform$OS.type
if (os == "windows") {
  j <- "J:/"
  h <- "H:/"
} else {
  j <- "/home/j/"
  h <- "/homes/USERNAME/"
}
library("stringr")
library("tidyverse")
#################

source("FILEPATH")

next_script <- paste0("FILEPATH")




job_name <- paste0("saving_paf_interpolation_interpolated")

system(paste( "qsub -P proj_anemia -l m_mem_free=110G -l fthread=8 -q long.q  -N ", job_name, "FILEPATH", next_script))


