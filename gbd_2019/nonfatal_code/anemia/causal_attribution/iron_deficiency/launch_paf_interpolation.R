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


##loop args
locs <- get_location_metadata(35, gbd_round_id = 6, decomp_step = 'step4')


for (loc in locs$location_id){
  job_name <- paste0("calculate_paf_interpolation_iron_", loc)
  
  system(paste( "qsub -P proj_anemia -l m_mem_free=6G -l fthread=1 -q long.q  -N ", job_name, "FILEPATH", next_script, loc))
  
}


