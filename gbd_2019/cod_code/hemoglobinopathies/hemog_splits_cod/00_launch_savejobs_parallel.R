rm(list=ls())
os <- .Platform$OS.type
if (os == "windows") {
  j <- "J:/"
  h <- "H:/"
} else {
  j <- "FILEPATH"
  h <- "USERNAME"
}

## QSUB
#slot_number <- 50
#fthreads <- 20
#m_mem_free <- 
next_script <- paste0("FILEPATH")


##loop args
#cause_list <- c(614, 615, 616)
# cause_list <- c(616, 618)
cause_list <- c(618)

for (cause in cause_list){
  job_name <- paste0("save_hemog_", cause)
  
  system(paste( "qsub -P proj_custom_models -l m_mem_free=110G -l fthread=15 -q long.q  -N ", job_name, "FILEPATH -s", next_script, cause))
}
