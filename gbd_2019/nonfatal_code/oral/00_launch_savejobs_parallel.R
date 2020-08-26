rm(list=ls())
os <- .Platform$OS.type
if (os == "windows") {
  j <- "J:/"
  h <- "H:/"
} else {
  j <- "FILEPATH"
  h <- "FILEPATH"
}

## QSUB
slot_number <- 4
next_script <- paste0("FILEPATH")


##loop args
# mes_to_upload <- c(2583, 2582, 2584, 3083, 2335, 3093, 3092, 3091)
# mes_to_upload <- c(2583, 2582, 3092, 3091, 3084, 3083, 3093)
# mes_to_upload <- 3092

mes_to_upload <- c(2584, 3093, 3084, 2583, 3092, 2582, 3091, 3083)
##mes_to_upload <- c(2335)

print("Starting save")

for (me in mes_to_upload){
  job_name <- paste0("save_dental_", me)
  
  system(paste( "qsub -P proj_custom_models -l m_mem_free=60G -l fthread=8 -l h_rt=05:00:00 -q long.q -N", job_name, "FILEPATH", next_script, me))
}
