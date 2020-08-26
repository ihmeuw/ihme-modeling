
# --------------

os <- .Platform$OS.type
if (os=="windows") {
  j<- "FILEPATH"
  h <-"FILEPATH"
  my_libs <- paste0(h, "/local_packages")
} else {
  j<- "FILEPATH"
  h<-"FILEPATH"
  my_libs <- paste0(h, "/cluster_packages")
}

library(data.table)
library(magrittr)
library(ggplot2)

source("FILEPATH"  )


locs <- get_demographics(gbd_team = "epi")$location_id

param_map <- data.table(loc = locs)

## First create param_map

param_map_filepath <- "FILEPATH"
write.csv(param_map, param_map_filepath, row.names = F, na = "")


## QSUB Command
job_name <- "birth_5plus_noedema"
thread_flag <- "-l fthread=2"
mem_flag <- "-l m_mem_free=4G"
runtime_flag <- "-l h_rt=6:00:00"
jdrive_flag <- "-l archive"
queue_flag <- "-q all.q"
n_jobs <- paste0("1:", nrow(param_map))
#n_jobs <- "1:1"
prev_job <- "nojobholds"
next_script <- "FILEPATH"
error_filepath <- paste0("FILEPATH", Sys.getenv("USER"), "/errors")
output_filepath <- paste0("FILEPATH", Sys.getenv("USER"), "/output")
project_flag<- paste0("-P ", "proj_neonatal")

qsub_command <- paste( "qsub", thread_flag, "-N", job_name, project_flag, mem_flag, runtime_flag, queue_flag, "-t", n_jobs, "-e", error_filepath, "-o", output_filepath, "-hold_jid", prev_job,  "FILEPATH", next_script, param_map_filepath )

system(qsub_command)


