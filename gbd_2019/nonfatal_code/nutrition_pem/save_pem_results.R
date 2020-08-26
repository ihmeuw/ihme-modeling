
## Purpose: save non fatal pem  inputs
## Dependencies:

# Set environment

# rm(list=ls())

os <- .Platform$OS.type
if (os == "windows") {
  jpath <- "FILEPATH"
} else {
  jpath <- "FILEPATH"
}


param_map <- data.table(input_dir = c("FILEPATH", 
                                      "FILEPATH", 
                                      "FILEPATH", 
                                      "FILEPATH"),
                        input_file_pattern = c("{location_id}_{age_group_id}.csv", "{location_id}_{age_group_id}.csv", "{measure_id}_{location_id}.csv", "{measure_id}_{location_id}.csv"),
                        me = c(10981, 1607, 1606, 1608))

param_map_filepath <- "FILEPATH"
write.csv(param_map, param_map_filepath, row.names = F, na = "")


## QSUB Command
job_name <- "save_edema"
thread_flag <- "-l fthread=30"
mem_flag <- "-l m_mem_free=80G"
runtime_flag <- "-l h_rt=12:00:00"
queue_flag <- "-q long.q"
n_jobs <- paste0("1:", nrow(param_map))
prev_job <- "nojobholds"
next_script <- "FILEPATH"
error_filepath <- paste0("FILEPATH", Sys.getenv("USER"), "/errors")
output_filepath <- paste0("FILEPATH", Sys.getenv("USER"), "/output")
project_flag<- paste0("-P ", "proj_neonatal")

qsub_command <- paste( "qsub", thread_flag, "-N", job_name, project_flag, mem_flag, runtime_flag, queue_flag, "-t", n_jobs, "-e", error_filepath, "-o", output_filepath, "-hold_jid", prev_job,  "FILEPATH", next_script, param_map_filepath )

system(qsub_command)




