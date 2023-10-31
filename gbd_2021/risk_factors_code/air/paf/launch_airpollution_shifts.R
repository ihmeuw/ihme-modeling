Sys.umask(mode = 002)

os <- .Platform$OS.type
my_libs <- "FILEPATH"

library(data.table)
library(magrittr)
library(ggplot2)

source("FILEPATH/get_demographics.R"  )

version = "VERSION"

demo <- get_demographics(gbd_team = "epi", gbd_round_id = 7)

jointdistr_fps <- list.files(file.path("FILEPATH", version, "/FILEPATH/"))

## First create param_map
param_map <- data.table(expand.grid(jointdistr_fp = jointdistr_fps,
                                    airpol_shift_version = 47,
                                    make_diagnostic = 0,
                                    results_dir = paste0("FILEPATH/", version, "/")))

param_map[location_id %% 5 == 0, make_diagnostic := 1]
param_map[is.na(make_diagnostic), make_diagnostic := 0]

param_map_filepath <- paste0("FILEPATH/", version, "/FILEPATH.csv")
write.csv(param_map, param_map_filepath, row.names = F, na = "")


## QSUB Command
job_name <- "bwga_airpol"
thread_flag <- "-l fthread=1"
mem_flag <- "-l m_mem_free=3G"
runtime_flag <- "-l h_rt=6:00:00"
jdrive_flag <- "-l archive"
queue_flag <- "-q all.q"
throttle_flag <- "-tc 1000"
n_jobs <- paste0("1:", nrow(param_map))
n_jobs <- "116:118"
prev_job <- "nojobholds"
next_script <- "FILEPATH/airpollution_shifts.R"
error_filepath <- paste0("FILEPATH/", Sys.getenv("USER"), "/FILEPATH")
output_filepath <- paste0("FILEPATH/", Sys.getenv("USER"), "/FILEPATH")
project_flag<- "-P PROJECT"


# add jdrive_flag if needed
qsub_command <- paste( "qsub", thread_flag, "-N", job_name, project_flag, mem_flag, runtime_flag, throttle_flag, queue_flag, "-t", n_jobs, "-e", error_filepath, "-o", output_filepath, "-hold_jid", prev_job,  "FILEPATH.sh -i FILEPATH.img -s", next_script, param_map_filepath )

system(qsub_command)

