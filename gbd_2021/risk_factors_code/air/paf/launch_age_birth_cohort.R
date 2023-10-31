rm(list=ls())

Sys.umask(mode = 002)

os <- .Platform$OS.type

library(data.table)
library(magrittr)
library(ggplot2)

# source all central functions
invisible(sapply(list.files("FILEPATH", full.names = T), source))

# ----- SET PARAMS 

version = "VERSION"
apply_air_pollution_shifting <- FALSE 
jointdistr_dir = "FILEPATH"
agedcohorts_dir = "FILEPATH"

jointdistr_files <- list.files(file.path(jointdistr_dir, version, "FILEPATH"))

param_map <- data.table(version = version,
                        jointdistr_dir = jointdistr_dir,
                        apply_air_pollution_shifting = apply_air_pollution_shifting,
                        agedcohorts_dir = agedcohorts_dir,
                        jointdistr_fp = jointdistr_files)

if(apply_air_pollution_shifting == TRUE){
  joint_distr_type = "joint_distr_airpol_shifted"  
} else{
  joint_distr_type = "joint_distr_raked"
}

param_map[, joint_distr_type := joint_distr_type]

dir.create(file.path(agedcohorts_dir, version, joint_distr_type, "param_map"), recursive = T, showWarnings = F)

param_map_filepath <- file.path(agedcohorts_dir, version, joint_distr_type, "FILEPATH", "param_map.csv")
write.csv(param_map, param_map_filepath, row.names = F, na = "")

## QSUB Command
job_name <- "age_birth_cohort"
thread_flag <- "-l fthread=1"
mem_flag <- "-l m_mem_free=3G"
runtime_flag <- "-l h_rt=6:00:00"
jdrive_flag <- "-l archive"
queue_flag <- "-q all.q"
throttle_flag <- "-tc 1500"
n_jobs <- paste0("1:", nrow(param_map))
#n_jobs <- "1:1"
prev_job <- "nojobholds"
next_script <- file.path("/homes", Sys.getenv("USER"), "FILEPATH/age_birth_cohort_parallel.R")
error_filepath <- paste0("FILEPATH", Sys.getenv("USER"), "/FILEPATH")
output_filepath <- paste0("FILEPATH", Sys.getenv("USER"), "/FILEPATH")
project_flag<- "-P PROJECT"

# add jdrive_flag if needed
qsub_command <- paste( "qsub", thread_flag, "-N", job_name, project_flag, mem_flag, runtime_flag, throttle_flag, queue_flag, "-t", n_jobs, "-e", error_filepath, "-o", output_filepath, "-hold_jid", prev_job,  "FILEPATH.sh -i FILEPATH.img -s", next_script, param_map_filepath )

system(qsub_command)


while(length(system(paste0("qstat | grep ", substr(job_name, 1, 9)), intern = T)) > 0){  Sys.sleep(5)  }

param_map[, index := .I]

for(task_id in 1:nrow(param_map)){
  
  print(task_id)
  
  fp <- file.path(agedcohorts_dir, 
                  version,
                  joint_distr_type,
                  "cohorts", 
                  param_map[task_id, jointdistr_fp])
  
  if(!file.exists(fp)){
    n_jobs <- paste0(task_id, ":", task_id)
    next_script <- file.path("/homes", Sys.getenv("USER"), "FILEPATH/age_birth_cohort_parallel.R")
    qsub_command <- paste( "qsub", thread_flag, "-N", job_name, project_flag, mem_flag, runtime_flag, throttle_flag, queue_flag, "-t", n_jobs, "-e", error_filepath, "-o", output_filepath, "-hold_jid", prev_job,  "FILEPATH.sh -i FILEPATH.img -s", next_script, param_map_filepath )
    system(qsub_command)
  }
  
}

length(list.files(file.path(agedcohorts_dir, 
                            version,
                            joint_distr_type,
                            "cohorts"))) == nrow(param_map)