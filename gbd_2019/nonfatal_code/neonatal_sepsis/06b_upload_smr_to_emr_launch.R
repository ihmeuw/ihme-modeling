# Purpose: Upload the EMR results from 05_smr_to_emr. Submit a parallel script for each long-term impairment "target" ME_id.
# 
# Input: FILEPATH/[target_me_id]_mtexcess.xlsx
# Output: FILEPATH/8621_mtexcess.xlsx

# --------------

rm(list=ls())

j <- "FILEPATH"
library(data.table)
library(magrittr)


# Key
key <- data.table(acause = c("neonatal_sepsis"),
                  bundle_id = c(499),
                  me_id = c(8674))

for(i in 1:nrow(key)){
  
  acause <- key[i, acause]
  bundle_id <- key[i, bundle_id]
  me_id <- key[i, me_id]
  
  ## QSUB Call
  job_flag <- paste0("-N smr_upload_", bundle_id) 
  project_flag <- paste0("-P proj_anemia")
  thread_flag <- "-l fthread=10"
  mem_flag <- "-l m_mem_free=80G"
  runtime_flag <- "-l h_rt=10:00:00"
  jdrive_flag <- "-l archive"
  queue_flag <- "-q long.q"
  next_script <- "FILEPATH/upload_smr_to_emr_parallel.R"
  errors_flag <- paste0("-e FILEPATH")
  outputs_flag <- paste0("-o FILEPATH")
  shell_script <- "-cwd FILEPATH/r_shell_ide.sh"
  args_to_pass <- paste(acause, bundle_id, me_id)
  
  qsub <- paste("qsub ", job_flag, project_flag, thread_flag, mem_flag, runtime_flag, jdrive_flag, queue_flag,
                errors_flag, outputs_flag, shell_script, next_script, args_to_pass)
  
  system(qsub)
  
}

