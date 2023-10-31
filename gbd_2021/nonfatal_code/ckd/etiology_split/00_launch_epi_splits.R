# ---HEADER--------------------------------------------------------------------------------------------------
# USERNAME
# Date Created: 12/24/2019
# Project: Split CKD epi model
#------------------------------------------------------------------------------------------------------------

# ---SETTINGS------------------------------------------------------------------------------------------------
rm(list=ls())
# Versioning 
output_version <- 2
# Description
description <- "DESCRIPTION"
# Decomp step
ds <- "iterative"
# Run 1 first, then 1.5, then 2.  
process <- 1
round <- "7"

#-------------------------------------------------------------------------------------------------------------

# ---CONFIG--------------------------------------------------------------------------------------------------
#System settings
threads <- 30
mem_free <- 60
runtime <- "24:00:00"
q <- "long.q"
project <- "proj_yld"
code_general <- "FILEPATH"

# Shell
shell <- 'SHELL'
py_shell<- paste0(code_general, '/shells/py_shell.sh')

#Scripts
split_epi <- paste0("FILEPATH")
fix_alb <- paste0("FILEPATH")
append_inc_prev <- paste0("FILEPATH")
save_epi <- paste0("FILEPATH")

# load packages 
require(data.table)
require(openxlsx,lib.loc = paste0(share_path,"rlibs"))
library(openxlsx)

# ---SETTINGS-------------------------------------------------------------------------------------------------

type <- "ckd5hf"

if (type == "all") {
  map<-as.data.table(read.xlsx(paste0("FILEPATH")))
} else if (type == "stages") {
  map<-as.data.table(read.xlsx(paste0("FILEPATH")))
} else if (type == "alb") {
  map <- as.data.table(read.xlsx(paste0("FILEPATH")))
} else if (type == "ckd5hf") {
  map <- as.data.table(read.xlsx(paste0("FILEPATH")))
} else {
  map<-as.data.table(read.xlsx(paste0("FILEPATH")))
}

# set source mes 
source_mes<-unique(map[,source_me_id])

# --- LAUNCH SPLIT AND SAVE-----------------------------------------------------------------------------------
# STEP 1 Launch split jobs-------------------------------------------------------
if (process == 1) {
  errors <- paste0("FILEPATH")
  if (!file.exists(errors)){dir.create(errors,recursive=T)}
  for (source_me in source_mes){
    name<- paste0('split_ckd_epi_mod_',source_me)
    sys_sub<- paste0('qsub -P ', project, " -N ",name ," -e ", errors, ' -l m_mem_free=', mem_free, 'G', " -l fthread=", threads, " -l h_rt=", runtime, " -q ", q, " -l archive=True ")
    system(paste(sys_sub, shell, split_epi, output_version, source_me, ds, type, round))
    print(paste(sys_sub, shell, split_epi, output_version, source_me, ds, type, round))
  }
  
} else if (process == 1.5) {
  # STEP 1.5a & b------------------------------------------------
  errors <- paste0("FILEPATH")
  if (!file.exists(errors)){dir.create(errors,recursive=T)}
  threads <- 1
  mem_free <- 2.5
  runtime <- "5:00:00"
  
  target_mes<-map[grepl("alb",source_me_name),target_me_id]
  for (target_me in target_mes){
    name<- paste0('fix_alb_splits_', target_me)
    sys_sub<- paste0('qsub -P ', project, " -N ",name ," -e ", errors, ' -l m_mem_free=', mem_free, 'G', " -l fthread=", threads, " -l h_rt=", runtime, " -q ", q, " -l archive=True ")
    system(paste(sys_sub, py_shell, fix_alb, target_me, output_version, type))
    print(paste(sys_sub, py_shell, fix_alb, target_me, output_version, type))
  }
  
  # WHEN STAGE 3 AND 3-5 JOBS ARE DONE: Bind incidece from stage 3-5 to prevalence for stage 3 -----------------------------------
  errors <- paste0("FILEPATH")
  if (!file.exists(errors)){dir.create(errors,recursive=T)}
  target_mes<-unique(map[grepl("stage III",source_me_name),target_me_id])
  for (target_me in target_mes){
    name<- paste0('append_inc_prev_', target_me)
    sys_sub<- paste0('qsub -P ', project, " -N ",name ," -e ", errors, ' -l m_mem_free=', mem_free, 'G', " -l fthread=", threads, " -l h_rt=", runtime, " -q ", q, " -l archive=True ")
    system(paste(sys_sub, py_shell, append_inc_prev, target_me, output_version))
    print(paste(sys_sub, py_shell, append_inc_prev, target_me, output_version))
  }
  
} else if (process == 2) {
  message("Saving Epi Splits -------------------------------------------------------------------------")
  # Process 2 Save Results ----------------------------------------------------------------
  errors <- paste0("FILEPATH")
  if (!file.exists(errors)){dir.create(errors,recursive=T)}
  # Save it
  threads <- 10
  mem_free <- 50
  runtime <- "6:00:00"
  outputs<-unique(map[,target_me_id])
  
  for (output in outputs){
    out_dir<-paste0("FILEPATH")
    name<- paste0('save_ckd_epi_splits_',output)
    bid <- map$bid[map$target_me_id==output]
    cvid <- map$cvid[map$target_me_id==output]
    sys_sub<- paste0('qsub -P ', project, " -N ",name ," -e ", errors, ' -l m_mem_free=', mem_free, 'G', " -l fthread=", threads, " -l h_rt=", runtime, " -q ", q)
    system(paste(sys_sub, shell, save_epi, out_dir, description, output, ds, bid, cvid, round))
    print(paste(sys_sub, shell, save_epi, out_dir, description, output, ds, bid, cvid, round))
  }
} else {
  stop(paste0("Process value ", process, " is not a valid argument."))
}