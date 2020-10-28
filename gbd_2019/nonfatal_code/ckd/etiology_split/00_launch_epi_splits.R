# ---HEADER--------------------------------------------------------------------------------------------------
# Author: Carrie Purcell
# Update: Paul Briant and Alton Lu
# Date: 12/24/2019 (chrismtas eve work cry)
# Project: Split CKD epi model
# NOTE: JOB HOLDS ARE NOT BUILT INTO THIS SCRIPT. DON'T QSUB THIS SCRIPT ITSELF. I WROTE IT TO RUN INTERACTIVELY 
# FROM RSTUDIO IDE. NEED TO WAIT FOR THE SPLIT JOBS TO FINISH BEFORE LAUNCHING THE SAVE JOBS.
# GBD 2019 - NEED TO UPDATE MAP FOR DECOMP STEPs 3+ WHEN ANEMIA INPUT MEs CHANGE
# -----
# Updated to run on new cluster, smoother as well.
#
#------------------------------------------------------------------------------------------------------------

# ---SETTINGS------------------------------------------------------------------------------------------------
rm(list=ls())
# Versioning 
output_version <- 2
# Description
description <- "first_full_split_for_prelim_EPIC"
# Decomp step
ds <- "iterative"
# stages_ds <- "step4"
# esrd_ds <- "step3"
# Run 1 first, then 1.5, then 2.  
process <- 2
round <- "7"

#-------------------------------------------------------------------------------------------------------------

# ---CONFIG--------------------------------------------------------------------------------------------------
date<-gsub("_", "_", Sys.Date())
user <- Sys.info()["user"]
# set runtime configuration 
if (Sys.info()['sysname'] == 'Linux') {
  j_root <- '/home/j/' 
  code_general <- '/share/epi/ckd/ckd_code'
  share_path <- code_general
} else { 
  stop("This code needs to be run on the cluster")
}

#System settings
threads <- 30
mem_free <- 60
runtime <- "24:00:00"
q <- "long.q"
project <- "proj_yld"

# Shell
shell <- '/share/singularity-images/health_fin/forecasting/shells/health_fin_forecasting_shell_singularity.sh'
py_shell<- paste0(code_general, '/shells/py_shell.sh')

#Scripts
split_epi <- paste0(share_path, '/etiology_splits/epi/01_split_epi_mod.R')
fix_alb <- paste0(share_path, '/etiology_splits/epi/01.5_fix_alb_outputs.py')
append_inc_prev <- paste0(share_path, '/etiology_splits/epi/01.5_append_inc_prev.py')
save_epi <- paste0(share_path, '/etiology_splits/epi/02_save_ckd_epi.R')

# load packages 
require(data.table)
require(openxlsx,lib.loc = paste0(share_path,"rlibs"))
library(openxlsx)
#-------------------------------------------------------------------------------------------------------------


# ---SETTINGS-------------------------------------------------------------------------------------------------
# choose a type to run

type <- "ckd5hf" #options are: "all", "stages", "esrd", "ckd5hf" to run a subset of CKD

if (type == "all") {
  map<-as.data.table(read.xlsx(paste0(share_path,"/etiology_splits/epi/me_measure_map.xlsx")))
} else if (type == "stages") {
  map<-as.data.table(read.xlsx(paste0(share_path,"/etiology_splits/epi/stages_me_measure_map.xlsx")))
} else if (type == "alb") {
  map <- as.data.table(read.xlsx(paste0(share_path,"/etiology_splits/epi/me_measure_map_stages_album_only.xlsx")))
} else if (type == "ckd5hf") {
  map <- as.data.table(read.xlsx(paste0(share_path,"/etiology_splits/epi/ckd5hf_me_measure_map.xlsx")))
} else {
  map<-as.data.table(read.xlsx(paste0(share_path,"/etiology_splits/epi/esrd_me_measure_map.xlsx")))
}

# set source mes 
source_mes<-unique(map[,source_me_id])

# --- LAUNCH SPLIT AND SAVE-----------------------------------------------------------------------------------
# STEP 1 Launch split jobs-------------------------------------------------------
if (process == 1) {
  #   errors <- paste0("/share/scratch/users/",user,"/ylds/ckd/errors/step9_etiology_splits/part1")
  #   if (!file.exists(errors)){dir.create(errors,recursive=T)}
  #   for (source_me in source_mes){
  #     jname<- paste0('split_ckd_epi_mod_',source_me)
  #     sys_sub<- paste0('qsub -P ', project, " -N ",jname ," -e ", errors, ' -l m_mem_free=', mem_free, 'G', " -l fthread=", threads, " -l h_rt=", runtime, " -q ", q)
  #     system(paste(sys_sub, shell, split_epi, output_version, source_me, stages_ds, esrd_ds))
  #     print(paste(sys_sub, shell, split_epi, output_version, source_me, stages_ds, esrd_ds))
  #   }
  errors <- paste0("/share/scratch/users/",user,"/ckd/errors/step9_etiology_splits/part1")
  if (!file.exists(errors)){dir.create(errors,recursive=T)}
  for (source_me in source_mes){
    jname<- paste0('split_ckd_epi_mod_',source_me)
    sys_sub<- paste0('qsub -P ', project, " -N ",jname ," -e ", errors, ' -l m_mem_free=', mem_free, 'G', " -l fthread=", threads, " -l h_rt=", runtime, " -q ", q, " -l archive=True ")
    system(paste(sys_sub, shell, split_epi, output_version, source_me, ds, type, round))
    print(paste(sys_sub, shell, split_epi, output_version, source_me, ds, type, round))
  }
  
} else if (process == 1.5) {
  # STEP 1.5a & b------------------------------------------------
  # WHEN ALB JOBS ARE DONE: Fix albuminuria splits: proportion models can't feed into EPIC -- change split results from 
  # measure id 18 to 5
  errors <- paste0("/share/scratch/users/",user,"/ckd/errors/step9_etiology_splits/part1_2a")
  if (!file.exists(errors)){dir.create(errors,recursive=T)}
  threads <- 1
  mem_free <- 2.5
  runtime <- "5:00:00"
  
  target_mes<-map[grepl("alb",source_me_name),target_me_id]
  for (target_me in target_mes){
    jname<- paste0('fix_alb_splits_', target_me)
    sys_sub<- paste0('qsub -P ', project, " -N ",jname ," -e ", errors, ' -l m_mem_free=', mem_free, 'G', " -l fthread=", threads, " -l h_rt=", runtime, " -q ", q, " -l archive=True ")
    system(paste(sys_sub, py_shell, fix_alb, target_me, output_version, type))
    print(paste(sys_sub, py_shell, fix_alb, target_me, output_version, type))
  }
  
  # WHEN STAGE 3 AND 3-5 JOBS ARE DONE: Bind incidece from stage 3-5 to prevalence for stage 3 -----------------------------------
  errors <- paste0("/share/scratch/users/",user,"/ckd/errors/step9_etiology_splits/part1_2b")
  if (!file.exists(errors)){dir.create(errors,recursive=T)}
  target_mes<-unique(map[grepl("stage III",source_me_name),target_me_id])
  for (target_me in target_mes){
    jname<- paste0('append_inc_prev_', target_me)
    sys_sub<- paste0('qsub -P ', project, " -N ",jname ," -e ", errors, ' -l m_mem_free=', mem_free, 'G', " -l fthread=", threads, " -l h_rt=", runtime, " -q ", q, " -l archive=True ")
    system(paste(sys_sub, py_shell, append_inc_prev, target_me, output_version))
    print(paste(sys_sub, py_shell, append_inc_prev, target_me, output_version))
  }
  
} else if (process == 2) {
  message("Saving Epi Splits -------------------------------------------------------------------------")
  # Process 2 Save Results ----------------------------------------------------------------
  errors <- paste0("/share/scratch/users/",user,"/ckd/errors/step9_etiology_splits/part2")
  if (!file.exists(errors)){dir.create(errors,recursive=T)}
  # Save it
  threads <- 10
  mem_free <- 50
  runtime <- "6:00:00"
  outputs<-unique(map[,target_me_id])
  
  for (output in outputs){
    out_dir<-paste0("/ihme/epi/ckd/ckd_epi_splits/",output_version,"/to_save/",output)
    jname<- paste0('save_ckd_epi_splits_',output)
    bid <- map$bid[map$target_me_id==output]
    cvid <- map$cvid[map$target_me_id==output]
    sys_sub<- paste0('qsub -P ', project, " -N ",jname ," -e ", errors, ' -l m_mem_free=', mem_free, 'G', " -l fthread=", threads, " -l h_rt=", runtime, " -q ", q)
    system(paste(sys_sub, shell, save_epi, out_dir, description, output, ds, bid, cvid, round))
    print(paste(sys_sub, shell, save_epi, out_dir, description, output, ds, bid, cvid, round))
  }
} else {
  stop(paste0("Process value ", process, " is not a valid argument."))
}

#-------------------------------------------------------------------------------------------------------------

### Checking H5 files in outputs of splits or albuminuria process

#library(rhdf5)
#h5_location <- paste0("/ihme/epi/ckd/ckd_epi_splits/16/to_save/19835/10.h5")
#data <- h5read(h5_location)
