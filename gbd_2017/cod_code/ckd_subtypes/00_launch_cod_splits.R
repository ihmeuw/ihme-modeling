# ---HEADER--------------------------------------------------------------------------------------------------
# Date: 12/12/2017
# Project: Split CKD cod model
#------------------------------------------------------------------------------------------------------------

# ---SETTINGS------------------------------------------------------------------------------------------------
rm(list=ls())
# Versioning 
output_version<-2
# Set description
description<-"split_ckd_06_28_18"
#-------------------------------------------------------------------------------------------------------------

# ---CONFIG--------------------------------------------------------------------------------------------------
# set runtime configuration 
if (Sys.info()['sysname'] == 'Linux') {
  j_root <- 'FILEPATH' 
  h_root <- 'FILEPATH'
} else { 
  j_root <- 'FILEPATH'
  h_root <- 'FILEPATH'
}

# Source shared functions
source(paste0(h_root,'FILEPATH'))
source_shared_functions("get_demographics")

#System settings
slots<- 20
mem <- slots*2 
shell <- paste0(h_root, 'FILEPATH')
script_01 <- paste0(h_root, 'FILEPATH')
script_02 <- paste0(h_root, 'FILEPATH')
script_03<-paste0(h_root, 'FILEPATH')
project <- '-P proj_custom_models ' 
sge_output_dir <- '-o FILEPATH -e FILEPATH '

# load packages 
require(data.table)
#-------------------------------------------------------------------------------------------------------------

# ---SETTINGS-------------------------------------------------------------------------------------------------
#specify output directory 
out_dir<-paste0('FILEPATH',output_version)
dir.create(out_dir,recursive = TRUE)
#-------------------------------------------------------------------------------------------------------------

# --- LAUNCH SPLIT AND SAVE-----------------------------------------------------------------------------------
cause_ids<-c(997,998,591,592,593)
slots<-5 
mem_free<-slots*2
for (id in cause_ids){
  out_dir_id<-paste0(out_dir,"/",id)
  job_name<- paste0('-N delete_me_',id)
  sys_sub<- paste0('qsub -cwd ', project, sge_output_dir, job_name, ' -pe multi_slot ',slots, ' -l mem_free=', mem, 'G')
  system(paste(sys_sub, shell, script_03, out_dir_id, id))
}

slots<-20
mem_free<-slots*2
for (id in cause_ids){
  out_dir_id<-paste0(out_dir,"/",id)
  job_name<- paste0('-N save_cod_etio_',id)
  sys_sub<- paste0('qsub -cwd ', project, sge_output_dir, job_name, ' -pe multi_slot ',slots, ' -l mem_free=', mem, 'G')
  system(paste(sys_sub, shell, script_02, out_dir_id, description, id))
  print(paste(sys_sub, shell, script_02, out_dir_id, description, id))
}
#-------------------------------------------------------------------------------------------------------------


