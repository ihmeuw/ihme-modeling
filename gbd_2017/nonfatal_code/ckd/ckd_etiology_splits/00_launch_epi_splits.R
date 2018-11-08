# ---HEADER--------------------------------------------------------------------------------------------------
# Date: 12/12/2017
# Project: Split CKD epi model
#------------------------------------------------------------------------------------------------------------

# ---SETTINGS------------------------------------------------------------------------------------------------
rm(list=ls())
# Versioning 
output_version<-1
# Description
description<-"stage_specific_splits"
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

#System settings
slots<- 20
mem <- slots*2 
shell <- 'FILEPATH'
py_shell<- paste0(h_root, 'FILEPATH')
#Scripts
split_epi <- paste0(h_root, 'FILEPATH')
save_epi <- paste0(h_root, 'FILEPATH')
project <- '-P proj_custom_models ' 
sge_output_dir <- '-o FILEPATH -e FILEPATH '

# load packages 
require(data.table)
require(openxlsx)
#-------------------------------------------------------------------------------------------------------------


# ---SETTINGS-------------------------------------------------------------------------------------------------
# read in map of ME to measure id 
map<-as.data.table(read.xlsx(paste0(h_root,"FILEPATH.xlsx")))

# set source mes 
source_mes<-unique(map[,source_me_id])
#-------------------------------------------------------------------------------------------------------------

# --- LAUNCH SPLIT AND SAVE-----------------------------------------------------------------------------------
# Launch split jobs 
for (source_me in source_mes){
  job_name<- paste0('-N split_ckd_epi_mod_',source_me)
  sys_sub<- paste0('qsub -cwd ', project, sge_output_dir, job_name, ' -pe multi_slot ',slots, ' -l mem_free=', mem, 'G')
  system(paste(sys_sub, shell, split_epi, output_version, source_me))
  print(paste(sys_sub, shell, split_epi, output_version, source_me))
}

slots<-20
mem<-slots*2
outputs<-unique(map[,target_me_id])

for (output in outputs){
  out_dir<-paste0("/ihme/epi/ckd/ckd_epi_splits/",output_version,"/to_save/",output)
  job_name<- paste0('-N save_ckd_epi_splits_',output)
  sys_sub<- paste0('qsub -cwd ', project, sge_output_dir, job_name, ' -pe multi_slot ',slots, ' -l mem_free=', mem, 'G')
  system(paste(sys_sub, shell, save_epi, out_dir, description, output))
}
#-------------------------------------------------------------------------------------------------------------


