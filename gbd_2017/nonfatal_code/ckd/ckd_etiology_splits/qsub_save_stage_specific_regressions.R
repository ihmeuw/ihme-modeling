# ---HEADER--------------------------------------------------------------------------------------------------
# Date: 12/12/2017
# Project: Split CKD epi model
#------------------------------------------------------------------------------------------------------------

# ---SETTINGS------------------------------------------------------------------------------------------------
rm(list=ls())
description<-"save_results"
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
slots<-20
mem<-slots*2
shell <- paste0(h_root, 'FILEPATH')

#Scripts
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
source_mes<-unique(map[grepl("end-stage",source_me_name)==F,proportion_me_id])
#-------------------------------------------------------------------------------------------------------------

# --- SAVE MODELS---------------------------------------------------------------------------------------------
for (me in source_mes){
  job_name<- paste0('-N save_etio_results_',me)
  sys_sub<- paste0('qsub -cwd ', project, sge_output_dir, job_name, ' -pe multi_slot ',slots, ' -l mem_free=', mem, 'G')
  system(paste(sys_sub, shell, save_epi, description, me))
}
#-------------------------------------------------------------------------------------------------------------


