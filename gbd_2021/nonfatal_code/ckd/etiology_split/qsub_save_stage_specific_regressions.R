# ---HEADER--------------------------------------------------------------------------------------------------
# USERNAME
# Date Created: 12/12/2017
# Project: Split CKD epi model

# ---SETTINGS------------------------------------------------------------------------------------------------
description<-"DESCRIPTION"
#-------------------------------------------------------------------------------------------------------------

# ---CONFIG--------------------------------------------------------------------------------------------------

#System settings
slots<-20
mem<-slots*2
shell <- "SHELL"

#Scripts
save_epi <- source('save_stage_specific_regressions.R')
project <- '-P proj_custom_models ' 
sge_output_dir <- "FILEPATH"

# load packages 
require(data.table)
require(openxlsx)

# ---SETTINGS-------------------------------------------------------------------------------------------------
# read in map of modelable entity to measure id 
map<-as.data.table(read.xlsx(paste0("FILEPATH"))

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