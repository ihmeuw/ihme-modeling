# ---HEADER--------------------------------------------------------------------------------------------------
# Author: Carrie Purcell
# Date: 12/12/2017
# Project: Split CKD epi model
# NOTE: JOB HOLDS ARE NOT BUILT INTO THIS SCRIPT. DON'T QSUB THIS SCRIPT ITSELF. I WROTE IT TO RUN INTERACTIVELY 
# FROM RSTUDIO IDE. NEED TO WAIT FOR THE SPLIT JOBS TO FINISH BEFORE LAUNCHING THE SAVE JOBS.
#------------------------------------------------------------------------------------------------------------

# ---SETTINGS------------------------------------------------------------------------------------------------
rm(list=ls())
description<-"save_geisinger_results"
#-------------------------------------------------------------------------------------------------------------

# ---CONFIG--------------------------------------------------------------------------------------------------
# set runtime configuration 
if (Sys.info()['sysname'] == 'Linux') {
  j_root <- '/home/j/' 
  h_root <- '~/'
} else { 
  j_root <- 'J:/'
  h_root <- 'H:/'
}

#System settings
slots<-20
mem<-slots*2
shell <- paste0(h_root, 'code_general/_lib/shells/new_r_shell.sh')

#Scripts
save_epi <- paste0(h_root, 'ckd/etiology_splits/epi/save_stage_specific_regressions.R')
project <- '-P proj_custom_models ' 
sge_output_dir <- '-o /share/temp/sgeoutput/purcellc/output -e /share/temp/sgeoutput/purcellc/errors '

# load packages 
require(data.table)
require(openxlsx)
#-------------------------------------------------------------------------------------------------------------


# ---SETTINGS-------------------------------------------------------------------------------------------------
# read in map of ME to measure id 
map<-as.data.table(read.xlsx(paste0(h_root,"ckd/etiology_splits/epi/me_measure_map.xlsx")))

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


