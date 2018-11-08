# ---INPUTS--------------------------------------------------------------------------------------------------
# clear workspace
rm(list=ls())

require(data.table)
if (Sys.info()['sysname'] == 'Linux') {
  j_root <- 'FILEPATH' 
  h_root <- 'FILEPATH'
} else { 
  j_root <- 'FILEPATH'
  h_root <- 'FILEPATH'
}
#------------------------------------------------------------------------------------------------------------

# ---CONFIG--------------------------------------------------------------------------------------------------
#System settings
slots<- 2
mem <- slots*2 
shell <- ('FILEPATH/shell.sh')
script <- paste0(h_root, 'FILEPATH/01_upload_hospital_data.R')
project <- '-P proj_custom_models ' 
sge_output_dir <- '-o FILEPATH -e FILEPATH '
#-------------------------------------------------------------------------------------------------------------

# ---SUBMIT---------------------------------------------------------------------------------------------------
for (i in c(4:4)){
  #create qsub
  job_name<- paste0('-N upload_',i)
  sys_sub<- paste0('qsub -cwd ', project, sge_output_dir, job_name, ' -pe multi_slot ',slots, ' -l mem_free=', mem, 'G')
  system(paste(sys_sub, shell, script, i))
  print(paste(sys_sub, shell, script, i))
}
#------------------------------------------------------------------------------------------------------------