# ---HEADER--------------------------------------------------------------------------------------------------
# Date: 5/22/2016
# Project: CKD Envelope Readjustment 
#------------------------------------------------------------------------------------------------------------

# ---SETTINGS------------------------------------------------------------------------------------------------
rm(list=ls())
# Versioning 
output_version<-1
# Description for saving results 
description<-"squeeze_mvids"
# Should these models be bested?
best<-F
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
slots<- 5
mem <- slots*2 
shell <- paste0(h_root, 'FILEPATH')
script_01 <- paste0(h_root, 'FILEPATH')
script_02 <- paste0(h_root, 'FILEPATH')
project <- '-P proj_custom_models ' 
sge_output_dir <- '-o FILEPATH -e FILEPATH '

# load packages 
require(data.table)

# source functions
source(paste0(h_root,'FILEPATH.R'))
source_shared_functions(c("get_demographics"))
#-------------------------------------------------------------------------------------------------------------

# ---SETTINGS-------------------------------------------------------------------------------------------------
#Create list of most detailed locations to parallelize over
demographics<-get_demographics("epi")
locations<-demographics$location_id

#List of squeezed mes to save 
me_ids<-c(10732,10733,10734)

#Create output directories 
lapply(me_ids,function(x) assign(paste0("out_dir_",x),paste0('/FILEPATH',x,'/',output_version),envir = globalenv()))
lapply(me_ids,function(x) dir.create(path=get(paste0("out_dir_",x)),recursive = T))
#-------------------------------------------------------------------------------------------------------------

# --- LAUNCH SQUEEZE AND SAVE---------------------------------------------------------------------------------
for (loc in locations) {
  #create qsub
  job_name<- paste0('-N squeeze_',loc)
  sys_sub<- paste0('qsub -cwd ', project, sge_output_dir, job_name, ' -pe multi_slot ',slots, ' -l mem_free=', mem, 'G')
  system(paste(sys_sub, shell, script_01, loc, out_dir_10732, out_dir_10733, out_dir_10734))
  print(paste(sys_sub, shell, script_01, loc, out_dir_10732, out_dir_10733, out_dir_10734))
}

check<-check_missing_locs(out_dir_10733,filepattern = "{location_id}.csv",team = "epi")

slots<- 20
mem <- slots*2

for(me in me_ids){
  out_dir<-get(paste0("out_dir_",me))
  job_name<-paste0("-N save_envelope_squeeze_",me)
  sys_sub<- paste0('qsub -cwd ', project, sge_output_dir, job_name, ' -pe multi_slot ',slots, ' -l mem_free=', mem, 'G')
  system(paste(sys_sub, shell, script_02, me, out_dir, description,best))
  print(paste(sys_sub, shell, script_02, me, out_dir, description,best))
}

#-------------------------------------------------------------------------------------------------------------


