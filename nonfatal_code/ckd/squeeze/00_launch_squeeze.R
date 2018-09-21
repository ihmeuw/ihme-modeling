# ---HEADER--------------------------------------------------------------------------------------------------
# Author: 
# Date: 5/22/2016
# Project: CKD Envelope Readjustment 
#------------------------------------------------------------------------------------------------------------

# ---SETTINGS------------------------------------------------------------------------------------------------
# Versioning 
output_version<-6
#-------------------------------------------------------------------------------------------------------------

# ---CONFIG--------------------------------------------------------------------------------------------------
# set runtime configuration 
if (Sys.info()['sysname'] == 'Linux') {
  j_root <- '/home/j/' 
  h_root <- '/homes/USERNAME/'
} else { 
  j_root <- 'J:/'
  h_root <- 'H:/'
}

#System settings
slots<- 5
mem <- slots*2 
shell <- FILEPATH
script_01 <- FILEPATH
script_02 <- FILEPATH
project <- '-P proj_custom_models ' 
sge_output_dir <- '-o FILEPATH -e FILEPATH '

# load packages 
require(data.table)

# source functions
source(FILEPATH) #get_location_metadata
#-------------------------------------------------------------------------------------------------------------


# ---SETTINGS-------------------------------------------------------------------------------------------------
#Create list of most detailed locations to parallelize over
location_dt<- get_location_metadata(location_set_id = 9)
location_dt<-location_dt[most_detailed==1,list(location_id,location_name,location_type)] 
locations<-location_dt[,location_id]

#specify acause 
acause<-'ckd' 
#-------------------------------------------------------------------------------------------------------------

# --- LAUNCH SQUEEZE--------------------------------------------------------------------------------
  for (loc in locations) {
    #create qsub
    job_name<- paste0('-N squeeze_',loc)
    sys_sub<- paste0('qsub -cwd ', project, sge_output_dir, job_name, ' -pe multi_slot ',slots, ' -l mem_free=', mem, 'G')
    system(paste(sys_sub, shell, script_01, loc, output_version))
    print(paste(sys_sub, shell, script_01, loc, output_version))
  }
#-------------------------------------------------------------------------------------------------------------


