# ---HEADER--------------------------------------------------------------------------------------------------
# Author: Carrie Purcell
# Date: 12/12/2016
# Project: Save CKD cod splits
#------------------------------------------------------------------------------------------------------------

# ---CONFIG--------------------------------------------------------------------------------------------------
# set runtime configuration 
if (Sys.info()['sysname'] == 'Linux') {
  j_root <- 'FILEPATH' 
  h_root <- 'FILEPATH'
} else { 
  j_root <- 'FILEPATH'
  h_root <- 'FILEPATH'
}

# source functions
source(paste0(h_root,'FILEPATH'))
source_shared_functions("save_results_cod")

# pass args 
args<-commandArgs(trailingOnly = TRUE)
out_dir<-args[1]
description<- args[2]
id<-as.numeric(args[3])
file_pattern<-"death_{location_id}.csv"

print(paste("cause id : ",id))
print(paste("description :",description))
print(paste("input_dir : ", out_dir))
print(paste("file pattern: ", file_pattern))
#-------------------------------------------------------------------------------------------------------------

# ---SPLIT----------------------------------------------------------------------------------------------------
save_results_cod(input_dir = out_dir,input_file_pattern =file_pattern ,cause_id = id,
                 description = description,mark_best = T)
#-------------------------------------------------------------------------------------------------------------
