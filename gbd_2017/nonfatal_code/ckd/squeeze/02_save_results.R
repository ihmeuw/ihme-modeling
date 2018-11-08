# ---HEADER--------------------------------------------------------------------------------------------------
# Author: Carrie Purcell
# Date: 12/12/2016
# Project: DM Envelope Readjustment 
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

# load packages 
require(data.table)

# source functions
source(paste0(h_root,'FILEPATH.R'))
source_shared_functions(c("save_results_epi"))
#-------------------------------------------------------------------------------------------------------------

#Pass arguments from qsub  
args<-commandArgs(trailingOnly = TRUE)
me<-as.numeric(args[1])
out_dir<-args[2]
description<-args[3]
best<-args[4]

save_results_epi(input_dir = out_dir,
                 input_file_pattern ="{location_id}.csv",
                 modelable_entity_id = me, 
                 description = description, 
                 measure_id = 5,
                 mark_best = best)