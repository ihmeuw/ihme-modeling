# ---HEADER--------------------------------------------------------------------------------------------------
# Date: 12/28/2017
# Project: Save CKD epi splits  
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
source(paste0(h_root,'FILEPATH'))
source_shared_functions(c("save_results_epi"))
#-------------------------------------------------------------------------------------------------------------

#Pass arguments from qsub  
args<-commandArgs(trailingOnly = TRUE)
out_dir<-args[1]
descrip<-args[2]
output<-as.numeric(args[3])

if (output %in% c(19433,19434,3030,3031,3032)){
  save_results_epi(input_dir = out_dir,input_file_pattern ="{location_id}.h5",modelable_entity_id = output, description =descrip, mark_best = T,measure_id = c(5,6))  
}else{
  save_results_epi(input_dir = out_dir,input_file_pattern ="{location_id}.h5",modelable_entity_id = output, description =descrip, mark_best = T,measure_id = 5)
}

