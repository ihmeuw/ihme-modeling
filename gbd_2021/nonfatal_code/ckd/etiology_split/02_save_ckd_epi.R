# ---HEADER--------------------------------------------------------------------------------------------------
# USERNAME
# Date Created: 12/28/2017
# Project: Save CKD epi splits  
#------------------------------------------------------------------------------------------------------------

# ---CONFIG--------------------------------------------------------------------------------------------------
# load packages 
require(data.table)
code_general <- "FILEPATH"
source(paste0(code_general,'/function_lib.R'))
source_shared_functions(c("save_results_epi"))
#-------------------------------------------------------------------------------------------------------------

#Pass arguments from qsub  
args<-commandArgs(trailingOnly = TRUE)
out_dir<-args[1]
descrip<-args[2]
output<-as.numeric(args[3])
ds<-args[4]
bid<-as.numeric(args[5])
cvid<-as.numeric(args[6])
round <- as.numeric(args[7])

if (output %in% c(19433,19434,3030,3031,3032)){
  save_results_epi(input_dir = out_dir,
                   input_file_pattern ="{location_id}.h5",
                   modelable_entity_id = output, 
                   description =descrip, 
                   mark_best = T,
                   measure_id = c(5,6),
                   decomp_step=ds,
                   bundle_id = bid,
                   crosswalk_version_id = cvid,
                   gbd_round_id = round)  
}else{
  save_results_epi(input_dir = out_dir,
                   input_file_pattern ="{location_id}.h5",
                   modelable_entity_id = output, 
                   description =descrip,
                   mark_best = T,
                   measure_id = 5,
                   decomp_step=ds,
                   bundle_id = bid,
                   crosswalk_version_id = cvid,
                   gbd_round_id = round)  
}