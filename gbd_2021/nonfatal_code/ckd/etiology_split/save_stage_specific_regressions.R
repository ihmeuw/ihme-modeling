# ---HEADER--------------------------------------------------------------------------------------------------
# USERNAME
# Date Created: 12/28/2017
# Project: Save CKD epi splits  
#------------------------------------------------------------------------------------------------------------

# ---CONFIG--------------------------------------------------------------------------------------------------
# load packages 
require(data.table)
require(openxlsx)

# source functions
code_general <- "FILEPATH"
source(paste0(code_general,'/function_lib.R'))
source_shared_functions(c("save_results_epi"))

# read in map
map<-as.data.table(read.xlsx(paste0("FILEPATH")))
#-------------------------------------------------------------------------------------------------------------
 
args<-commandArgs(trailingOnly = TRUE)
if(length(args)==0) {
  descrip <- description
} else {
  descrip<-args[1]
  me<-as.numeric(args[2])
  ds<-args[3]
  bid<-as.numeric(args[4])
  cvid<-as.numeric(args[5])
  date<-args[6]
}

stage<-unique(map[proportion_me_id==me,dir_stage])
etio<-unique(map[proportion_me_id==me,dir_etio])
dir<-paste0("FILEPATH")

print(paste("directory =",dir))
print(paste("me =",me))
save_results_epi(input_dir = dir, input_file_pattern ="{location_id}.csv",
                 modelable_entity_id = me, bundle_id = bid, crosswalk_version_id = cvid,
                 description =descrip, measure_id = 18, mark_best = T, decomp_step=ds, gbd_round_id = 7)