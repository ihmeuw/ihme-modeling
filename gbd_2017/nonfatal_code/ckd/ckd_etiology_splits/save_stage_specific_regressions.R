# ---HEADER--------------------------------------------------------------------------------------------------
# Author: Carrie Purcell
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
require(openxlsx)

# source functions
source(paste0(h_root,'FILEPATH.R'))
source_shared_functions(c("save_results_epi"))

# read in map
map<-as.data.table(read.xlsx(paste0(h_root,"FILEPATH.xlsx")))
#-------------------------------------------------------------------------------------------------------------

# pass arguments from qsub  
args<-commandArgs(trailingOnly = TRUE)
descrip<-args[1]
me<-as.numeric(args[2])

# create dir
stage<-unique(map[proportion_me_id==me,dir_stage])
etio<-unique(map[proportion_me_id==me,dir_etio])
dir<-paste0("FILEPATH",stage,"/",etio,"/")

save_results_epi(input_dir = dir, input_file_pattern ="{location_id}.csv",modelable_entity_id = me, description =descrip, mark_best = T,
                 measure_id = 18)


