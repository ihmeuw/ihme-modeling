# ---HEADER--------------------------------------------------------------------------------------------------
# Date: 12/12/2016
# Project: Split CKD model 
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

# require functions
require(data.table)
require(openxlsx)

# source functions
source(paste0(h_root,'FILEPATH'))
source_shared_functions(c("split_epi_model"))

# read in map of ME to measure id 
map<-as.data.table(read.xlsx(paste0(h_root,"FILEPATH.xlsx")))

# pass args 
args<-commandArgs(trailingOnly = TRUE)
output_version<-args[1]
source_me<-as.numeric(args[2])
#-------------------------------------------------------------------------------------------------------------

# ---SPLIT----------------------------------------------------------------------------------------------------
# set options for split 
target_mes<-unique(map[source_me_id==source_me,target_me_id])
prop_mes<-unique(map[source_me_id==source_me,proportion_me_id])
split_meas_id<-unique(map[source_me_id==source_me,measure_id])
folder_name<-paste(sort(unique(map[source_me_id==source_me,folder_name])),collapse = "_")

# specify output directory 
out_dir<-paste0(FILEPATH)
dir.create(out_dir,recursive = TRUE)

split_epi_model(source_meid=source_me,
                 target_meids=target_mes,
                 split_meas_ids = split_meas_id,
                 prop_meas_id = 18,
                 prop_meids=prop_mes,
                 output_dir = out_dir)
#-------------------------------------------------------------------------------------------------------------
