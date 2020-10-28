# ---HEADER--------------------------------------------------------------------------------------------------
# Author: Carrie Purcell
# Date: 12/12/2016
# Project: Split CKD cod model 
#------------------------------------------------------------------------------------------------------------

# ---CONFIG--------------------------------------------------------------------------------------------------
rm(list=ls())
date<-gsub("_", "_", Sys.Date())
user <- Sys.info()["user"]
# set runtime configuration 
if (Sys.info()['sysname'] == 'Linux') {
  j_root <- '/home/j/' 
  code_general <- '/share/epi/ckd/ckd_code'
  share_path <- code_general
} else { 
  stop("This code needs to be run on the cluster")
}

# require functions
require(data.table)
require(openxlsx)

# source functions
source(paste0(code_general,'/function_lib.R'))
# Normal path to shared functions.
source_shared_functions(c("split_epi_model"))

# read in map of ME to measure id 
map <- as.data.table(read.xlsx(paste0(share_path,"/etiology_splits/epi/me_measure_map.xlsx")))
# stages_map <- as.data.table(read.xlsx(paste0(share_path,"/etiology_splits/epi/stages_me_measure_map.xlsx")))
# map <- as.data.table(read.xlsx(paste0(share_path,"/etiology_splits/epi/esrd_me_measure_map.xlsx")))
# pass args 
args<-commandArgs(trailingOnly = TRUE)
output_version<-args[1]
source_me<-as.numeric(args[2])
ds <- args[3]
# stages_ds<-args[3]
# esrd_ds<-args[4]

#-------------------------------------------------------------------------------------------------------------

# ---SPLIT----------------------------------------------------------------------------------------------------
# set options for split 

target_mes<-unique(map[source_me_id==source_me,target_me_id])
prop_mes<-unique(map[source_me_id==source_me,proportion_me_id])
split_meas_id<-unique(map[source_me_id==source_me,measure_id])
folder_name<-paste(sort(unique(map[source_me_id==source_me,folder_name])),collapse = "_")

# specify output directory
out_dir<-paste0('/ihme/epi/ckd/ckd_epi_splits/',output_version,"/",folder_name)
dir.create(out_dir,recursive = T)

message(paste0("Splitting ", source_me))
split_epi_model(source_meid=source_me,
                target_meids=target_mes,
                split_measure_ids = split_meas_id,
                prop_meas_id = 18,
                prop_meids=prop_mes,
                output_dir = out_dir,
                decomp_step = ds)

# return to one call of split epi model for GBD 2020
# if (source_me %in% c(2020, 24686)){
#   target_mes<-unique(esrd_map[source_me_id==source_me,target_me_id])
#   prop_mes<-unique(esrd_map[source_me_id==source_me,proportion_me_id])
#   split_meas_id<-unique(esrd_map[source_me_id==source_me,measure_id])
#   folder_name<-paste(sort(unique(esrd_map[source_me_id==source_me,folder_name])),collapse = "_")
# 
#   # specify output directory
#   out_dir<-paste0('/ihme/epi/ckd/ckd_epi_splits/',output_version,"/",folder_name)
#   dir.create(out_dir,recursive = T)
# 
#   message("Splitting for either Dialysis or Transplant")
#   split_epi_model(source_meid=source_me,
#                   target_meids=target_mes,
#                   split_measure_ids = split_meas_id,
#                   prop_meas_id = 18,
#                   prop_meids=prop_mes,
#                   output_dir = out_dir,
#                   decomp_step = esrd_ds)
# } else {
#   target_mes<-unique(stages_map[source_me_id==source_me,target_me_id])
#   prop_mes<-unique(stages_map[source_me_id==source_me,proportion_me_id])
#   split_meas_id<-unique(stages_map[source_me_id==source_me,measure_id])
#   folder_name<-paste(sort(unique(stages_map[source_me_id==source_me,folder_name])),collapse = "_")
# 
#   # specify output directory
#   out_dir<-paste0('/ihme/epi/ckd/ckd_epi_splits/',output_version,"/",folder_name)
#   dir.create(out_dir,recursive = T)
# 
#   message("Splitting for either Albuminuria or Stages")
#   split_epi_model(source_meid=source_me,
#                   target_meids=target_mes,
#                   split_measure_ids = split_meas_id,
#                   prop_meas_id = 18,
#                   prop_meids=prop_mes,
#                   output_dir = out_dir,
#                   decomp_step = stages_ds)
# }
  
#-------------------------------------------------------------------------------------------------------------
split_epi_model(source_meid=25693,
                target_meids=c(26013, 26018, 26010, 26009, 26011),
                split_measure_ids = 5,
                prop_meas_id = 18,
                prop_meids=c(20372, 20373, 20374, 20375, 20376),
                output_dir = "/ihme/epi/ckd/ckd_epi_splits/1/to_save",
                decomp_step = "iterative")
