# -----------------------------------------------------------------------------------------------------------
# Purpose: Proportionally split anemia severities 
#------------------------------------------------------------------------------------------------------------

# ---CONFIG--------------------------------------------------------------------------------------------------
rm(list=ls())

# require functions
require(data.table)
require(openxlsx)

# source functions
source(paste0(code_general, FILEPATH)
functions_dir <- FILEPATH
source(paste0(functions_dir, "split_epi_model.R"))

# read in map of ME to measure id 
# TODO: update this
map<-as.data.table(read.xlsx(FILEPATH))
# specify output directory 

# pass args 
args<-commandArgs(trailingOnly = TRUE)
output_version<-args[1]
source_me<-as.numeric(args[2])
ds<-args[3]

out_dir <- FILEPATH
#-------------------------------------------------------------------------------------------------------------

# ---SPLIT----------------------------------------------------------------------------------------------------
# set options for split 
target_mes<-unique(map[source_me_id==source_me, target_me_id])
prop_mes<-unique(map[source_me_id==source_me, proportion_me_id])
split_meas_id<-unique(map[source_me_id==source_me, measure_id])
folder_name<-paste(sort(unique(map[source_me_id==source_me, folder_name])), collapse = "_")

if (!file.exists(out_dir)){dir.create(out_dir, recursive=T)}

message("Initiating splits")
split_epi_model(source_meid=source_me,
                target_meids=target_mes,
                split_measure_ids = split_meas_id,
                prop_meas_id = 18,
                prop_meids=prop_mes,
                output_dir = out_dir,
                gbd_round_id = 6, 
                decomp_step = ds)
#-------------------------------------------------------------------------------------------------------------