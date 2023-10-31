# ---HEADER--------------------------------------------------------------------------------------------------
# USERNAME
# Date Created: 12/12/2016
# Project: Split CKD cod model 
#------------------------------------------------------------------------------------------------------------

# ---CONFIG--------------------------------------------------------------------------------------------------
# require functions
require(data.table)
require(openxlsx)
code_general <- "FILEPATH"

# source functions
source(paste0(code_general,'/function_lib.R'))
source_shared_functions(c("split_epi_model"))

# read in map of modelable entities to measure id 
map <- as.data.table(read.xlsx(paste0("FILEPATH")))

# pass args 
args<-commandArgs(trailingOnly = TRUE)
output_version<-args[1]
source_me<-as.numeric(args[2])
ds <- args[3]

# ---SPLIT----------------------------------------------------------------------------------------------------
target_mes<-unique(map[source_me_id==source_me,target_me_id])
prop_mes<-unique(map[source_me_id==source_me,proportion_me_id])
split_meas_id<-unique(map[source_me_id==source_me,measure_id])
folder_name<-paste(sort(unique(map[source_me_id==source_me,folder_name])),collapse = "_")

# specify output directory
out_dir<-paste0("FILEPATH")
dir.create(out_dir,recursive = T)

message(paste0("Splitting ", source_me))
split_epi_model(source_meid=source_me,
                target_meids=target_mes,
                split_measure_ids = split_meas_id,
                prop_meas_id = 18,
                prop_meids=prop_mes,
                output_dir = out_dir,
                decomp_step = ds)

#-------------------------------------------------------------------------------------------------------------
split_epi_model(source_meid=25693,
                target_meids=c(26013, 26018, 26010, 26009, 26011),
                split_measure_ids = 5,
                prop_meas_id = 18,
                prop_meids=c(20372, 20373, 20374, 20375, 20376),
                output_dir = "FILEPATH",
                decomp_step = "iterative")