##
## Author: USERNAME
## Date: DATE
##
## Purpose: Save acute stroke split ME IDs using save_results_epi

library(data.table)

#pull in arguments from parent script and assign them to variables
args <- commandArgs(trailingOnly = TRUE)
task_id <- ifelse(is.na(as.integer(Sys.getenv("SGE_TASK_ID"))), 1, as.integer(Sys.getenv("SGE_TASK_ID")))

gbd_round_id <- as.integer(args[2])
decomp_step <- as.character(args[3])
input_folder <- as.character(args[4])

args_path <- args[1]
args_path <- fread(args_path)

me_id <- as.integer(args_path[task_id, me_id])
me_folder <- as.character(args_path[task_id, me_folder])
bundle_id <- as.integer(args_path[task_id, bundle_id])
crosswalk_version_id <- as.integer(args_path[task_id, crosswalk_version_id])
parent_model <- as.integer(args_path[task_id, parent_model])
hf_model <- as.integer(args_path[task_id, hf_model])

print(args)

source("/FILEPATH/save_results_epi.R")

message = paste0("uploading acute stroke splits (parent stroke model ", parent_model, ", parent hf model ", hf_model, ")")

save_results_epi(modelable_entity_id=me_id, description=paste0(message),
                 measure_id=c(5, 6), mark_best=TRUE, gbd_round_id = gbd_round_id, decomp_step = decomp_step,
                 bundle_id = bundle_id, crosswalk_version_id = crosswalk_version_id,
                 input_file_pattern="{location_id}.csv",
                 input_dir=paste0(input_folder, me_folder, "/"))