# ---HEADER--------------------------------------------------------------------------------------------------
# Save output results for squeezes or saves
#------------------------------------------------------------------------------------------------------------

# ---CONFIG--------------------------------------------------------------------------------------------------
# set runtime configuration 

user <- Sys.info()["user"]

# load packages 
require(data.table)

# source functions
source('FILEPATH/function_lib.R')
source_shared_functions(c("save_results_epi"))
library(stringr)
#-------------------------------------------------------------------------------------------------------------

#Pass arguments from qsub  
args <- commandArgs(trailingOnly = TRUE)
me <- as.numeric(args[1])
file_locations <- args[2]
description <- args[3]
best <- args[4]
ds <- args[5]
round <- args[6]
measure <- as.numeric(unlist(strsplit(as.character(args[7]),',')))

message("Here are parameters passed")
message(paste("ME:", me))
message(paste("Draws saved at:", file_locations))
message(paste("description:", description))
message(paste("Mark Best?", best))
message(paste("step:", ds))

message("Starting Save Results -----------------------------------------------------")
save_results_epi(input_dir = file_locations,
                 input_file_pattern ="{location_id}.{extension}",
                 modelable_entity_id = me,
                 description = description,
                 measure_id = measure,
                 mark_best = best,
                 decomp_step = ds,
                 gbd_round_id = round)
message("Finished Save Results ----------------------------------------------------")