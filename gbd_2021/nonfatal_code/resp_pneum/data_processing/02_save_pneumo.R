message("Starting to save pneumos ----------------------------------")

args <- commandArgs(trailingOnly = TRUE)
path_to_map <- args[1]
meid_to_save <- args[2]
directory_with_files <- args[3]
model_description <- args[4]

message("Path to mapping ", path_to_map)
message("Saving model to ", meid_to_save)
message("Saving files from ", directory_with_files)

mapping <- read.csv(path_to_map, stringsAsFactors = FALSE)

source("FILEPATHsave_results_epi.R")

message("Starting Save")
save_results_epi(input_dir = directory_with_files,
                 input_file_pattern = "{location_id}.csv",
                 modelable_entity_id = meid_to_save,
                 description = model_description,
                 measure_id = c(5,6),
                 mark_best = T,
                 bundle_id = mapping$bundle_id[1],
                 crosswalk_version_id = mapping$crosswalk_version_id[1],
                 decomp_step = 'iterative',
                 gbd_round_id = 7)

message("***Finished***")