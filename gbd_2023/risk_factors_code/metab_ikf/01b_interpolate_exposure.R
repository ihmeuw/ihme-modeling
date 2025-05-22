#--------------------------------------------------------------
# Used to save interpolation results
#--------------------------------------------------------------

message("Starting Save Results Script ----------")

invisible(sapply(list.files("FILEPATH", full.names = T), source))

args<-commandArgs(trailingOnly = T)
meid <-  as.numeric(args[1])
path_to_map <- args[2]
release_id <- args[3]
input_directory <- args[4]

mapping <- as.data.table(read.csv(path_to_map))

year_start <- unique(mapping$year_start)
year_end <- unique(mapping$year_end)

save_results_epi(input_dir = paste0(input_directory, meid, "/"),
                 input_file_pattern = "{location_id}.csv",
                 modelable_entity_id = meid,
                 bundle_id = mapping[target_me_id==meid,]$bundle_id,
                 crosswalk_version_id = mapping[target_me_id==meid,]$crosswalk_version_id,
                 description = "GBD 2023 final processed models, fixed loc mistakes, extended tw", # update description here
                 measure_id = mapping[target_me_id==meid,]$target_measure_id,
                 mark_best = TRUE,
                 release_id = release_id,
                 year_id = c(year_start:year_end))

message("Save Results completed ---------------")