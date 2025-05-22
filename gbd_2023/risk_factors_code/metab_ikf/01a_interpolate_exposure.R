#--------------------------------------------------------------
# Used to interpolate a dismod model and save to a directory
#--------------------------------------------------------------

message("Starting Interpolation Script ----------")
require(data.table)
require(openxlsx)

invisible(sapply(list.files("FILEPATH", full.names = T), source))

args<-commandArgs(trailingOnly = T)
loc <-  as.numeric(args[1])
path_to_map <- args[2]
release_id <- args[3]
directory_to_save <- args[4]

message(paste("loc:", loc))
message(paste("path_to_map:", path_to_map))
message(paste("release:", release_id))

mapping <- as.data.table(read.csv(path_to_map))

for (i in 1:nrow(mapping)) {
  meid <- mapping$source_meid[i]
  message(paste("Starting interpolation for ", meid))
  target_me_id <- mapping$target_me_id[i]
  measure_id <- as.numeric(unlist(strsplit(as.character(mapping$measure_id[i]),',')))
  dir_save <- paste0(directory_to_save, target_me_id, "/")
  
  dt <- interpolate(gbd_id_type = "modelable_entity_id",
                    gbd_id = meid,
                    source = "epi",
                    measure_id = measure_id,
                    location_id = loc,
                    reporting_year_start = mapping$year_start[1],
                    reporting_year_end = mapping$year_end[1],
                    release_id = release_id,
                    status = "best")
  
  # remove meid column so we can save to new meid later
  dt[ , modelable_entity_id:= NULL]
  
  # for stage 1-2, change from proportion to prevalence 
  if (measure_id == 18) {
    dt[, measure_id := 5]
  }
  
  message("Saving interpolation into ", dir_save)
  ifelse(dir.exists(dir_save),dir_save, dir.create(dir_save))
  write.csv(dt, paste0(dir_save, loc, ".csv"), row.names = F)
}

message("Interpolation completed ---------------")