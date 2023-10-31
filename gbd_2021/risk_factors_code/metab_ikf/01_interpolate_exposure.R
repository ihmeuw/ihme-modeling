#--------------------------------------------------------------
# Name: Alton Lu
# Date: 2020 April
# Used to interpolate a dismod model and save to a directory
#--------------------------------------------------------------

message("Starting Interpolation Script ----------")
require(data.table)
require(openxlsx)
args <- commandArgs(trailingOnly = T)

# Arguments passed
path_to_map <- args[2]
code_dir <- args[3]
ds <- args[4]
round <- as.numeric(args[5])

# Get parallelized locations
source(paste0(code_dir, "general_func_lib.R")) # array function package
source_shared_functions("interpolate")
getit <- job.array.child()
loc <- getit[[1]] # getting the location from array
loc <- as.numeric(loc)

message(paste("loc:", loc))
message(paste("path_to_map:", path_to_map))
message(paste("func_lib:", code_dir))
message(paste("step:", ds))
message(paste("round:", round))

mapping <- as.data.table(read.csv(path_to_map))
directory_to_save <- mapping$directory_to_save[1] # this is often where errors start



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
                    gbd_round_id = round,
                    decomp_step = ds,
                    status = "best")

  # remove meid column so we can save to new meid later
  dt[ , modelable_entity_id:= NULL]

  message("Saving interpolation into ", dir_save)
  ifelse(dir.exists(dir_save),dir_save, dir.create(dir_save))
  write.csv(dt, paste0(dir_save, loc, ".csv"), row.names = F)
}

message("Interpolation completed ---------------")
