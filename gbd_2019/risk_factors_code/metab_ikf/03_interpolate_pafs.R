### Interpolate PAF's if more needed

source("FILEPATH/interpolate.R")

message("Starting Interpolation Script -----------------------------------------------")
source("FILEPATH/function_lib.R")

# Parameters
args <- commandArgs(trailingOnly = T)

loc <- args[1]
me_map <- args[2]
func_lib <- args[3]
directory <- args[4]
ds <- args[5]
round <- as.numeric(args[6])
measure <- as.numeric(args[7])

message(paste("loc:", loc))
message(paste("me_map:", me_map))
message(paste("func_lib:", func_lib))
message(paste("directory:", directory))
message(paste("step:", ds))
message(paste("round:", round))
message(paste("measure:", measure))

directory <- "FILEPATH"

message("Starting interpolation")
data <- interpolate(gbd_id_type='rei_id', gbd_id=341, source='paf', location_id = loc, measure_id=measure,
                  gbd_round_id=6,decomp_step='step4', reporting_year_start=1990,
                  reporting_year_end=2019)

file_name <- paste0(directory, loc, "_", measure, ".csv")

message(paste("Saving file", file_name))
write.csv(data, file_name)
message("Completed ---------------------------------------------------------------")
