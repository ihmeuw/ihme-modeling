#--------------------------------------------------------------
# Name: USERNAME
# Date: 2018-07-19
# Project: IKF annual expsoures 
# Purpose: Interpolate DisMod models to get annualed exposures
# for IKF PAF calculation 
#--------------------------------------------------------------

message("Starting Interpolation Script")
source("FILEPATHfunction_lib.R")

# Parameters
args <- commandArgs(trailingOnly = T)

i <- args[1]
loc <- args[2]
me_map <- args[3]
func_lib <- args[4]
directory <- args[5]
ds <- args[6]
round <- as.numeric(args[7])

message(paste("i:", i))
message(paste("loc:", loc))
message(paste("me_map:", me_map))
message(paste("func_lib:", func_lib))
message(paste("directory:", directory))
message(paste("step:", ds))
message(paste("round:", round))


if (i == 10509) {
  measure_id <- 18
} else {
  measure_id <- 5
}
message(measure_id)


# setup -------------------------------------------------------
message("Set Up ---------------------------------------------------")
ckd_repo <- "FILEPATH"

require(data.table)
require(openxlsx)
func_lib <- paste0(ckd_repo,"function_lib.R")
source(func_lib)

source_shared_functions("interpolate")
map_id <- as.data.table(read.xlsx(me_map))

for (n_col in 1:ncol(map_id)) {
  col_name <- names(map_id)[n_col]
  assign(names(map_id)[n_col],map_id[input_me == i, get(col_name)])
}

message("Starting interpolation -------------------------------------")

dt <- interpolate(gbd_id_type = "modelable_entity_id",
                  gbd_id = i,
                  source = "epi",
                  measure_id = measure_id,
                  location_id = loc,
                  reporting_year_start = 1990,
                  reporting_year_end = 2019,
                  gbd_round_id = round,
                  decomp_step=ds,
                  status = "best")

dt[ , modelable_entity_id:= NULL]

outputs <- paste0("FILEPATH", directory, i, "/")

# Gives interpolated estimates of impaired kidney functions
message("Writing CSV ----------------------------------------")
message(paste("To directory:", paste0(outputs, loc,".csv")))
write.csv(dt, file.path(paste0(outputs, loc,".csv")), row.names = F)
