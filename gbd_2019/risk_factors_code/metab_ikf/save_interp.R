#--------------------------------------------------------------
# Name: USERNAME
# Date: 2018-07-18
# Project: IKF exposure interpolation
# Purpose: Save interpolated IKF expsoure estimates
#--------------------------------------------------------------

# setup -------------------------------------------------------

require(data.table)
require(openxlsx)
ckd_repo<-"FILEPATH"
source(paste0(ckd_repo,"function_lib.R"))
source_shared_functions("save_results_epi")

args <- commandArgs(trailingOnly = T)
input <- as.numeric(args[1])
output <- as.numeric(args[2])
ds <- args[3]
file_location <- args[4]
year_start <- as.numeric(args[5])
year_end <- as.numeric(args[6])
descript <- args[7]

if (input == 10509) {
  measure_id <- 18
} else {
  measure_id <- 5
}

input_directory <- paste0("FILEPATH", file_location, input)

map_id <- as.data.table(read.xlsx(paste0(ckd_repo,"FILEPATH/me_map_lualton.xlsx")))

message(paste("input:", input))
message(paste("output:", input))
message(paste("step:", ds))
message(paste("file_location:", file_location))
message(paste("input directory:", input_directory))

for (n_col in 1:ncol(map_id)){
  col_name<-names(map_id)[n_col]
  assign(names(map_id)[n_col],map_id[input_me==i,get(col_name)])
}

year_ids <- seq(year_start,year_end,1)

save_results_epi(modelable_entity_id=output_me, input_dir=input_directory, input_file_pattern="{location_id}.csv",
                 description=descript, measure_id=measure_ids, sex_id=sex_ids, mark_best=best, year_id=year_ids, decomp_step=ds)