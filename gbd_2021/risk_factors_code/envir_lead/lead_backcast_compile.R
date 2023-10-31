#----HEADER-------------------------------------------------------------------------------------------------------------
## Project: RF: envir_lead_bone and envir_lead_blood
## Purpose: Backcast lead exposure
#***********************************************************************************************************************

#----CONFIG-------------------------------------------------------------------------------------------------------------
rm(list=ls())

library(data.table)
library(magrittr)
library(parallel)

# version history (see lead_backcast_launch.R)
output_version <- 14 # GBD 2020 final

run_id <- 168590 # best st-gpr run id; make sure to update this as needed
exp_type <- c("blood","bone")

source("FILEPATH/get_location_metadata.R")
locations <- get_location_metadata(gbd_round_id = 7, location_set_id = 22) # CHECK GBD ROUND ID
locs <- locations[is_estimate == 1, location_id]

for (type in exp_type) {
  input_dir <- file.path("FILEPATH", paste0(type,"_backcast"), output_version, run_id, "output_by_draw")
  output_dir <- file.path("FILEPATH", paste0(type,"_backcast"), output_version, run_id, "output_by_loc")
  if(!dir.exists(output_dir)) dir.create(output_dir, recursive = TRUE)
  
  # compile
  for (loc in locs) {
    print(loc)
    
    files <- list.files(input_dir,pattern = paste0("^",loc,"_"))
    df <- rbindlist(mclapply(file.path(input_dir,files),fread,mc.cores=5), use.names = TRUE)
    df <- dcast(df, measure_id + location_id + year_id + sex_id + age_group_id ~ variable)
    write.csv(df,file.path(output_dir,paste0(loc,".csv")),row.names = F)
  }
}

## END
