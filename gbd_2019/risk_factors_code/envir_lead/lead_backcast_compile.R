#----HEADER-------------------------------------------------------------------------------------------------------------
## Project: RF: envir_lead_bone and envir_lead_blood
## Purpose: Backcast lead exposure
#***********************************************************************************************************************

#----CONFIG-------------------------------------------------------------------------------------------------------------
rm(list=ls())

library(data.table)
library(magrittr)
library(parallel)

# version history (see lead_backcast_master.R)
output.version <- 1 # gbd17 first versions; initial 2019 run stored here too (oops)
output.version <- 2 # gbd17
output.version <- 3 # 2019 decomp step 1
output.version <- 4 # 2019 decomp step 2
output.version <- 5 # 2019 decomp step 3 (edit 1/3/2020: no changes made in step 4, so this is the final output.version for GBD 2019)

run_id <- 83492 # st-gpr run id; make sure to update this as needed
exp_type <- c("blood", "bone")

for (type in exp_type) {
  input_dir <- file.path("FILEPATH")
  output_dir <- file.path("FILEPATH")
  if(!dir.exists(output_dir)) dir.create(output_dir, recursive = TRUE)
  
  # compile
  source("FUNCTION")
  locations <- get_location_metadata(gbd_round_id = 6, location_set_id = 22) # CHECK GBD ROUND ID
  locs <- locations[level > 2,location_id]
  
  for (loc in locs) {
    files <- list.files(input_dir,pattern = paste0("^",loc,"_"))
    df <- rbindlist(mclapply(file.path(input_dir,files),fread,mc.cores=10), use.names = TRUE)
    df <- dcast(df, measure_id + location_id + year_id + sex_id + age_group_id ~ variable)
    write.csv(df,file.path(output_dir,paste0(loc,".csv")),row.names = F)
  }
}

## END
