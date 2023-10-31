# Purpose: Apply a custom function to transform DisMod data into ST-GPR input data; output ST-GPR crosswalk version for each worm
# Notes: Save DisMod Bundle Versions, Create STGPR Bundle Data

### ========================= BOILERPLATE ========================= ###
library(data.table)
library(openxlsx)
source("FILEPATH/save_bundle_version.R")
source("FILEPATH/get_bundle_version.R")
source("FILEPATH/save_crosswalk_version.R")

decomp_step <- ADDRESS
gbd_round_id <- ADDRESS

###------- LOAD FUNCTIONS AND PACKAGES -------###

transform_dismod_to_stgpr <- function(in_dismod_data, worm) {
  
  stgpr_data <- copy(in_dismod_data)
  if (!(worm %in% c('ascariasis', 'trichuriasis', 'hookworm'))){ stop('only for worm in ascariasis, trichuriasis, hookworm')}
  
  if (worm == "ascariasis"){
    my_start  <- 0
    my_end    <- 16
  } else if (worm == "hookworm"){
    my_start  <- 5
    my_end    <- 20
  } else if (worm == "trichuriasis"){
    my_start  <- 5
    my_end    <- 20
  }
  
  stgpr_data <- stgpr_data[, ages := paste0(age_start, "_", age_end)]
  stgpr_data <- stgpr_data[is_outlier == 0]
  stgpr_data <- stgpr_data[, c("sex", "underlying_nid", "ihme_loc_id", "location_id", "sex", "age_start", "age_end", "ages", "year_start", "nid", "mean", "cases", "sample_size")]
  stgpr_data <- stgpr_data[age_start >= my_start & age_end <= my_end]
  
  ## Prep for ST-GPR
  stgpr_data[, age_group_id := 22]
  stgpr_data[, year_id := year_start]
  stgpr_data[, sex_id  := 3]
  
  stgpr_data[, val := mean]
  stgpr_data[, variance := (val * (1 - val) / sample_size)]
  stgpr_data <- stgpr_data[variance != 0]
  stgpr_data[, me_name := ifelse(worm == 'ascariasis', 'Ascariasis', ifelse(worm == 'trichuriasis', 'Trichuriasis', ifelse(worm == 'hookworm', 'Hookworm', stop('only for worm in ascariasis, trichuriasis, hookworm'))))]
  stgpr_data <- stgpr_data[val != 0]
  out_stgpr_data <- stgpr_data[, c("sex", "underlying_nid", "ihme_loc_id", "location_id", "nid", "year_id", "sex_id", "age_group_id", "me_name", "val", "variance", "sample_size")]
  out_stgpr_data[, seq := NA]
  out_stgpr_data[, is_outlier := 0]
  out_stgpr_data[, measure := 'proportion']
  if (worm == "ascariasis") stgpr_data <- stgpr_data[location_id != 97]
  
  return(out_stgpr_data)
}

###------- ascariasis -------###
# save dismod bundle version, write stgpr bundle data, save dismod crosswalk version
# *upload stgpr bundle data*
# save stgpr crosswalk version

# dismod bundle version
ascariasis_dismod_bundle_version_metadata <- save_bundle_version(bundle_id = ADDRESS1, decomp_step = decomp_step, gbd_round_id = gbd_round_id) 
ascariasis_dismod_bundle_version <- get_bundle_version(bundle_version_id = ADDRESS1, fetch = 'all')

# stgpr bundle data
ascariasis_stgpr_bundle_data <- transform_dismod_to_stgpr(in_dismod_data = ascariasis_dismod_bundle_version, worm = 'ascariasis')
fwrite(ascariasis_stgpr_bundle_data, 'FILEPATH')

# dismod crosswalk version
ascariasis_dismod_crosswalk_version <- copy(ascariasis_dismod_bundle_version)
ascariasis_dismod_crosswalk_version <- ascariasis_dismod_crosswalk_version[sex == 'Both']
ascariasis_dismod_crosswalk_version[, crosswalk_parent_seq := seq]
ascariasis_dismod_crosswalk_version[, seq := NA]
write.xlsx(ascariasis_dismod_crosswalk_version, 'FILEPATH', sheetName = 'extraction')
save_crosswalk_version(bundle_version_id = ADDRESS, data_filepath = 'FILEPATH', description = ADDRESS)
# check crosswalk version ids in dismod


###------- trichuriasis -------###
#save dismod bundle version, write stgpr bundle data, save dismod crosswalk version
# *upload stgpr bundle data*
# save stgpr crosswalk version

# dismod bundle version
trichuriasis_dismod_bundle_version_metadata <- save_bundle_version(bundle_id = ADDRESS2, decomp_step = decomp_step, gbd_round_id = gbd_round_id) 
trichuriasis_dismod_bundle_version <- get_bundle_version(bundle_version_id = ADDRESS2, fetch = 'all')

# stgpr bundle data
trichuriasis_stgpr_bundle_data <- transform_dismod_to_stgpr(in_dismod_data = trichuriasis_dismod_bundle_version, worm = 'trichuriasis')
fwrite(trichuriasis_stgpr_bundle_data, 'FILEPATH')

# dismod crosswalk version
trichuriasis_dismod_crosswalk_version <- copy(trichuriasis_dismod_bundle_version)
trichuriasis_dismod_crosswalk_version <- trichuriasis_dismod_crosswalk_version[sex == 'Both']
trichuriasis_dismod_crosswalk_version[, crosswalk_parent_seq := seq]
trichuriasis_dismod_crosswalk_version[, seq := NA]
write.xlsx(trichuriasis_dismod_crosswalk_version, 'FILEPATH', sheetName = 'extraction')
save_crosswalk_version(bundle_version_id = ADDRESS, data_filepath = 'FILEPATH', description = ADDRESS)
# check crosswalk version ids in dismod

###------- hookworm -------###
# save dismod bundle version, write stgpr bundle data, save dismod crosswalk version
# *upload stgpr bundle data*
# save stgpr crosswalk version

# dismod bundle version
hookworm_dismod_bundle_version_metadata <- save_bundle_version(bundle_id = ADDRESS3, decomp_step = decomp_step, gbd_round_id = gbd_round_id) 
hookworm_dismod_bundle_version <- get_bundle_version(bundle_version_id =ADDRESS3, fetch = 'all')

# stgpr bundle data
hookworm_stgpr_bundle_data <- transform_dismod_to_stgpr(in_dismod_data = hookworm_dismod_bundle_version, worm = 'hookworm')
fwrite(hookworm_stgpr_bundle_data, 'FILEPATH')

# dismod crosswalk version
hookworm_dismod_crosswalk_version <- copy(hookworm_dismod_bundle_version)
hookworm_dismod_crosswalk_version <- hookworm_dismod_crosswalk_version[sex == 'Both']
hookworm_dismod_crosswalk_version[, crosswalk_parent_seq := seq]
hookworm_dismod_crosswalk_version[, seq := NA]
write.xlsx(hookworm_dismod_crosswalk_version, 'FILEPATH', sheetName = 'extraction')
save_crosswalk_version(bundle_version_id = ADDRESS, data_filepath = 'FILEPATH', description = ADDRESS)
# check crosswalk version ids in dismod

###########################
# * post upload stgpr data*
###########################

# save stgpr bundle version and crosswalk version

###------- ascariasis -------###
ascariasis_stgpr_bundle_version_metadata <- save_bundle_version(bundle_id = ADDRESS1, decomp_step = decomp_step, gbd_round_id = gbd_round_id)
ascariasis_stgpr_bundle_version <- get_bundle_version(bundle_version_id = ADDRESS, fetch = 'all')
ascariasis_stgpr_crosswalk_version <- copy(ascariasis_stgpr_bundle_version)
ascariasis_stgpr_crosswalk_version[, crosswalk_parent_seq := seq]
ascariasis_stgpr_crosswalk_version[, seq := NA]
ascariasis_stgpr_crosswalk_version <- ascariasis_stgpr_crosswalk_version[sex == 'Both']
ascariasis_stgpr_crosswalk_version[, unit_value_as_published := as.integer(unit_value_as_published)]
ascariasis_stgpr_crosswalk_version[, unit_value_as_published := 1]
write.xlsx(ascariasis_stgpr_crosswalk_version, 'FILEPATH', sheetName = 'extraction')
save_crosswalk_version(bundle_version_id = ADDRESS, data_filepath = 'FILEPATH', description = ADDRESS)

###------- trichuriasis -------###
trichuriasis_stgpr_bundle_version_metadata <- save_bundle_version(bundle_id = ADDRESS2, decomp_step = decomp_step, gbd_round_id = gbd_round_id)
trichuriasis_stgpr_bundle_version <- get_bundle_version(bundle_version_id = ADDRESS, fetch = 'all')
trichuriasis_stgpr_crosswalk_version <- copy(trichuriasis_stgpr_bundle_version)
trichuriasis_stgpr_crosswalk_version[, crosswalk_parent_seq := seq]
trichuriasis_stgpr_crosswalk_version[, seq := NA]
trichuriasis_stgpr_crosswalk_version <- trichuriasis_stgpr_crosswalk_version[sex == 'Both']
trichuriasis_stgpr_crosswalk_version[, unit_value_as_published := as.integer(unit_value_as_published)]
trichuriasis_stgpr_crosswalk_version[, unit_value_as_published := 1]
write.xlsx(trichuriasis_stgpr_crosswalk_version, 'FILEPATH', sheetName = 'extraction')
save_crosswalk_version(bundle_version_id = ADDRESS, data_filepath = 'FILEPATH', description = ADDRESS)

###------- hookworm -------###
hookworm_stgpr_bundle_version_metadata <- save_bundle_version(bundle_id = ADDRESS3, decomp_step = decomp_step, gbd_round_id = gbd_round_id)
hookworm_stgpr_bundle_version <- get_bundle_version(bundle_version_id = ADDRESS, fetch = 'all')
hookworm_stgpr_crosswalk_version <- copy(hookworm_stgpr_bundle_version)
hookworm_stgpr_crosswalk_version[, crosswalk_parent_seq := seq]
hookworm_stgpr_crosswalk_version[, seq := NA]
hookworm_stgpr_crosswalk_version <- hookworm_stgpr_crosswalk_version[sex == 'Both']
hookworm_stgpr_crosswalk_version[, unit_value_as_published := as.integer(unit_value_as_published)]
hookworm_stgpr_crosswalk_version[, unit_value_as_published := 1]
write.xlsx(hookworm_stgpr_crosswalk_version, 'FILEPATH', sheetName = 'extraction')
save_crosswalk_version(bundle_version_id = ADDRESS, data_filepath = 'FILEPATH', description = ADDRESS)
