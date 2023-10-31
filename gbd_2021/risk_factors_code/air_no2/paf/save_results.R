# clear memory
rm(list=ls())

# runtime configuration
if (Sys.info()["sysname"] == "Linux") {
  
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
  central_lib <- "FILEPATH"
} else {
  j_root <- "FILEPATH"
  h_root <- "FILEPATH"
  central_lib <- "FILEPATH"
}

# load packages, install if missing
pacman::p_load(data.table, magrittr, ini, openxlsx, pbapply)

# version for upload
paf.version <- 18

#parameters
years <- c(1990:2020)
location_set_version_id <- 35
decomp <- "iterative"
ages <- c(2:3,6:8, 388, 389, 238, 34)
sexes <- c(1,2)
desc <- paste0("paf version ", paf.version, ": all years")
date <- "DATE"

cores.provided <- 25

#draw directories
paf.dir <- file.path("FILEPATH", paf.version)
save.dir <- file.path("FILEPATH",paf.version)
dir.create(save.dir,recursive = T)

#----------------------------Functions---------------------------------------------------

source(file.path(central_lib,"FILEPATH/save_results_epi.R"))
source(file.path(central_lib,"FILEPATH/save_results_risk.R"))
source(file.path(central_lib,"FILEPATH/get_population.R"))
source(file.path(central_lib,"FILEPATH/get_location_metadata.R"))
source(file.path(central_lib,"FILEPATH/get_bundle_data.R"))
source(file.path(central_lib,"FILEPATH/upload_bundle_data.R"))
source(file.path(central_lib,"FILEPATH/save_bundle_version.R"))
source(file.path(central_lib,"FILEPATH/save_crosswalk_version.R"))

locs <- get_location_metadata(35)
locs <- locs[most_detailed==1]

#custom fx
"%ni%" <- Negate("%in%") # create a reverse %in% operator

#--------------------Save results----------------------------------------------------------
# save CSVs for draws for most-detailed locations, sexes, & age-groups
sex_id <- c(1:2)
age_group_ids <- c(2:3,6:8, 388, 389, 238, 34)
draw_cols <- paste0("draw_",0:999)

# save PAFs
save_results_risk(input_dir = save.dir,
                  input_file_pattern = "{location_id}_{measure_id}.csv",
                  year_id = years,
                  modelable_entity_id = 26282,
                  description = desc,
                  decomp_step = decomp,
                  risk_type = "paf",
                  measure_id = "3",
                  gbd_round_id = 7,
                  mark_best=TRUE)
