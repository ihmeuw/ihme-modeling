################################################################################
## Description: Run the first stage of the prediction model for adult mortality
##              and calculate inputs to GPR
################################################################################

rm(list=ls())

# Import installed libraries
library(devtools)
library(methods)
library(argparse)
library(readr)
library(data.table)


# Get arguments
parser <- ArgumentParser()
parser$add_argument('--version_id', type="integer", required=TRUE,
                    help='The version_id for this run of 5q0')
parser$add_argument('--gbd_year', type="integer", required=TRUE,
                    help='The gbd_year for this run of 5q0')
parser$add_argument('--start_year', type="integer", required=TRUE,
                    help='The starting year for this run of 5q0')
parser$add_argument('--end_year', type="integer", required=TRUE,
                    help='The ending year for this run of 5q0')
parser$add_argument('--data_5q0_version', type="integer", required=TRUE,
                    help='5q0 data version id')
args <- parser$parse_args()

# Get arguments
version_id <- args$version_id
gbd_year <- args$gbd_year
start_year <- args$start_year
end_year <- args$end_year
data_5q0_version <- args$data_5q0_version


# Set core directories
root <- "FILEPATH"
username <- Sys.getenv("USER")
code_dir <- "FILEPATH"
output_dir <- "FILEPATH"

# Load the GBD specific libraries
source("FILEPATH")
source("FILEPATH")
source(paste0("FILEPATH", "/data_container.R"))
library(mortdb, lib = "FILEPATH")

gbd_round_id <- get_gbd_round(gbd_year=gbd_year)
prev_gbd_year <-get_gbd_year(gbd_round=gbd_round_id-1)

# Get data
dc = DataContainer$new(gbd_round_id = gbd_round_id, start_year = start_year,
                       end_year = end_year, version_id = version_id, output_dir = output_dir)
location_data <- dc$get('location')
input_data <- get_mort_outputs(model_name="5q0", model_type="data",
                               run_id=data_5q0_version, location_set_id = 82,
                               outlier_run_id = "active")

# Get spacetime locations
st_locs <- get_spacetime_loc_hierarchy(prk_own_region=T, old_ap=T, gbd_year = gbd_year)
fwrite(st_locs, paste0(output_dir, "/data/st_locs.csv"))


# Linear model targets
if (gbd_year == 2019){
  targets <- get_locations(gbd_type = "ap_old", level = "estimate", gbd_year = gbd_year)
  gbd_standard <- get_locations(gbd_type = "standard_modeling", level = "all",gbd_year=gbd_year)
  mortality_locations <- get_locations(gbd_type = "ap_old", level = "estimate",gbd_year=gbd_year)
  national_parents <- mortality_locations[level == 4, parent_id]
  standard_with_metadata <- mortality_locations[location_id %in% unique(c(gbd_standard$location_id, national_parents, 44533))]
  
  targets[location_id %in% standard_with_metadata$location_id, primary := T]
  targets[is.na(primary), primary := F]
  targets[, secondary := T]
} 
if (gbd_year == 2017) {
  locations <- as.data.table(get_locations(gbd_type = "ap_old", level = "estimate", gbd_year = gbd_year))
  locations_prev <- as.data.table(get_locations(level = "estimate", gbd_type = "ap_old", gbd_year = prev_gbd_year))
  locations_prev = locations_prev[!grepl("SAU_", ihme_loc_id)]
  locations_prev$primary <- TRUE
  
  locations$secondary <- TRUE
  
  targets <- as.data.table(merge(locations, locations_prev, by = c("location_id", "ihme_loc_id"), all = T))
  targets <- targets[, list(location_id, ihme_loc_id, primary, secondary)]
  targets[is.na(primary), primary := FALSE]
}

fwrite(targets, paste0(output_dir, "/data/04_fit_prediction_model_targets.csv"))

file.copy("FILEPATH",
          paste0(output_dir, "FILEPATH"))

# Save data
dc$save(input_data, 'input_5q0')