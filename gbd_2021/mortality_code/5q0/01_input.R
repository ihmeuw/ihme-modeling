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
parser$add_argument('--code_dir', type="character", required=TRUE,
                    help='Directory where child-mortality code is cloned')
args <- parser$parse_args()
list2env(args, .GlobalEnv)

# Set core directories
output_dir <- paste0("FILEPATH", version_id)

# Load the GBD specific libraries
source("FILEPATH/get_covariate_estimates.R")
source("FILEPATH/get_location_metadata.R")
source(paste0(code_dir, "/gbd5q0/R/data_container.R"))
library(mortdb, lib = "FILEPATH")

gbd_round_id <- mortdb::get_gbd_round(gbd_year=gbd_year)

# Get data
dc = DataContainer$new(gbd_round_id = gbd_round_id, start_year = start_year,
                       end_year = end_year, version_id = version_id, output_dir = output_dir)
location_data <- dc$get('location')
input_data <- get_mort_outputs(model_name="5q0", model_type="data",
                               run_id=data_5q0_version, location_set_id = 82,
                               outlier_run_id = "active")

# Get spacetime locations
st_locs <- mortdb::get_spacetime_loc_hierarchy(prk_own_region=T, old_ap=T, gbd_year = gbd_year)
fwrite(st_locs, paste0("FILEPATH"))

# Linear model targets
targets <- get_locations(gbd_type = "ap_old", level = "estimate", gbd_year = gbd_year)
gbd_standard <- get_locations(gbd_type = "standard_modeling", level = "all")
mortality_locations <- get_locations(gbd_type = "ap_old", level = "estimate")
national_parents <- mortality_locations[level == 4, parent_id]
standard_with_metadata <- mortality_locations[location_id %in% unique(c(gbd_standard$location_id, national_parents, 44533))]

targets[location_id %in% standard_with_metadata$location_id, primary := T]
targets[is.na(primary), primary := F]
targets[, secondary := T]

fwrite(targets, paste0("FILEPATH"))

# China MCHS births for 05_calculate_variance
file.copy("FILEPATH",
              paste0("FILEPATH"))

# Save data
dc$save(input_data, 'input_5q0')
