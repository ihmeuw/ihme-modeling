################################################################################
## Description: Run the first stage of the prediction model for adult mortality
##              and calculate inputs to GPR
################################################################################

rm(list=ls())

# Import installed libraries
library(devtools)
library(methods)
library(argparse)


# Get arguments
parser <- ArgumentParser()
parser$add_argument('--version_id', type="integer", required=TRUE,
                    help='The version_id for this run of 5q0')
parser$add_argument('--gbd_round_id', type="integer", required=TRUE,
                    help='The gbd_round_id for this run of 5q0')
parser$add_argument('--start_year', type="integer", required=TRUE,
                    help='The starting year for this run of 5q0')
parser$add_argument('--end_year', type="integer", required=TRUE,
                    help='The ending year for this run of 5q0')
args <- parser$parse_args()

# Get arguments
version_id <- args$version_id
gbd_round_id <- args$gbd_round_id
start_year <- args$start_year
end_year <- args$end_year


# Set core directories
code_dir <- "FILEPATH"
output_dir <- "FILEPATH"

# Get data
dc = DataContainer$new(gbd_round_id = gbd_round_id, start_year = start_year,
                       end_year = end_year, output_dir = output_dir)
location_data <- dc$get('location')
input_data <- get_mort_outputs(model_name="5q0", model_type="data",
                               run_id="best", location_set_id = 82,
                               outlier_run_id = "active")

# Save data
dc$save(input_data, 'input_5q0')
