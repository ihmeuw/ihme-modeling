################################################################################
## Date Created: 13 June 2012
## Description: Submit shells scripts to run all code necessary to produce
##              estimates of age and sex specific mortality risk under age 5
################################################################################

############
## Settings
############
rm(list=ls())
library(foreign)
library(devtools)
library(methods)
library(argparse)


# Get arguments
parser <- ArgumentParser()
parser$add_argument('--version_id', type="integer", required=TRUE,
                    help='The version_id for this run of age-sex')
args <- parser$parse_args()

# Get arguments
version_id <- args$version_id

workdir <- "FILEPATH"

# Set the version ID
output_dir <- "FILEPATH"
dir.create(output_dir, showWarnings = FALSE)

# Import get_locations function
codes <- get_locations(level="estimate", gbd_year=2017)

# Get the locations with HIV metadata
hiv_location_information <- get_locations(level="estimate", gbd_year=2017, hiv_metadata = T)
write.csv(hiv_location_information, "FILEPATH", row.names=F)

# Import check_loc_results function
source("FILEPATH")

# Generate age-sex combinations
sex_input <- c("male", "female")
age_input <- c("enn", "lnn", "pnn", "inf", "ch")

# Save location hierarchies
locs_for_stata <- get_locations(level="estimate", gbd_year=2017)
write.csv(locs_for_stata, "FILEPATH", row.names=F)

subnats <- get_locations(level="subnational", gbd_year=2017)
write.csv(subnats, "FILEPATH", row.names=F)

cplus_locs <- get_locations(gbd_year=2017)
write.csv(cplus_locs, "FILEPATH", row.names=F)
