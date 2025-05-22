################################################################################
## Description: Upload final live birth estimates for all reporting age groups.
##              Single-year age group estimates are saved but not uploaded.
################################################################################

library(data.table)
library(readr)
library(mortcore)
library(mortdb)

rm(list = ls())
USER <- Sys.getenv("USER")
Sys.umask(mode = "0002")


# Get arguments -----------------------------------------------------------

# load step specific settings into Global Environment
parser <- argparse::ArgumentParser()
parser$add_argument("--live_births_vid", type = "character",
                    help = 'The version number for this run of live births, used to read in settings file')
parser$add_argument("--test", type = "character",
                    help = 'Whether this is a test run of the process')
parser$add_argument("--code_dir", type = "character", 
                    help = 'Location of code')
args <- parser$parse_args()
if (interactive()) { # set interactive defaults, should never commit changes away from the test version to be safe
  args$live_births_vid <- "99999"
  args$test <- "T"
  args$code_dir <- "CODE_DIR_HERE"
}
args$test <- as.logical(args$test)
list2env(args, .GlobalEnv); rm(args)

# load model run specific settings into Global Environment
settings_dir <- paste0("FILEPATH", ifelse(test, "test/", ""), live_births_vid, "/run_settings.csv")
load(settings_dir)
list2env(settings, envir = environment())

age_groups <- fread(paste0(output_dir, "/inputs/age_groups.csv"))
location_hierarchy <- fread(paste0(output_dir, "/inputs/all_reporting_hierarchies.csv"))


# Single-year age group birth estimates -----------------------------------

births_single_year <- lapply(unique(location_hierarchy[, location_id]), function(loc_id) {
  location_estimates <- fread(paste0(output_dir, "outputs/raked_aggregated_by_loc/", loc_id, "_single_year_summary.csv"))
  return(location_estimates)
})
births_single_year <- rbindlist(births_single_year)

# check outputs
ids <- list(location_id = unique(location_hierarchy[, location_id]), year_id = years, sex_id = 1:3,
            age_group_id = age_groups[(fertility_single_year), age_group_id])
assertable::assert_ids(births_single_year, ids)
assertable::assert_values(births_single_year, colnames = "mean", test = "gte", test_val = 0)
assertable::assert_values(births_single_year, colnames = "lower", test = "gte", test_val = 0)
assertable::assert_values(births_single_year, colnames = "upper", test = "gte", test_val = 0)

# format and save draws
setcolorder(births_single_year, c("location_id", "year_id", "sex_id", "age_group_id", "mean", "lower", "upper"))
setkeyv(births_single_year, c("location_id", "year_id", "sex_id", "age_group_id"))
readr::write_csv(births_single_year, path = paste0(output_dir, "upload/births_single_year.csv"))


# Reporting age group birth estimates -------------------------------------

births_reporting <- lapply(unique(location_hierarchy[, location_id]), function(loc_id) {
  location_estimates <- fread(paste0(output_dir, "outputs/raked_aggregated_by_loc/", loc_id, "_reporting_summary.csv"))
  return(location_estimates)
})
births_reporting <- rbindlist(births_reporting)

# check outputs
ids <- list(location_id = unique(location_hierarchy[, location_id]), year_id = years, sex_id = 1:3,
            age_group_id = age_groups[(fertility_reporting), age_group_id])
assertable::assert_ids(births_reporting, ids)
assertable::assert_values(births_reporting, colnames = "mean", test = "gte", test_val = 0)
assertable::assert_values(births_reporting, colnames = "lower", test = "gte", test_val = 0)
assertable::assert_values(births_reporting, colnames = "upper", test = "gte", test_val = 0)

# format and save draws
setnames(births_reporting, "mean", "births")
setcolorder(births_reporting, c("location_id", "year_id", "sex_id", "age_group_id", "births", "lower", "upper"))
setkeyv(births_reporting, c("location_id", "year_id", "sex_id", "age_group_id"))
readr::write_csv(births_reporting, path = paste0(output_dir, "upload/births_reporting.csv"))

# upload estimates
if (!test) {
  mortdb::upload_results(filepath = paste0(output_dir, "upload/births_reporting.csv"),
                         model_name = "birth", model_type = "estimate",
                         run_id = live_births_vid, hostname = hostname)
  if (best) {
    mortdb::update_status(model_name = "birth", model_type = "estimate",
                          run_id = live_births_vid, new_status = "best",
                          hostname = hostname)
  }
}
