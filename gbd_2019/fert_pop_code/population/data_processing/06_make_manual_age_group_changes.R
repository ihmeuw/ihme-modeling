################################################################################
# Description: Make extra age grouping changes to deal with poor quality data or
# age misreporting not dealth with by the correction for age heaping.
# - in specified locations collapse to lower terminal age groups
# - in specified locations collapse to aggregate age groups (often 5 year age groups)
# - data_stage: 'processed, manual age group changes'
################################################################################

library(data.table)
library(readr)
library(mortdb, lib.loc = "FILEPATH/r-pkg")
library(mortcore, lib.loc = "FILEPATH/r-pkg")

rm(list = ls())
USER <- Sys.getenv("USER")
code_dir <- paste0("FILEPATH", "/population/data_processing/census_processing/")
Sys.umask(mode = "0002")


# Get arguments -----------------------------------------------------------

# load step specific settings into Global Environment
parser <- argparse::ArgumentParser()
parser$add_argument("--pop_processing_vid", type = "character",
                    help = 'The version number for this run of population data processing, used to read in settings file')
parser$add_argument("--test", type = "character",
                    help = 'Whether this is a test run of the process')
args <- parser$parse_args()
if (interactive()) { # set interactive defaults, should never commit changes away from the test version to be safe
  args$pop_processing_vid <- "99999"
  args$test <- "T"
}
args$test <- as.logical(args$test)
list2env(args, .GlobalEnv); rm(args)

# load model run specific settings into Global Environment
settings_dir <- paste0("FILEPATH", pop_processing_vid, "/run_settings.rdata")
load(settings_dir)
list2env(settings, envir = environment())

source(paste0(code_dir, "helper_functions.R"))

age_groups <- fread(paste0(output_dir, "/inputs/age_groups.csv"))
census_specific_settings <- fread(paste0(output_dir, "/inputs/census_specific_settings.csv"))
location_hierarchy <- fread(paste0(output_dir, "/inputs/location_hierarchy.csv"))
location_collapse_five_year_age_groups <- location_hierarchy[level >= 3 & !super_region_name %in% c("High-income", "Central Europe, Eastern Europe, and Central Asia"), location_id]

census_id_vars <- c(census_id_vars, "split", "aggregate", "smoother")


# Collapse to a lower terminal age group ----------------------------------

census_data <- fread(paste0(output_dir, "/outputs/05_correct_age_heaping.csv"))

# separate out needed censuses
census_data <- merge(census_data,
                     census_specific_settings[!is.na(collapse_to_age), list(location_id, year_id, collapse_to_age)],
                     all.x = T, by = c("location_id", "year_id"))
census_data_collapsed_terminal_age <- census_data[!is.na(collapse_to_age)]
census_data <- census_data[is.na(collapse_to_age)]
census_data[, "collapse_to_age" := NULL]

# collapse to needed age groups
census_data_collapsed_terminal_age <- merge(census_data_collapsed_terminal_age, age_groups[, list(age_group_id, age_group_years_start, age_group_years_end)], by = "age_group_id", all.x = T)
census_data_collapsed_terminal_age <- agg_age_data(census_data_collapsed_terminal_age, id_vars = census_id_vars, age_grouping_var = "collapse_to_age")
census_data <- rbind(census_data, census_data_collapsed_terminal_age, use.names = T)


# Aggregate to specified age groups ---------------------------------------

# separate out needed censuses
census_data <- merge(census_data, census_specific_settings[!is.na(aggregate_ages), list(location_id, year_id, aggregate_ages)],
                     all.x = T, by = c("location_id", "year_id"))
census_data[location_id %in% location_collapse_five_year_age_groups & is.na(aggregate_ages), aggregate_ages := "seq(0, terminal_age, 5)"]
census_data_aggregated <- census_data[!is.na(aggregate_ages)]
census_data <- census_data[is.na(aggregate_ages)]
census_data[, "aggregate_ages" := NULL]

census_data_aggregated <- merge(census_data_aggregated, age_groups[, list(age_group_id, age_group_years_start, age_group_years_end)], by = "age_group_id", all.x = T)
census_data_aggregated <- agg_age_data(census_data_aggregated, id_vars = census_id_vars, age_grouping_var = "aggregate_ages")
census_data <- rbind(census_data, census_data_aggregated, use.names = T)


# Save outputs ------------------------------------------------------------

census_data[, data_stage := "manual age group changes"]
setcolorder(census_data, c(census_id_vars, "sex_id", "age_group_id", "mean"))
setkeyv(census_data, c(census_id_vars, "sex_id", "age_group_id"))
readr::write_csv(census_data, path = paste0(output_dir, "/outputs/06_make_manual_age_group_changes.csv"))
