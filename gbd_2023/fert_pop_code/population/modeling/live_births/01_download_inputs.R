################################################################################
## Description: Download all inputs needed for calculating live births so that
##              the databases aren't repeatedly queried. This includes modeling
##              inputs and plotting inputs.
################################################################################

library(data.table)
library(readr)
library(assertable)
library(mortdb, lib.loc = "FILEPATH")

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

location_hierarchy <- fread(paste0(output_dir, "/inputs/all_reporting_hierarchies.csv"))
location_hierarchy <- location_hierarchy[location_set_name == "live_births"]


# Age groupings -----------------------------------------------------------

# get all possible age groups
age_groups_all <- mortdb::get_age_map(type = "all", gbd_year = gbd_year, drop_deleted_age_groups = T)
age_groups_all <- age_groups_all[!grepl("standardized", age_group_name)]
age_groups_all[age_group_id == 164, age_group_years_start := -1] # adjust age_group start and end for population at birth

age_groups_fertility <- mortdb::get_age_map(type = "fertility", gbd_year = gbd_year)
age_groups_fertility_single <- mortdb::get_age_map(type = "fertility_single_year", gbd_year = gbd_year)

# mark the type of age group for all that we need
age_groups_all[, fertility_reporting := age_group_id %in% c(age_groups_fertility$age_group_id, extra_reporting_age_groups)]
age_groups_all[, fertility_single_year := age_group_id %in% age_groups_fertility_single$age_group_id]

readr::write_csv(age_groups_all, path = paste0(output_dir, "/inputs/age_groups.csv"))


# Sex ratio at birth ------------------------------------------------------

srb <- mortdb::get_mort_outputs(model_name = "birth sex ratio", model_type = "estimate", run_id = srb_vid,
                                location_ids = location_hierarchy[is_estimate == 1, location_id], gbd_year = gbd_year)
assertable::assert_ids(srb, id_vars = list(location_id = location_hierarchy[is_estimate == 1, location_id], year_id = years))
readr::write_csv(srb, path = paste0(output_dir, "/inputs/srb.csv"))


# Population --------------------------------------------------------------

population <- mortdb::get_mort_outputs(model_name = "population single year", model_type = "estimate",
                                       sex_id = 2, location_ids = location_hierarchy[is_estimate == 1, location_id],
                                       age_group_ids = age_groups_all[(fertility_single_year), age_group_id],
                                       run_id = pop_vid, gbd_year = gbd_year)
readr::write_csv(population, path = paste0(output_dir, "/inputs/population.csv"))
