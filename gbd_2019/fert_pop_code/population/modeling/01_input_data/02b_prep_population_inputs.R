################################################################################
# Description: Prep inputs for fitting the population model.
# - load and format census data for given "drop above age"
# - optionally knockout census data (knockouts only)
# - format for TMB
# - save census data
################################################################################

library(data.table)
library(readr)
library(assertable)
library(mortdb, lib = "FILEPATH/r-pkg")
library(mortcore, lib.loc = "FILEPATH/r-pkg")

rm(list = ls())
USER <- Sys.getenv("USER")
code_dir <- paste0("FILEPATH", "/population/modeling/popReconstruct/")
Sys.umask(mode = "0002")


# Get arguments -----------------------------------------------------------

# load step specific settings into Global Environment
parser <- argparse::ArgumentParser()
parser$add_argument("--pop_vid", type = "character",
                    help = "The version number for this run of population, used to read in settings file")
parser$add_argument("--test", type = "character",
                    help = "Whether this is a test run of the process")
args <- parser$parse_args()
if (interactive()) { # set interactive defaults, should never commit changes away from the test version to be safe
  args$pop_vid <- "99999"
  args$test <- "T"
}
args$test <- as.logical(args$test)
list2env(args, .GlobalEnv); rm(args)

# load model run specific settings into Global Environment
settings_dir <- paste0("FILEPATH", pop_vid, "/run_settings.rdata")
load(settings_dir)
list2env(settings, envir = environment())

source(paste0(code_dir, "functions/formatting.R"))
source(paste0(code_dir, "functions/ccmpp.R"))

# need to set the seed for knockouts
version <- 0
set.seed(version)

# read in population modeling location hierarchy
location_hierarchy <- fread(paste0(output_dir, "/database/all_location_hierarchies.csv"))
location_hierarchy <- location_hierarchy[location_set_name == "population"]

# read in gbd age group ids
age_groups <- fread(paste0(output_dir, "/database/age_groups.csv"))

# read in census specific settings
census_specific_settings <- fread(paste0(output_dir, "/database/census_specific_settings.csv"))
census_specific_settings <- merge(census_specific_settings, location_hierarchy[, list(ihme_loc_id, location_id)], by = "ihme_loc_id", all.x = T)

# read in location specific settings and set defaults if they are not specified
location_specific_settings <- fread(paste0(output_dir, "/database/location_specific_settings.csv"))
location_specific_settings <- merge(location_specific_settings, location_hierarchy[, list(ihme_loc_id, location_id)], by = "ihme_loc_id", all.x = T)
dont_use_backprojected_baseline_locations <- location_specific_settings[use_backprojected_baseline == "F" & location_id %in% location_hierarchy[is_estimate == 1, location_id], location_id]

super_regions_to_drop_under5_counts <- c("South Asia", "Sub-Saharan Africa",
                                         "North Africa and Middle East",
                                         "Latin America and Caribbean",
                                         "Southeast Asia, East Asia, and Oceania")

# Id variables needed
age_groups_needed <- age_groups[if(age_int == 1) single_year_model else five_year_model,
                                list(age_group_id, age_group_years_start, age_group_years_end)]
setkeyv(age_groups_needed, c("age_group_years_start"))
baseline_id_vars <-  list(location_id = location_hierarchy[is_estimate == 1, location_id],
                          year_id = year_start,
                          sex_id = 1:2,
                          age_group_years_start = age_groups_needed$age_group_years_start)
census_id_vars <- list(location_id = location_hierarchy[is_estimate == 1, location_id],
                       sex_id = 1:2)


# Load population data ----------------------------------------------------

## LOAD
census_data <- fread(paste0(output_dir, "/database/census_data.csv"))
census_data <- census_data[location_id %in% location_hierarchy[is_estimate == 1, location_id] & between(year_id, year_start, year_end)]

# replace with gbd baseline population
if (length(dont_use_backprojected_baseline_locations) > 0) {
  gbd_baseline <- fread(paste0(output_dir, "/database/gbd_population_current_round_best.csv"))
  gbd_baseline <- gbd_baseline[location_id %in% location_hierarchy[is_estimate == 1, location_id]]
  gbd_baseline <- gbd_baseline[location_id %in% dont_use_backprojected_baseline_locations & year_id == year_start,
                               list(location_id, year_id, sex_id, age_group_id, mean = population)]

  # aggregate into age groups needed for the baseline
  gbd_baseline <- mortcore::agg_results(gbd_baseline, id_vars = c("location_id", "year_id", "sex_id", "age_group_id"), value_vars = "mean",
                                        agg_hierarchy = F, age_aggs = age_groups_needed[, age_group_id])
  gbd_baseline <- gbd_baseline[age_group_id %in% age_groups_needed[, age_group_id]]

  # mark as duplicated the baseline census in these locations
  census_data[location_id %in% dont_use_backprojected_baseline_locations & year_id == year_start & outlier_type == "not outliered", outlier_type := "duplicated"]

  # add on the gbd baseline
  gbd_baseline[, source_name := "GBD"]
  gbd_baseline[, outlier_type := "not outliered"]
  census_data <- rbind(census_data, gbd_baseline, fill = T, use.names = T)
}

# aggregate censuses to age interval if at a more granular level (modeling with five year age groups then need to collapse single year age groups)
census_data <- merge(census_data, age_groups[, list(age_group_id, age_group_years_start, age_group_years_end)], all.x = T, by = "age_group_id")
census_data[, aggregate_ages := paste0("seq(0,", terminal_age, ",", age_int, ")")]
census_data <- agg_age_data(census_data, id_vars = census_data_id_vars, age_grouping_var = "aggregate_ages")

# mark certain age groups to be dropped from the model
census_data <- merge(census_data, age_groups[, list(age_group_id, age_group_years_start, age_group_years_end)], all.x = T, by = "age_group_id")
census_data[, n := age_group_years_end - age_group_years_start]
census_data[age_group_years_end == age_group_table_terminal_age, n := 0]

census_data[, drop := F]

# automatically drop under 5 population in these super regions to deal with
# age misreporting and undercounts
census_data <- merge(census_data, location_hierarchy[, list(location_id, super_region_name)], by = "location_id", all.x = T)
census_data[, age_middle_floor := floor(age_group_years_start + (n / 2))]
census_data[age_group_years_end == age_group_table_terminal_age, age_middle_floor := (terminal_age - age_group_years_start) / 2]
census_data[super_region_name %in% super_regions_to_drop_under5_counts & age_middle_floor <= 5, drop := T]
census_data[, c("age_middle_floor", "super_region_name") := NULL]

# drop certain age groups for modeling purposes
drop_age_groups_settings <- census_specific_settings[!is.na(drop_age_groups), list(location_id, year_id, drop_age_groups)]
drop_age_groups_settings <- drop_age_groups_settings[, list(drop_age_groups = list(eval(parse(text = drop_age_groups)))), by = c("location_id", "year_id")]
census_data <- merge(census_data, census_specific_settings[!is.na(drop_age_groups), list(location_id, year_id, drop_age_groups)],
                     by = c("location_id", "year_id"), all = T)
census_data[, drop := ifelse(age_group_years_start %in% eval(parse(text = drop_age_groups)), T, drop), by = "drop_age_groups"]
census_data[, drop_age_groups := NULL]

## FORMAT FOR TMB
setcolorder(census_data, c(census_data_id_vars, "sex_id", "age_group_years_start", "age_group_years_end", "n", "age_group_id", "drop", "mean"))
setkeyv(census_data, c(census_data_id_vars, "sex_id", "age_group_years_start", "age_group_years_end", "n", "age_group_id", "drop"))

## CHECK
# check baseline populations
assertable::assert_ids(census_data[year_id == year_start & outlier_type == "not outliered"], id_vars = baseline_id_vars)
# check non-baseline populations
assertable::assert_ids(census_data[year_id != year_start & outlier_type == "not outliered"], id_vars = census_id_vars, assert_dups = F)
# check all populations
assertable::assert_values(census_data[outlier_type == "not outliered"], colnames="value_mean", test="gte", test_val=0)


# Save population inputs --------------------------------------------------

readr::write_csv(census_data, path = paste0(output_dir, "/inputs/population.csv"))
