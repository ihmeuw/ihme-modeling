################################################################################
# Description: Download all inputs needed for processing so that the databases
#              aren't repeatedly queried
# - age groups
# - gbd population estimates
# - compiled census data
# - historical subnational and national location mappings
# - dismod global age pattern of underenumeration
# - pes data
# - sdi estimates
# - full lifetables
# - Load and make adjustments to location and census specific settings
################################################################################

library(data.table)
library(readr)
library(assertable)
library(mortdb, lib.loc = "FILEPATH/r-pkg")

rm(list = ls())
USER <- Sys.getenv("USER")
code_dir <- paste0("FILEPATH", "/population/data_processing/census_processing/")
Sys.umask(mode = "0002")


# Get arguments -----------------------------------------------------------

# load step specific settings into Global Environment
parser <- argparse::ArgumentParser()
parser$add_argument("--pop_processing_vid", type = "character",
                    help = "The version number for this run of population data processing, used to read in settings file")
parser$add_argument("--test", type = "character",
                    help = "Whether this is a test run of the process")
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

location_hierarchy <- fread(paste0(output_dir, "/inputs/location_hierarchy.csv"))


# Download inputs ---------------------------------------------------------

## pull age groups
age_groups <- mortdb::get_age_map(type = "all", drop_deleted_age_groups = T)
age_groups <- age_groups[!grepl("standardized", age_group_name)]
readr::write_csv(age_groups, path = paste0(output_dir, "/inputs/age_groups.csv"))


## pull current round best population estimates
gbd_pop <- mortdb::get_mort_outputs(model_name = "population single year",model_type = "estimate",
                                    sex_id = 1:2, run_id = pop_single_current_round_run_id,
                                    location_ids = location_hierarchy[is_estimate == 1, location_id])
gbd_pop[, c("upload_population_single_year_estimate_id", "ihme_loc_id") := NULL]
setnames(gbd_pop, "population", "mean")
setkeyv(gbd_pop, c("run_id", "location_id", "year_id", "sex_id", "age_group_id"))
readr::write_csv(gbd_pop, path = paste0(output_dir, "/inputs/gbd_pop_current_round_best.csv"))


## pull previous round best population estimates
gbd_pop <- mortdb::get_mort_outputs(model_name = "population single year",model_type = "estimate",
                                    sex_id = 1:2, run_id = pop_single_previous_round_run_id,
                                    location_ids = location_hierarchy[is_estimate == 1, location_id])
gbd_pop[, c("upload_population_single_year_estimate_id", "ihme_loc_id") := NULL]
setnames(gbd_pop, "population", "mean")
setkeyv(gbd_pop, c("run_id", "location_id", "year_id", "sex_id", "age_group_id"))
readr::write_csv(gbd_pop, path = paste0(output_dir, "/inputs/gbd_pop_previous_round_best.csv"))


# non-GBD comparison population estimates
comparators <- mortdb::get_mort_outputs(model_name = "population comparison", model_type = "data",
                                        location_ids = location_hierarchy[is_estimate == 1, location_id],
                                        run_id = comparator_data_run_id)
# drop both sexes combined age-specific estimates until fixed in upload code or switch to using agg_results in plot code
comparators <- comparators[!(grepl("WPP", source_name) & sex_id == 3 & age_group_id != 22)]
comparators[, c("upload_population_comparison_data_id", "ihme_loc_id", "comparison_source_type_id") := NULL]
readr::write_csv(comparators, path = paste0(output_dir, "/inputs/comparators.csv"))


## pull compiled census data
record_type_ids <- mortdb::get_mort_ids(type = "record_type")
method_ids <- mortdb::get_mort_ids(type = "method")
pes_adjustment_type_ids <- mortdb::get_mort_ids(type = "pes_adjustment_type")
outlier_type_ids <- mortdb::get_mort_ids(type = "outlier_type")

census_data <- mortdb::get_mort_outputs(model_name = "census raw", model_type = "data",
                                        run_id = census_data_vid)

# merge on ids
census_data <- merge(census_data, record_type_ids, by = "record_type_id", all.x = T)
census_data <- merge(census_data, method_ids, by = "method_id", all.x = T)
census_data <- merge(census_data, pes_adjustment_type_ids, by = "pes_adjustment_type_id", all.x = T)
census_data <- merge(census_data, outlier_type_ids, by = "outlier_type_id", all.x = T)

census_data <- census_data[, list(location_id, year_id, nid, underlying_nid, source_name, record_type, method_short,
                                  pes_adjustment_type, outlier_type, data_stage = "raw", sex_id, age_group_id, mean)]
census_data <- census_data[between(year_id, year_start, year_end)]
setkeyv(census_data, c(census_id_vars, "sex_id", "age_group_id"))
readr::write_csv(census_data, path = paste0(output_dir, "/outputs/01_raw.csv"))


## save subnational mappings
subnational_locations_to_split <- c("BRA", "CHN", "IND", "IRN", "KEN", "PAK", "PHL", "RUS", "UKR")
subnational_mappings <- lapply(subnational_locations_to_split, function(loc) {
  map <- fread(paste0(subnational_mappings_dir, loc, ".csv"))
  map[, national_ihme_loc_id := loc]
  return(map)
})
subnational_mappings <- rbindlist(subnational_mappings)
readr::write_csv(subnational_mappings, path = paste0(output_dir, "/inputs/subnational_mappings.csv"))


## save national mappings
national_mapping <- fread(paste0(subnational_mappings_dir, "national.csv"))
readr::write_csv(national_mapping, path = paste0(output_dir, "/inputs/national_mapping.csv"))


## pull in dismod global age pattern of underenumeration
# TODO: update the gbd round id once we have run DISMOD for this year
source(paste0(shared_functions_dir, "get_model_results.R"))
dismod_fit <- get_model_results("epi", model_version_id = best_pes_dismod_version_id, location_id = 1, gbd_round_id = mortdb::get_gbd_round(2017))
dismod_fit <- dismod_fit[age_group_id != 27] # remove age-standardized age group
readr::write_csv(dismod_fit, path = paste0(output_dir, "/inputs/pes_dismod_age_pattern.csv"))


## pull compiled pes data
pes_data <- mortdb::get_mort_outputs(model_name = "pes", model_type = "data", run_id = pes_data_run_id)
# drop outliers
pes_data <- pes_data[!(location_id == get_location_id("ZAF", location_hierarchy) & year_id == 2011)]
pes_data <- pes_data[!(location_id == get_location_id("GHA", location_hierarchy) & year_id == 1960)]
readr::write_csv(pes_data, path = paste0(output_dir, "/inputs/pes_data.csv"))


## pull sdi estimates
source(paste0(shared_functions_dir, "get_covariate_estimates.R"))
sdi_data <- get_covariate_estimates(covariate_id = 881, gbd_round_id = mortdb::get_gbd_round(gbd_year), model_version_id = sdi_mv_id, decomp_step = decomp_step)
readr::write_csv(sdi_data, path = paste0(output_dir, "/inputs/sdi.csv"))


## pull full lifetables
lt_parameter_ids <- get_mort_ids("life_table_parameter")[, list(life_table_parameter_id, life_table_parameter_name = parameter_name)]
full_lt_age_groups <- mortdb::get_age_map(type = "single_year")[, list(age_group_id, age = age_group_years_start)]
lt_files <- list.files(paste0("FILEPATH", no_shock_death_number_run_id, "/full_lt/with_shock/"), full.names = T)
if (length(lt_files) == 0) stop(paste0("No full life tables exist for no shock death number run id ", no_shock_death_number_run_id))
full_lifetables <- lapply(location_hierarchy[is_estimate == 1, location_id], function(loc_id) {
  print(loc_id)
  lt_file <- lt_files[grepl(paste0("summary_full_", loc_id, ".csv"), lt_files) | grepl(paste0("summary_full_", loc_id, "_aggregated.csv"), lt_files)]
  lt <- fread(lt_file)

  # this is needed because the aggregated files are formatted differently
  if (grepl("aggregated", lt_file)) {
    lt <- merge(lt, lt_parameter_ids, by = "life_table_parameter_id", all.x = T)
    lt <- merge(lt, full_lt_age_groups, by = "age_group_id", all.x = T)
  }
  lt <- lt[life_table_parameter_name %in% c("lx", "nLx", "Tx"), list(location_id, year_id, sex_id, age, life_table_parameter_name, mean)]

  return(lt)
})
full_lifetables <- rbindlist(full_lifetables)
full_lifetables <- data.table::dcast(full_lifetables, formula = location_id + year_id + sex_id + age ~ life_table_parameter_name, value.var = "mean")
ids <- list(location_id = location_hierarchy[is_estimate == 1, location_id], year_id = year_start:year_end,
            sex_id = 1:2, age = 0:110)
assertable::assert_ids(full_lifetables, ids)
setkeyv(full_lifetables, c("location_id", "year_id", "sex_id", "age"))
setcolorder(full_lifetables, c("location_id", "year_id", "sex_id", "age", "lx", "nLx", "Tx"))
readr::write_csv(full_lifetables, path = paste0(output_dir, "/inputs/full_lt.csv"))


# Load and make adjustments to census/location settings -------------------

## Census specific settings
census_specific_settings <- data.table(readr::read_csv(census_specific_settings_dir))
census_specific_settings <- merge(census_specific_settings, location_hierarchy[, list(ihme_loc_id, location_id)], by = "ihme_loc_id", all.x = T)

# specify defaults for these settings if they are not pre-specified

census_specific_settings <- census_specific_settings[, list(location_id, ihme_loc_id, year_id, pes_adjust,
                                                            census_use_age_heaping_smoother = as.logical(use_age_heaping_smoother),
                                                            drop_age_groups, collapse_to_age, aggregate_ages)]
readr::write_csv(census_specific_settings, path = paste0(output_dir, "/inputs/census_specific_settings.csv"))


## Location specific settings
location_specific_settings <- data.table(readr::read_csv(location_specific_settings_dir))
location_specific_settings <- merge(location_specific_settings, location_hierarchy[, list(ihme_loc_id, location_id)], by = "ihme_loc_id", all.x = T)

# don't use age heaping smoothers in "High-income" and "Central Europe, Eastern Europe, and Central Asia" super regions unless pre-specified to use it
location_specific_settings <- merge(location_specific_settings, location_hierarchy[, list(ihme_loc_id, super_region_name)], all.x = T, by = "ihme_loc_id")
location_specific_settings[is.na(use_age_heaping_smoother) & super_region_name %in% c("High-income", "Central Europe, Eastern Europe, and Central Asia"),
                           use_age_heaping_smoother := F]
# don't use age heaping smoothers in China and all its subnational unitsunless pre-specified to use it
location_specific_settings[is.na(use_age_heaping_smoother) & grepl("CHN", ihme_loc_id), use_age_heaping_smoother := F]

# specify defaults for these settings if they are not pre-specified
location_specific_settings[is.na(use_backprojected_baseline), use_backprojected_baseline := T]
location_specific_settings[is.na(scale_backprojected_baseline), scale_backprojected_baseline := F]
location_specific_settings[is.na(fill_missing_triangle_above), fill_missing_triangle_above := terminal_age]

location_specific_settings <- location_specific_settings[, list(location_id, ihme_loc_id, use_backprojected_baseline, scale_backprojected_baseline,
                                                                fill_missing_triangle_above, location_use_age_heaping_smoother = use_age_heaping_smoother)]
write_csv(location_specific_settings, path = paste0(output_dir, "/inputs/location_specific_settings.csv"))
