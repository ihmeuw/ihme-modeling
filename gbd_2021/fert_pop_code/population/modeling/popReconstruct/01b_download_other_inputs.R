################################################################################
## Description: Download all inputs needed for splitting population to the under
##              1 age groups, creating uncertainty and making plots so that the
##              databases aren't repeatedly queried.
################################################################################

library(data.table)
library(readr)
library(parallel)
library(assertable)
library(mortdb, lib = "FILEPATH")
library(mortcore, lib = "FILEPATH")

rm(list = ls())
MKL_NUM_THREADS <- Sys.getenv("MKL_NUM_THREADS")
USER <- Sys.getenv("USER")
code_dir <-"FILEPATH"
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
settings_dir <- paste0("FILEPATH", ifelse(test, "test/", ""), pop_vid, "/run_settings.rdata")
load(settings_dir)
list2env(settings, envir = environment())

location_hierarchy <- fread(paste0(output_dir, "/database/all_location_hierarchies.csv"))
location_hierarchy <- location_hierarchy[location_set_name == "population"]


# Under1 population proportions -------------------------------------------

under1_pop_proportions_dir <- "FILEPATH"
under1_pop <- parallel::mclapply(location_hierarchy[is_estimate == 1, location_id], function(loc_id) {
  print(loc_id)
  pop <- fread(paste0(under1_pop_proportions_dir, loc_id, ".csv"))
  return(pop)
}, mc.cores = MKL_NUM_THREADS)
under1_pop <- rbindlist(under1_pop)

under1_age_groups <- data.table(age_group_id = c(2, 3, 388, 389), age_group = c("enn", "lnn", "pna", "pnb"))
under1_pop <- under1_pop[age_group %in% c("enn", "lnn", "pna", "pnb")]
under1_pop <- merge(under1_pop, under1_age_groups, by = c("age_group"), all.x = T)

under1_pop <- under1_pop[, list(location_id, year_id, sex_id, age_group_id, mean)]
assertable::assert_ids(under1_pop, id_vars = list(location_id = location_hierarchy[is_estimate == 1, location_id],
                                                  year_id = years, sex_id = 1:2, age_group_id = c(2, 3, 388, 389)))
setkeyv(under1_pop, c("location_id", "year_id", "sex_id", "age_group_id"))
readr::write_csv(under1_pop, path = paste0(output_dir, "/database/under1_pop.csv"))
rm(under1_pop); gc()


# Comparator populations --------------------------------------------------

# previous GBD round's best population estimates
previous_round_best_run <- mortdb::get_mort_outputs(model_name = "population single year", model_type = "estimate",
                                                    sex_id = 1:2, location_ids = if (length(test_locations) > 0) location_hierarchy[is_estimate == 1, location_id] else location_hierarchy[, location_id],
                                                    run_id = pop_single_previous_round_run_id)
readr::write_csv(previous_round_best_run, path = paste0(output_dir, "/database/gbd_population_previous_round_best.csv"))
rm(previous_round_best_run); gc()

# non-GBD comparison population estimates
comparators <- mortdb::get_mort_outputs(model_name = "population comparison", model_type = "data",
                                        location_ids = location_hierarchy[, location_id],
                                        run_id = pop_comparator_run_id)
comparators[, detailed_source := gsub("WPP_", "WPP", detailed_source)]
readr::write_csv(comparators, path = paste0(output_dir, "/database/comparators.csv"))
rm(comparators); gc()

# Previous census data sources --------------------------------------------

pop_sy_prev_round_parents <- mortdb::get_proc_lineage(model_name = "population single year",
                                                      model_type = "estimate",
                                                      run_id = pop_single_previous_round_run_id)
prev_census_data_run_id <- pop_sy_prev_round_parents[parent_process_name == "census processed data",
                                                     parent_run_id]
prev_census_data <- mortdb::get_mort_outputs(model_name = "census processed",
                                             model_type = "data",
                                             sex_id = 1:2,
                                             location_ids = if (length(test_locations) > 0) location_hierarchy[is_estimate == 1, location_id] else location_hierarchy[, location_id],
                                             run_id = prev_census_data_run_id)

# For our purposes we just need unique nid and underlying nid
prev_round_census_sources <- unique(prev_census_data[, .(nid, underlying_nid)])
readr::write_csv(prev_round_census_sources, file = paste0(output_dir, "/database/prev_round_sources.csv"))

rm(pop_sy_prev_round_parents, prev_census_data_run_id, prev_census_data,
   prev_round_census_sources); gc()

# Previous migration estimates --------------------------------------------

# the first run for a new gbd round does not have a best version yet
if (!is.na(migration_current_round_run_id)) {
  current_round_best_run <- mortdb::get_mort_outputs(model_name = "migration", model_type = "estimate",
                                                     sex_id = 1:2, location_ids = if (length(test_locations) > 0) location_hierarchy[is_estimate == 1, location_id] else location_hierarchy[, location_id],
                                                     run_id = migration_current_round_run_id)
  readr::write_csv(current_round_best_run, path = paste0(output_dir, "/database/gbd_migration_current_round_best.csv"))
  rm(current_round_best_run); gc()
}

previous_round_best_run <- mortdb::get_mort_outputs(model_name = "migration", model_type = "estimate",
                                                    sex_id = 1:2, location_ids = if (length(test_locations) > 0) location_hierarchy[is_estimate == 1, location_id] else location_hierarchy[, location_id],
                                                    run_id = migration_previous_round_run_id)
readr::write_csv(previous_round_best_run, path = paste0(output_dir, "/database/gbd_migration_previous_round_best.csv"))
rm(previous_round_best_run); gc()


# WPP net migration -------------------------------------------------------

wpp_years <- c(2017, 2019)
wpp_total_migration <- lapply(c(wpp_years), function(wpp_year) {
  data <- fread(paste0("FILEPATH"))
  data <- data[, list(ihme_loc_id, year_id, sex_id = 3, age_group_id = 22,
                      measure_id = 19, source = paste0("WPP", wpp_year), mean = value)]
  return(data)
})
wpp_total_migration <- rbindlist(wpp_total_migration)
readr::write_csv(wpp_total_migration, path = paste0(output_dir, "/database/wpp_total_net_migration.csv"))


# SDI ---------------------------------------------------------------------

# pull sdi data and save in "database"
source(paste0(shared_functions_dir, "get_covariate_estimates.R"))
sdi <- get_covariate_estimates(covariate_id = 881, gbd_round_id = mortdb::get_gbd_round(gbd_year), model_version_id = sdi_mv_id, decomp_step = "iterative",
                               location_id = location_hierarchy[is_estimate == 1, location_id])
write_csv(sdi, path = paste0(output_dir, "/database/sdi.csv"))
