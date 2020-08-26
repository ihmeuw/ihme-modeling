################################################################################
## Description: Download all inputs needed for the model fitting so that the
##              databases aren't repeatedly queried and we avoid pulling from
##              the j drive in other scripts.
################################################################################

library(data.table)
library(readr)
library(parallel)
library(assertable)
library(mortdb, lib = "FILEPATH/r-pkg")
library(mortcore, lib = "FILEPATH/r-pkg")

rm(list = ls())
MKL_NUM_THREADS <- Sys.getenv("MKL_NUM_THREADS")
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

location_hierarchy <- fread(paste0(output_dir, "/database/all_location_hierarchies.csv"))
location_hierarchy <- location_hierarchy[location_set_name == "population"]


# Age groupings -----------------------------------------------------------

# get all possible age groups
age_groups_all <- mortdb::get_age_map(type = "all", drop_deleted_age_groups = T)
age_groups_all <- age_groups_all[!grepl("standardized", age_group_name)]
age_groups_all[age_group_id == 164, age_group_years_start := -1] # adjust age_group start and end for births

age_groups_gbd <- mortdb::get_age_map(type = "gbd")
age_groups_reporting <- mortdb::get_age_map(type = "population")
age_groups_single <- mortdb::get_age_map(type = "single_year", single_year_terminal_age_start = terminal_age)
age_groups_fertility_single <- mortdb::get_age_map(type = "fertility_single_year")


# need to subset from gbd standard age groups to population five year age groups
age_groups_five_model <- rbind(age_groups_all[age_group_id == 1], age_groups_gbd[age_group_years_start >= 5])

# mark the type of age group for all that we need
age_groups_all[, single_year_model := age_group_id %in% age_groups_single$age_group_id]
age_groups_all[, five_year_model := age_group_id %in% age_groups_five_model$age_group_id]
age_groups_all[, most_detailed := age_group_id %in% c(2:3, 388, 389) | age_group_id %in% age_groups_single[age_group_years_start >= 1, age_group_id]]
age_groups_all[, reporting := age_group_id %in% age_groups_reporting$age_group_id]
age_groups_all[, reporting_migration := age_group_id %in% reporting_age_groups_migration | age_group_id %in% age_groups_gbd[age_group_years_start >= 1, age_group_id]]
age_groups_all[, plot_migration := age_group_id %in% c(28, 5) | (five_year_model & age_group_years_start >= 5)]
age_groups_all[, fertility_single_year := age_group_id %in% age_groups_fertility_single$age_group_id]

readr::write_csv(age_groups_all, path = paste0(output_dir, "/database/age_groups.csv"))


# Census data -------------------------------------------------------------

census_data <- mortdb::get_mort_outputs(model_name = "census processed", model_type = "data", run_id = census_processed_data_vid,
                                        location_ids = location_hierarchy[is_estimate == 1, location_id])

# merge on ids
record_type_ids <- mortdb::get_mort_ids(type = "record_type")
method_ids <- mortdb::get_mort_ids(type = "method")
pes_adjustment_type_ids <- mortdb::get_mort_ids(type = "pes_adjustment_type")
data_stage_ids <- mortdb::get_mort_ids(type = "data_stage")
outlier_type_ids <- mortdb::get_mort_ids(type = "outlier_type")
census_data <- merge(census_data, record_type_ids, by = "record_type_id", all.x = T)
census_data <- merge(census_data, method_ids, by = "method_id", all.x = T)
census_data <- merge(census_data, pes_adjustment_type_ids, by = "pes_adjustment_type_id", all.x = T)
census_data <- merge(census_data, data_stage_ids, by = "data_stage_id", all.x = T)
census_data <- merge(census_data, outlier_type_ids, by = "outlier_type_id", all.x = T)

# subset to the last processing step with the baseline included
census_data <- census_data[data_stage == "baseline included" & sex_id %in% c(1, 2),
                           list(location_id, year_id, nid, underlying_nid, source_name, outlier_type,
                                record_type, method_short, pes_adjustment_type, data_stage,
                                sex_id, age_group_id, mean)]
census_id_vars <- c("location_id", "year_id", "nid", "underlying_nid", "source_name",
                    "outlier_type", "record_type", "method_short", "pes_adjustment_type", "data_stage")
setkeyv(census_data, c(census_data_id_vars, "sex_id", "age_group_id"))
readr::write_csv(census_data, path = paste0(output_dir, "/database/census_data.csv"))
rm(census_data); gc()


# Migration data ----------------------------------------------------------

# pull data from database
migration_data <- mortdb::get_mort_outputs(model_name = "migration flow", model_type = "data", run_id = migration_data_vid,
                                           location_ids = location_hierarchy[is_estimate == 1, location_id])
readr::write_csv(migration_data, path = paste0(output_dir, "/database/migration_data.csv"))
rm(migration_data); gc()


# Migration age patterns --------------------------------------------------

# QAT migration age pattern
QAT_best_drop_age <- fread("FILEPATH/versions_best.csv")[ihme_loc_id == "QAT", drop_age]
file.copy(from = paste0("FILEPATH/migration_proportion_drop", QAT_best_drop_age, ".csv"),
          to = paste0(output_dir, "/database/QAT_migration.csv"), overwrite = T)

# EUROSTAT migration age pattern
file.copy(from = paste0("FILEPATH/3_selected_migration.csv"),
          to = paste0(output_dir, "/database/EUROSTAT_migration.csv"), overwrite = T)


# Sex ratio at birth ------------------------------------------------------

srb <- mortdb::get_mort_outputs(model_name = "birth sex ratio", model_type = "estimate", run_id = srb_vid,
                                location_ids = location_hierarchy[is_estimate == 1, location_id])
assertable::assert_ids(srb, id_vars = list(location_id = location_hierarchy[is_estimate == 1, location_id],
                                           year_id = years))
readr::write_csv(srb, path = paste0(output_dir, "/database/srb.csv"))
rm(srb); gc()


# ASFR --------------------------------------------------------------------

asfr_dir <- paste0("FILEPATH + asfr_vid")
age_int_name <- "one"
fert_age_int <- 1
asfr <- parallel::mclapply(location_hierarchy[is_estimate == 1, ihme_loc_id], function(ihme_loc) {
  print(ihme_loc)
  loc_id <- location_hierarchy[ihme_loc_id == ihme_loc, location_id]
  data <- fread(paste(asfr_dir, paste0(age_int_name, "_by_", age_int_name),
                      paste0(ihme_loc, "_fert_", fert_age_int, "by", fert_age_int, ".csv"), sep = "/"))
  data <- data[, list(location_id = loc_id, year_id, age_group_years_start = age, mean = value_mean)]
  return(data)
}, mc.cores = MKL_NUM_THREADS)
asfr <- rbindlist(asfr)
asfr <- mortcore::age_start_to_age_group_id(asfr, id_vars = c("location_id", "year_id"), keep_age_group_id_only = F)
assertable::assert_ids(asfr, id_vars = list(location_id = location_hierarchy[is_estimate == 1, location_id],
                                            year_id = years,
                                            age_group_years_start = 0:terminal_age))
readr::write_csv(asfr, path = paste0(output_dir, "/database/asfr.csv"))
rm(asfr); gc()


# Full lifetables ---------------------------------------------------------

## pull full lifetables
lt_parameter_ids <- mortdb::get_mort_ids("life_table_parameter")[, list(life_table_parameter_id, life_table_parameter_name = parameter_name)]
full_lt_age_groups <- mortdb::get_age_map(type = "single_year")[, list(age_group_id, age = age_group_years_start)]
lt_files <- list.files(paste0("FILEPATH", no_shock_death_number_vid, "/full_lt/with_shock/"), full.names = T)
full_lifetables <- parallel::mclapply(location_hierarchy[is_estimate == 1, location_id], function(loc_id) {
  print(loc_id)
  lt_file <- lt_files[grepl(paste0("summary_full_", loc_id, ".csv"), lt_files) | grepl(paste0("summary_full_", loc_id, "_aggregated.csv"), lt_files)]
  lt <- fread(lt_file)

  if (grepl("aggregated", lt_file)) {
    lt <- merge(lt, lt_parameter_ids, by = "life_table_parameter_id", all.x = T)
    lt <- merge(lt, full_lt_age_groups, by = "age_group_id", all.x = T)
  }
  lt <- lt[life_table_parameter_name %in% c("ax", "qx"), list(location_id, year_id, sex_id, age, life_table_parameter_name, mean)]

  return(lt)
}, mc.cores = MKL_NUM_THREADS)
full_lifetables <- rbindlist(full_lifetables)
full_lifetables <- data.table::dcast(full_lifetables, formula = location_id + year_id + sex_id + age ~ life_table_parameter_name, value.var = "mean")
assertable::assert_ids(full_lifetables, id_vars = list(location_id = location_hierarchy[is_estimate == 1, location_id],
                                                       year_id = years, sex_id = 1:2, age = 0:110))
setcolorder(full_lifetables, c("location_id", "year_id", "sex_id", "age", "ax", "qx"))
setkeyv(full_lifetables, c("location_id", "year_id", "sex_id", "age"))
readr::write_csv(full_lifetables, path = paste0(output_dir, "/database/full_lt.csv"))
rm(full_lifetables); gc()


# Previous best population estimates --------------------------------------

# current GBD round's best population estimates
current_round_best_run <- mortdb::get_mort_outputs(model_name = "population single year", model_type = "estimate",
                                                   sex_id = 1:2, location_ids = if (length(test_locations) > 0) location_hierarchy[is_estimate == 1, location_id] else location_hierarchy[, location_id],
                                                   run_id = pop_single_current_round_run_id)
readr::write_csv(current_round_best_run, path = paste0(output_dir, "/database/gbd_population_current_round_best.csv"))
