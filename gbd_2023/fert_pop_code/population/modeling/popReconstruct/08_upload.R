################################################################################
# Description: Upload all outputs of the population modeling run.
# - read in population, single-year age group population, and net migrant
#   files to upload
# - save single-year age group net migration file since takes too long to upload
# - assertion checks
# - make uploads
# - optionally mark as best
################################################################################

library(data.table)
library(readr)
library(assertable)
library(parallel)
library(mortdb, lib = "FILEPATH")

rm(list = ls())
MKL_NUM_THREADS <- Sys.getenv("MKL_NUM_THREADS")
USER <- Sys.getenv("USER")
code_dir <- paste0("FILEPATH")
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
  MKL_NUM_THREADS <- "5"
}
args$test <- as.logical(args$test)
list2env(args, .GlobalEnv); rm(args)

# load model run specific settings into Global Environment
settings_dir <- paste0("FILEPATH", ifelse(test, "test/", ""), pop_vid, "/run_settings.rdata")
load(settings_dir)
list2env(settings, envir = environment())

age_groups <- fread(paste0(output_dir, "/database/age_groups.csv"))
location_hierarchy <- fread(paste0(output_dir, "/database/all_location_hierarchies.csv"))
location_hierarchy_modeled <- location_hierarchy[location_set_name == "population"]


# Read in files to upload -------------------------------------------------

# standard gbd and reporting age groups
pop_reporting <- parallel::mclapply(location_hierarchy[, unique(location_id)], function(loc_id) {
  print(loc_id)
  dir <- paste0(output_dir, loc_id, "/outputs/summary/population_reporting_raked_ui.csv")
  summary <- fread(dir)
  return(summary)
}, mc.cores = MKL_NUM_THREADS)
pop_reporting <- rbindlist(pop_reporting)
setnames(pop_reporting, "mean", "population")
setkeyv(pop_reporting, c("location_id", "year_id", "sex_id", "age_group_id"))

# single year age group populations
pop <- parallel::mclapply(location_hierarchy[, unique(location_id)], function(loc_id) {
  print(loc_id)
  dir <- paste0(output_dir, loc_id, "/outputs/summary/population_raked_ui.csv")
  summary <- fread(dir)
  return(summary)
}, mc.cores = MKL_NUM_THREADS)
pop <- rbindlist(pop)
setnames(pop, "mean", "population")

# we don't upload population at birth or neonatal age groups to this table currently
pop <- pop[age_group_id %in% age_groups[(single_year_model), age_group_id]]
pop <- rbind(pop_reporting[age_group_id == 28], pop, use.names = T)
setkeyv(pop, c("location_id", "year_id", "sex_id", "age_group_id"))

# net migration proportion and count
drop_above_age <- data.table::fread(paste0(output_dir, "/versions_best.csv"))
net_migration <- parallel::mclapply(location_hierarchy_modeled[is_estimate == 1, unique(location_id)], function(loc_id) {
  print(loc_id)
  ihme_loc <- location_hierarchy_modeled[location_id == loc_id, unique(ihme_loc_id)]
  drop_age <- drop_above_age[ihme_loc_id == ihme_loc, drop_age]
  dir <- paste0(output_dir, loc_id, "/outputs/model_fit/net_migration_posterior_drop", drop_age, ".csv")
  summary <- fread(dir)
  return(summary)
}, mc.cores = MKL_NUM_THREADS)
net_migration <- rbindlist(net_migration)
net_migration <- net_migration[year_id != year_end]

# save the single year age groups for HIV use
readr::write_csv(net_migration[age_group_id %in% age_groups[(single_year_model), age_group_id]], paste0(output_dir, "/upload/net_migration_single_year.csv"))
# but only upload the reporting age groups
net_migration <- net_migration[age_group_id %in% age_groups[(reporting_migration), age_group_id]]
setkeyv(net_migration, c("location_id", "year_id", "sex_id", "age_group_id", "measure_id"))
net_migration[, estimate_stage_id := 6] # final


# Check prior to upload ---------------------------------------------------

pop_id_vars <- list(location_id = unique(location_hierarchy$location_id), year_id = years,
                    sex_id = 1:3, age_group_id = age_groups[(single_year_model), age_group_id])
assertable::assert_ids(pop, pop_id_vars)
assertable::assert_values(pop, colnames = "mean", test = "gt", test_val = 0)

pop_reporting_id_vars <- list(location_id = unique(location_hierarchy$location_id), year_id = years,
                              sex_id = 1:3, age_group_id = age_groups[(reporting), age_group_id])
assertable::assert_ids(pop_reporting, pop_reporting_id_vars)
assertable::assert_values(pop_reporting, colnames = "mean", test = "gt", test_val = 0)

net_migration_id_vars <- list(location_id = location_hierarchy_modeled[is_estimate == 1, unique(location_id)], year_id = years[years != max(years)],
                              sex_id = 1:3, age_group_id = age_groups[(reporting_migration), age_group_id], measure_id = c(77, 55), estimate_stage_id = 6)
assertable::assert_ids(net_migration, net_migration_id_vars)
assertable::assert_values(net_migration, colnames = "mean", test = "not_na")


# Make uploads ------------------------------------------------------------

pop_upload_dir <- paste0(output_dir, "/upload/pop.csv")
readr::write_csv(pop, pop_upload_dir)
if (!test) mortdb::upload_results(filepath = pop_upload_dir, model_name = "population single year", model_type = "estimate", run_id = pop_single_vid, hostname = hostname)

pop_reporting_upload_dir <- paste0(output_dir, "/upload/pop_reporting.csv")
readr::write_csv(pop_reporting, pop_reporting_upload_dir)
if (!test) mortdb::upload_results(filepath = pop_reporting_upload_dir, model_name = "population", model_type = "estimate", run_id = pop_reporting_vid, hostname = hostname)

net_migration_upload_dir <- paste0(output_dir, "/upload/net_migration.csv")
readr::write_csv(net_migration, net_migration_upload_dir)
if (!test) mortdb::upload_results(filepath = net_migration_upload_dir, model_name = "migration", model_type = "estimate", run_id = migration_vid, hostname = hostname)


# Mark as best ------------------------------------------------------------

if (best & !test) {
  mortdb::update_status(model_name = "population single year", model_type = "estimate", run_id = pop_single_vid, new_status = "best", hostname = hostname)
  mortdb::update_status(model_name = "population", model_type = "estimate", run_id = pop_reporting_vid, new_status = "best", assert_parents = F, hostname = hostname)
  mortdb::update_status(model_name = "migration", model_type = "estimate", run_id = migration_vid, new_status = "best", hostname = hostname)
}


# Output comparison of drop ages across versions --------------------------

drop_above_age <- fread(paste0(output_dir, "/versions_best.csv"))
setnames(drop_above_age, "drop_age", paste0("GBD", gbd_year, "_", pop_reporting_vid))

current_round_last_drop_above_age <- fread(paste0("FILEPATH", pop_current_round_run_id, "/versions_best.csv"))
setnames(current_round_last_drop_above_age, "drop_age", paste0("GBD", gbd_year, "_", pop_current_round_run_id))

previous_round_last_drop_above_age <- fread(paste0("FILEPATH", pop_previous_round_run_id, "/versions_best.csv"))
setnames(previous_round_last_drop_above_age, "drop_age", paste0("GBD", gbd_year_previous, "_", pop_previous_round_run_id))

drop_ages <- merge(previous_round_last_drop_above_age, current_round_last_drop_above_age, all = T)
drop_ages <- merge(drop_ages, drop_above_age, all = T)

drop_ages[, different_previous_round := get(paste0("GBD", gbd_year_previous, "_", pop_previous_round_run_id)) != get(paste0("GBD", gbd_year, "_", pop_reporting_vid))]
drop_ages[, different_current_round := get(paste0("GBD", gbd_year, "_", pop_current_round_run_id)) != get(paste0("GBD", gbd_year, "_", pop_reporting_vid))]

readr::write_csv(drop_ages, paste0(output_dir, "/diagnostics/drop_ages_changes.csv"))


# Output additional diagnostics for baby migrants -------------------------

baby_diagnostics <- net_migration[age_group_id == 28 & measure_id == 77 & sex_id == 3]
baby_diagnostics <- merge(baby_diagnostics, location_hierarchy_modeled[, list(location_id, ihme_loc_id, location_name)], by = "location_id", all.x = T)
baby_diagnostics <- baby_diagnostics[order(-abs(mean))]
baby_diagnostics[, mean := round(mean, 5)]
readr::write_csv(baby_diagnostics, paste0(output_dir, "/diagnostics/baby_migration.csv"))
