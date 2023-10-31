################################################################################
## Description: Create new population version and all output directories
##              needed. Submits `00b_submit.R`.
################################################################################

library(data.table)
library(readr)
library(mortdb, lib = "FILEPATH")
library(mortcore, lib = "FILEPATH")

rm(list = ls())
USER <- Sys.getenv("USER")
code_dir <- "FILEPATH"
Sys.umask(mode = "0002")


# Set up all the settings and versions that are needed --------------------

best <- F # whether to immediately mark the new population and migration estimates as best
gbd_year <- 2020
year_end <- gbd_year + 2
comment <- "Informative comment"

test <- T # whether to create new versions and actually upload results
test_locations <- c() # specify ihme_loc_ids if you only want to run on a subset of locations, will skip over raking if any are specified

copy_previous_version <- F # whether to skip model fitting and just copy over the 03a_fit_model results from the previous version `copy_vid`. Useful if we need to just fix a couple locations
copy_vid <- "99999" # the version to copy over from
rerun_locations <- c() # specify ihme_loc_ids that you do want to rerun for and not copy over, empty if you want to copy over all results from previous version


# Create new estimate versions --------------------------------------------

hostname <- "HOSTNAME"

if (test) {
  pop_reporting_vid <- pop_single_vid <- migration_vid <- 99999
} else {
  # make a new population version which we will base the folder structure off of
  pop_reporting_vid <- gen_new_version(model_name = "population", model_type = "estimate",
                                       comment = comment, gbd_year = gbd_year,
                                       hostname = hostname)

  # make a new single year population version which will be the parent of other things
  pop_single_vid <- gen_new_version(model_name = "population single year", model_type = "estimate",
                                    comment = comment, gbd_year = gbd_year,
                                    hostname = hostname)
  # upload migration as well
  migration_vid <- gen_new_version(model_name = "migration", model_type = "estimate",
                                   comment = comment, gbd_year = gbd_year,
                                   hostname = hostname)
}


# Make settings list ------------------------------------------------------

gbd_year_previous <- mortdb::get_gbd_year(mortdb::get_gbd_round(gbd_year) - 1)

settings <- list(
  pop_reporting_vid = pop_reporting_vid,
  pop_single_vid = pop_single_vid,
  migration_vid = migration_vid,
  comment = comment,

  test = test,
  test_locations = test_locations,
  best = best,
  hostname = hostname,

  copy_previous_version = copy_previous_version,
  copy_vid = copy_vid,
  rerun_locations = rerun_locations,

  queue = "all",
  submission_project_name = "proj_popreconstruction",
  default_resubmission_attempts = 3,
  r_shell_singularity_version = "3501",
  r_shell_submit_singularity = "4131",
  slack_channel_updates = F,

  # basic settings
  gbd_year = gbd_year,
  gbd_year_previous = gbd_year_previous,
  age_int = 1,
  terminal_age = 95,
  year_start = 1950,
  year_end = year_end,
  year_int = 1,
  draws = 1000,
  census_data_id_vars = c("location_id", "year_id", "nid", "underlying_nid", "source_name",
                          "outlier_type", "record_type", "method_short", "pes_adjustment_type", "data_stage"),
  age_group_table_terminal_age = 125,
  reporting_age_groups_migration = c(1, 21:26, 28, 157:159, 162), # TODO move this to get_age_map
  heatmap_age_groups = c(22, 1, 23, 24, 41, 234),
  modeling_hierarchy = ifelse(gbd_year == 2017, "mortality", "population"), # mortality hierarchy was used for 2017 since the population hierarchy was created late and incorrectly
  reporting_hierarchies <- list("sdi", "who", "world bank", "eu",
                                "commonwealth", "four world regions",
                                "world bank income levels", "oecd", "g20",
                                "african union", "nordic", 135, 13, 130, 131,
                                132, 136, 138),
  comparison_heatmap_rounds = c("current", "previous", "WPP"),

  # allow demographic components to vary in model fitting
  fix_baseline = FALSE,
  fix_migration = FALSE,
  fix_survival = TRUE,
  fix_fertility = TRUE,
  fix_srb = TRUE,

  # figure out best version of all inputs needed
  decomp_step = "iterative", # used to pull sdi
  asfr_vid = mortdb::get_proc_version(model_name = "asfr", model_type = "estimate", run_id = "best", gbd_year = gbd_year),
  srb_vid = mortdb::get_proc_version(model_name = "birth sex ratio", model_type = "estimate", run_id = "best", gbd_year = gbd_year),
  no_shock_death_number_vid = mortdb::get_proc_version(model_name = "no shock death number", model_type = "estimate", run_id = "best", gbd_year = gbd_year),
  u5_vid = mortdb::get_proc_version(model_name = "u5 envelope", model_type = "estimate", run_id = "best", gbd_year = gbd_year),
  census_processed_data_vid = mortdb::get_proc_version(model_name = "census processed", model_type = "data", run_id = "best", gbd_year = gbd_year),
  migration_data_vid = mortdb::get_proc_version(model_name = "migration flow", model_type = "data", run_id = "best", gbd_year = gbd_year),

  pop_current_round_run_id = mortdb::get_proc_version(model_name = "population", model_type = "estimate", run_id = "best", gbd_year = gbd_year),
  pop_previous_round_run_id = mortdb::get_proc_version(model_name = "population", model_type = "estimate", run_id = "best", gbd_year = gbd_year_previous),
  pop_single_current_round_run_id = mortdb::get_proc_version(model_name = "population single year", model_type = "estimate", run_id = "best", gbd_year = gbd_year),
  pop_single_previous_round_run_id = mortdb::get_proc_version(model_name = "population single year", model_type = "estimate", run_id = "best", gbd_year = gbd_year_previous),
  pop_comparator_run_id = mortdb::get_proc_version(model_name = "population comparison", model_type = "data", run_id = "best", gbd_year = gbd_year),

  migration_current_round_run_id = mortdb::get_proc_version(model_name = "migration", model_type = "estimate", run_id = "best", gbd_year = gbd_year),
  migration_previous_round_run_id = mortdb::get_proc_version(model_name = "migration", model_type = "estimate", run_id = "best", gbd_year = gbd_year_previous),

  # location and census specific settings files
  location_specific_settings_dir = "FILEPATH",
  census_specific_settings_dir = "FILEPATH",

  # uncertainty inputs
  cv_rmse_dir = "FILEPATH"

  # output directories
  output_dir = "FILEPATH",
  sandbox_dir = "FILEPATH",

  shared_functions_dir = "FILEPATH"
)
source(paste0(settings$shared_functions_dir, "get_covariate_estimates.R"))
settings$sdi_mv_id <- unique(get_covariate_estimates(covariate_id = 881, gbd_round_id = mortdb::get_gbd_round(gbd_year), location_id = 1, year_id = gbd_year, decomp_step = settings$decomp_step)$model_version_id)
settings$full_life_table_run_id <- mortdb::get_proc_lineage(model_name = "no shock death number", model_type = "estimate", run_id = settings$no_shock_death_number_vid, lineage_type = "child")[child_process_name == "full life table estimate", child_run_id]
settings$years <- seq(settings$year_start, settings$year_end, settings$year_int)

# load into the environment
list2env(settings, envir = environment())


# Parent-child relationships ----------------------------------------------

# generate parent to child relationships
if (!test) {
  # generate parent to child relationships for the single year population estimates
  mortdb::gen_parent_child(parent_runs = list("census processed data" = census_processed_data_vid,
                                              "migration flow data" = migration_data_vid,
                                              "population single year estimate" = pop_single_current_round_run_id,
                                              "population estimate" = pop_current_round_run_id,
                                              "no shock death number estimate" = no_shock_death_number_vid, # we use the full lifetables that are versioned in the file system by this run id
                                              "full life table estimate" = full_life_table_run_id, # we use the full life tables but they aren't always uploaded so we pull from the file system
                                              "u5 envelope estimate" = u5_vid,
                                              "asfr estimate" = asfr_vid,
                                              "birth sex ratio estimate" = srb_vid),
                           child_process = "population single year estimate", child_id = pop_single_vid,
                           hostname = hostname)
  mortdb::gen_external_input_map(external_input_versions = list("sdi" = sdi_mv_id),
                                 process_name = "population single year estimate", run_id = pop_single_vid,
                                 hostname = hostname)

  # generate parent to child relationships for reporting age groups population estimates
  mortdb::gen_parent_child(parent_runs = list("population single year estimate" = pop_single_vid),
                           child_process = "population estimate", child_id = pop_reporting_vid,
                           hostname = hostname)

  # generate parent to child relationships for the net migration estimates
  mortdb::gen_parent_child(parent_runs = list("census processed data" = census_processed_data_vid,
                                              "migration flow data" = migration_data_vid,
                                              "population single year estimate" = pop_single_current_round_run_id,
                                              "population estimate" = pop_current_round_run_id,
                                              "no shock death number estimate" = no_shock_death_number_vid, # we use the full lifetables that are versioned in the file system by this run id
                                              "full life table estimate" = full_life_table_run_id, # we use the full life tables but they aren't always uploaded so we pull from the file system
                                              "asfr estimate" = asfr_vid,
                                              "birth sex ratio estimate" = srb_vid),
                           child_process = "migration estimate", child_id = migration_vid,
                           hostname = hostname)
}


# Location hierarchies ----------------------------------------------------

loc_hierarchies <- c(modeling_hierarchy, reporting_hierarchies)
all_hierarchies <- lapply(loc_hierarchies, function(loc_type) {
  locs <- data.table(mortdb::get_locations(gbd_type = loc_type, gbd_year = gbd_year, level = "all"))
  locs[, location_set_name := loc_type]
  return(locs)
})
all_hierarchies <- data.table::rbindlist(all_hierarchies, use.names = T)

# make GBR a location that is not modeled for the population hierarchy
if (modeling_hierarchy == "mortality") {
  all_hierarchies[location_set_name == "mortality", location_set_name := "population"]
  all_hierarchies[location_set_name == "population" & ihme_loc_id == "GBR", is_estimate := 0]
}

# determine which locations are national locations that will not be raked
CHN_GBR_locids <- all_hierarchies[location_set_name == "population" & ihme_loc_id %in% c("CHN", "GBR"), location_id]
all_hierarchies[location_set_name == "population", national_location := as.numeric((level == 3 & !location_id %in% CHN_GBR_locids) | parent_id %in% CHN_GBR_locids)]

# determine which locations to copy results over from a previous version
all_hierarchies[, copy_results := as.numeric(copy_previous_version & is_estimate == 1)]
all_hierarchies[ihme_loc_id %in% rerun_locations, copy_results := 0]

# if only running a test for subset of locations can subset location hierarchy
if (test & length(test_locations) > 0) all_hierarchies <- all_hierarchies[ihme_loc_id %in% test_locations]


# Create directories ------------------------------------------------------

# delete old test version if running a new one
if (test) {
  unlink(output_dir, recursive = T)
  unlink(sandbox_dir, recursive = T)
}

# create parent directories
dir.create(paste(output_dir, "task_map", sep = "/"), recursive = T)
dir.create(paste(output_dir, "database", sep = "/"), recursive = T)
dir.create(paste(output_dir, "inputs", sep = "/"), recursive = T)
dir.create(paste(output_dir, "upload", sep = "/"), recursive = T)
dir.create(paste(output_dir, "raking_draws", sep = "/"), recursive = T)
dir.create(paste(output_dir, "diagnostics/location_drop_age/", sep = "/"), recursive = T)
dir.create(paste(output_dir, "diagnostics/location_best/", sep = "/"), recursive = T)

# create sandbox directories
dir.create(paste(sandbox_dir, "diagnostics/location_drop_age/", sep = "/"), recursive = T)
dir.create(paste(sandbox_dir, "diagnostics/location_best/", sep = "/"), recursive = T)

# create location specific directories
for (loc_id in unique(all_hierarchies[, location_id])) {
  ihme_loc <- all_hierarchies[location_id == loc_id, ihme_loc_id]
  print(ihme_loc)

  # create model fit output folders
  if (all_hierarchies[location_set_name == "population" & location_id == loc_id, is_estimate] == 1) {
    dir.create(paste(output_dir, loc_id, "/outputs/model_fit/", sep = "/"), recursive = T)
  }

  # Create output directories for summary and draws for aggregates and modeled locations
  dir.create(paste(output_dir, loc_id, "/outputs/summary/", sep = "/"), recursive = T)
  dir.create(paste(output_dir, loc_id, "/outputs/draws/", sep = "/"), recursive = T)
}

# save settings
settings_dir <- paste(output_dir, "/run_settings.rdata", sep = "/")

# version 2 is compatible with R versions < 3.5, which model fitting uses
save(settings, file = settings_dir, version = 2)


# save location hierarchies
readr::write_csv(all_hierarchies, paste0(output_dir, "/database/all_location_hierarchies.csv"))


# Location and census specific settings -----------------------------------

# TODO: add check for all locations and required settings
location_specific_settings <- data.table::fread(location_specific_settings_dir, na.strings=c("NA", ""))
census_specific_settings <- data.table::fread(census_specific_settings_dir, na.strings=c("NA", ""))
location_specific_settings[is.na(pooling_ages), pooling_ages := "seq(65,95,10)"]

# Make sure all boolean variable are actually encoded as strings
to_replace <- names(which(sapply(location_specific_settings, is.logical)))
location_specific_settings <- location_specific_settings[, (to_replace) := lapply(.SD, as.character), .SDcols = to_replace]
for (var in to_replace) {
  location_specific_settings[, (var) := substr(get(var), 1, 1)]
}

to_replace <- names(which(sapply(census_specific_settings, is.logical)))
census_specific_settings <- census_specific_settings[, (to_replace) := lapply(.SD, as.character), .SDcols = to_replace]
for (var in to_replace) {
  census_specific_settings[, (var) := substr(get(var), 1, 1)]
}

readr::write_csv(census_specific_settings, paste(output_dir, "/database/census_specific_settings.csv", sep = "/"))
readr::write_csv(location_specific_settings, paste(output_dir, "/database/location_specific_settings.csv", sep = "/"))


# Submit submission job ---------------------------------------------------

mortcore::qsub(jobname = paste0("submit_population_", pop_reporting_vid),
               cores = 1, mem = 1, wallclock = "48:00:00", archive_node = F,
               queue = queue, proj = submission_project_name,
               shell = "FILEPATH",
               pass_shell = list(
                 i = "FILEPATH"
               ),
               code = paste0(code_dir, "00b_submit.R"),
               pass_argparse = list(pop_vid = pop_reporting_vid,
                                    test = test),
               submit = T)


# Message slack update ----------------------------------------------------

get_model_inputs <- function(model_name, model_type, run_id) {
  library(DBI)

  process_name <- paste(model_name, model_type)

  myconn <- db_init(hostname = hostname, db_permissions = "read_write")
  on.exit(tryCatch(DBI::dbDisconnect(myconn), error=function(e){}, warning = function(w) {}))

  # get all the run ids, comments, statuses etc.
  select_command <- paste0("SELECT proc_version_id, run_id, process_id, process_name, status, gbd_round_id, comment, best_start_date, best_end_date, pv.date_inserted, pv.last_updated, pv.last_updated_by ",
                           "FROM mortality.process_version pv LEFT JOIN mortality.process using(process_id) LEFT JOIN mortality.lookup_run_status rs ON pv.status_id=rs.run_status_id")
  proc_versions <- setDT(DBI::dbGetQuery(myconn, select_command))
  proc_versions <- proc_versions[, list(parent_process_name = process_name, parent_run_id = run_id,
                                        status, comment, best_start_date, best_end_date, date_inserted, last_updated, last_updated_by)]

  # get the parent-child mappings
  select_command <- paste0("SELECT * FROM mortality.vw_process_version_map")
  pv_map <- setDT(DBI::dbGetQuery(myconn,select_command), key = c("parent_process_name", "child_process_name"))
  inputs <- pv_map[child_process_name == process_name & child_run_id == run_id, list(parent_process_name, parent_run_id)]

  inputs <- merge(inputs, proc_versions, by = c("parent_process_name", "parent_run_id"), all.x = T)

  return(inputs)
}

message_model_inputs <- function(inputs) {
  inputs_text <- paste(capture.output(inputs), collapse="\n")
  message <- paste0("Population model run id ", pop_reporting_vid, " launched!\n",
                    "Run comment: ", comment, "\n\n",
                    "Used the following inputs:", "\n\n```", inputs_text, "```")
  mortdb::send_slack_message(message = message, channel = paste0("@", USER), icon = "rocket", botname = "PopBot")
  if (!test & slack_channel_updates) send_slack_message(message = message, channel = "#population", icon = "rocket", botname = "PopBot")
}

input_versions <- get_model_inputs(model_name = "population single year", model_type = "estimate", run_id = pop_single_vid)
message_model_inputs(input_versions)
