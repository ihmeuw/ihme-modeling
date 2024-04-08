################################################################################
# Description: Submits new version of population data processing.
# - Generate new version
# - Establish parent to child relationships
# - Generate directories
# - Submit jobs
################################################################################

library(data.table)
library(readr)
library(parallel)
library(mortdb)
library(mortcore)

rm(list = ls())
USER <- Sys.getenv("USER")
code_dir <- "FILEPATH"
Sys.umask(mode = "0002")


# Run settings ------------------------------------------------------------

test <- F
best <- F
gbd_year <- 2020
end_year <- 2022
comment <- "Informative comment"


# Create new estimate version ---------------------------------------------

hostname <- "HOST"
if (test) {
  pop_processing_vid <- 99999
} else {
  pop_processing_vid <- gen_new_version(model_name = "census processed", model_type = "data",
                                        comment = comment, gbd_year = gbd_year,
                                        hostname = hostname)
}


# Make settings list ------------------------------------------------------

gbd_year_previous <- mortdb::get_gbd_year(mortdb::get_gbd_round(gbd_year) - 1)

settings <- list(
  pop_processing_vid = pop_processing_vid,
  gbd_year = gbd_year,
  gbd_year_previous = gbd_year_previous,

  comment = comment,

  test = test,
  best = best,
  hostname = hostname,

  queue = "all",
  submission_project_name = "proj_popreconstruction",
  r_shell_singularity_version = "4133",

  # basic settings
  terminal_age = 95,
  year_start = 1950,
  year_end = end_year,
  age_group_table_terminal_age = 125,

  # figure out best version of all inputs needed
  decomp_step = "iterative", # used to pull sdi
  pop_current_round_run_id = mortdb::get_proc_version(model_name = "population", model_type = "estimate", run_id = "best", gbd_year = gbd_year),
  pop_previous_round_run_id = mortdb::get_proc_version(model_name = "population", model_type = "estimate", run_id = "best", gbd_year = gbd_year_previous),
  pop_single_current_round_run_id = mortdb::get_proc_version(model_name = "population single year", model_type = "estimate", run_id = "best", gbd_year = gbd_year),
  pop_single_previous_round_run_id = mortdb::get_proc_version(model_name = "population single year", model_type = "estimate", run_id = "best", gbd_year = gbd_year_previous),
  comparator_data_run_id = mortdb::get_proc_version(model_name = "population comparison", model_type = "data", run_id = "best", gbd_year = gbd_year),
  pes_data_run_id = mortdb::get_proc_version(model_name = "pes", model_type = "data", run_id = "best", gbd_year = gbd_year),
  no_shock_death_number_run_id = mortdb::get_proc_version(model_name = "no shock death number", model_type = "estimate", run_id = "best", gbd_year = gbd_year),
  best_pes_dismod_version_id = 304259,

  census_data_vid = mortdb::get_proc_version(model_name = "census raw", model_type = "data", run_id = "best", gbd_year = gbd_year),
  census_id_vars = c("location_id", "year_id", "nid", "underlying_nid",
                     "source_name", "record_type", "method_short", "pes_adjustment_type", "outlier_type", "data_stage"),
  subnational_mappings_dir = paste0("/home/j/WORK/02_fertilitypop/population/gbd_2019/data_processing/subnational_mapping/"),
  no_baseline_locs = c("ERI", "PAK_53617"),
  use_raw_net_pes_adjustment_locs = c("AUS", "USA", "NZL", "IND", "COL"),
  adjust_non_registry_data_locations = c("OMN", "PER"), # if there are locations where we want to adjust data other than population registry data

  pop_processing_current_round_vid = mortdb::get_proc_version(model_name = "census processed", model_type = "data", run_id = "best", gbd_year = gbd_year),
  pop_processing_previous_round_vid = mortdb::get_proc_version(model_name = "census processed", model_type = "data", run_id = "best", gbd_year = gbd_year_previous),

  # location and census specific settings files
  location_specific_settings_dir = "FILEPATH",
  census_specific_settings_dir = "FILEPATH",

  output_dir = "FILEPATH",
  sandbox_dir = "FILEPATH",

  shared_functions_dir = "FILEPATH"
)
source(paste0(settings$shared_functions_dir, "get_covariate_estimates.R"))
settings$sdi_mv_id <- unique(get_covariate_estimates(covariate_id = 881, gbd_round_id = mortdb::get_gbd_round(gbd_year), decomp_step = settings$decomp_step, location_id = 1, year_id = gbd_year)$model_version_id)
settings$full_life_table_run_id <- mortdb::get_proc_lineage(model_name = "no shock death number", model_type = "estimate", run_id = settings$no_shock_death_number_run_id, lineage_type = "child")[child_process_name == "full life table estimate", child_run_id]

# load into the environment
list2env(settings, envir = environment())


# Parent-child relationships ----------------------------------------------

# generate parent to child relationships
if (!test) {
  mortdb::gen_parent_child(parent_runs = list("census raw data" = census_data_vid,
                                              "population single year estimate" = pop_single_current_round_run_id,
                                              "population estimate" = pop_current_round_run_id,
                                              "pes data" = pes_data_run_id,
                                              "population comparison data" = comparator_data_run_id,
                                              "no shock death number estimate" = no_shock_death_number_run_id, # we use the full lifetables that are versioned in the file system by this run id
                                              "full life table estimate" = full_life_table_run_id), # we use the full life tables but they aren't always uploaded so we pull from the file system
                           child_process = "census processed data", child_id = pop_processing_vid)
  mortdb::gen_external_input_map(external_input_versions = list("sdi" = sdi_mv_id,
                                                                "pes_dismod" = best_pes_dismod_version_id),
                                 process_name = "census processed data", run_id = pop_processing_vid)
}


# Create directories ------------------------------------------------------

# delete old test version if running a new one
if (test) unlink(output_dir, recursive = T)
if (test) unlink(sandbox_dir, recursive = T)

dir.create(paste0(output_dir, "/task_map"), recursive = T)
dir.create(paste0(output_dir, "/inputs"), recursive = T)
dir.create(paste0(output_dir, "/outputs"), recursive = T)
dir.create(paste0(output_dir, "/diagnostics/location_specific/"), recursive = T)
dir.create(paste0(output_dir, "/diagnostics/compiled_super_regions/"), recursive = T)
dir.create(paste0(output_dir, "/diagnostics/compiled_subnationals/"), recursive = T)

# save settings
settings_dir <- paste(output_dir, "/run_settings.rdata", sep = "/")
save(settings, file = settings_dir)


# Location hierarchies ----------------------------------------------------

hierarchy_name <- ifelse(gbd_year == 2017, "mortality", "population")
location_hierarchy <- mortdb::get_locations(gbd_year = gbd_year, gbd_type = hierarchy_name, level = "all")
# make GBR a location that is not modeled for the population hierarchy
if (hierarchy_name == "mortality") location_hierarchy[ihme_loc_id == "GBR", is_estimate := 0]
readr::write_csv(location_hierarchy, path = paste0(output_dir, "/inputs/location_hierarchy.csv"))


# Submit submission job ---------------------------------------------------

mortcore::qsub(jobname = paste0("submit_population_processing_", pop_processing_vid),
               cores = 1,
               mem = 1,
               wallclock = "00:05:00",
               archive_node = F,
               queue = queue,
               proj = submission_project_name,
               shell = "FILEPATH",
               pass_shell = list(
                 i = "FILEPATH"
               ),
               code = paste0(code_dir, "00b_submit.R"),
               pass_argparse = list(pop_processing_vid = pop_processing_vid,
                                    test = test,
                                    resubmit = FALSE),
               submit = T)


# Message slack update ----------------------------------------------------

get_model_inputs <- function(model_name, model_type, run_id) {
  library(DBI)

  process_name <- paste(model_name, model_type)

  myconn <- db_init(hostname = hostname, db_permissions = "read_write")
  on.exit(tryCatch(DBI::dbDisconnect(myconn), error=function(e){}, warning = function(w) {}))

  # get all the run ids, comments, statuses etc.
  select_command <- paste0("QUERY")
  proc_versions <- setDT(DBI::dbGetQuery(myconn, select_command))
  proc_versions <- proc_versions[, list(parent_process_name = process_name, parent_run_id = run_id,
                                        status, comment, best_start_date, best_end_date, date_inserted, last_updated, last_updated_by)]

  # get the parent-child mappings
  select_command <- paste0("QUERY")
  pv_map <- setDT(DBI::dbGetQuery(myconn,select_command), key = c("parent_process_name", "child_process_name"))
  inputs <- pv_map[child_process_name == process_name & child_run_id == run_id, list(parent_process_name, parent_run_id)]

  inputs <- merge(inputs, proc_versions, by = c("parent_process_name", "parent_run_id"), all.x = T)

  return(inputs)
}

message_model_inputs <- function(inputs) {
  inputs_text <- paste(capture.output(inputs), collapse="\n")
  message <- paste0("Census processing run id ", pop_processing_vid, " launched!\n",
                    "Run comment: ", comment, "\n\n",
                    "Used the following inputs:", "\n\n```", inputs_text, "```")
  send_slack_message(message = message, channel = "#population", icon = "rocket", botname = "PopBot")
}


input_versions <- get_model_inputs(model_name = "census processed", model_type = "data", run_id = pop_processing_vid)
message_model_inputs(input_versions)
