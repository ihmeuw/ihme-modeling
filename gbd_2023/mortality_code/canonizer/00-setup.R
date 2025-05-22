
# Meta --------------------------------------------------------------------

# Set up new run of canonizer


# Load packages -----------------------------------------------------------

library(data.table)
library(jobmonr)


# Parse arguments ---------------------------------------------------------

parser <- argparse::ArgumentParser()

# Workflow-related arguments
parser$add_argument(
  "--code_dir",
  type = "character",
  required = !interactive(),
  default = fs::path_dir(here::here()),
  help = "Working directory for code"
)
parser$add_argument(
  "--workflow_args",
  type = "character",
  required = !interactive(),
  default = format(Sys.time(), "%Y_%m_%d-%H_%M_%S"),
  help = "Workflow args from main run-all"
)

# Version arguments
parser$add_argument(
  "--version",
  type = "integer",
  required = !interactive(),
  default = 463,
  help = "Version of canonizer process"
)

# Process arguments
parser$add_argument(
  "--gbd_year",
  type = "integer",
  required = !interactive(),
  default = 2021
)
parser$add_argument(
  "--start_year",
  type = "integer",
  required = !interactive(),
  default = 1950,
  help = "First year of estimation"
)
parser$add_argument(
  "--end_year",
  type = "integer",
  required = !interactive(),
  default = 2022,
  help = "Last year of estimation"
)
parser$add_argument(
  "--upload_lt_env",
  action = "store_true",
  help = "Upload summary life tables and envelopes"
)
parser$add_argument(
  "--special_aggregates",
  action = "store_true",
  help = "Create special reporting location aggregates"
)

args <- parser$parse_args()


# Set parameters ----------------------------------------------------------

cfg <- config::get(file = fs::path(args$code_dir, "canonize-mortality/config.yml"))

dir <- cfg$dir
stopifnot(all(purrr::map_lgl(dir, fs::dir_exists)))
dir$main <- fs::path(dir$base, args$version)
dir$code <- fs::path(args$code_dir, "canonize-mortality")
dir$log_err <- fs::path(dir$log, Sys.getenv("USER"), "error")
dir$log_out <- fs::path(dir$log, Sys.getenv("USER"), "output")

run_years <- (args$start_year):(args$end_year)

gbd_round_id <- demInternal::get_gbd_round(args$gbd_year)


# Load maps ---------------------------------------------------------------

map_proc <- c(
  "no shock life table estimate" = "nslt",
  "no shock death number estimate" = "nsdn",
  "with shock life table estimate" = "wslt",
  "with shock death number estimate" = "wsdn",
  "population estimate" = "pop",
  "population single year estimate" = "pop_sy"
)

map_loc_lowest <- demInternal::get_locations(gbd_year = args$gbd_year, level = "lowest")[level > 2]
map_loc <- demInternal::get_locations(gbd_year = args$gbd_year)

all_locs <- map_loc$location_id

if (isTRUE(args$special_aggregates)) {

  map_loc_special <-
    cfg$special_location_sets |>
    rlang::set_names() |>
    lapply(\(x) demInternal::get_locations(
      gbd_year = args$gbd_year,
      location_set_name = x
    )) |>
    rbindlist(idcol = "location_set")

  all_locs <- unique(c(all_locs, map_loc_special$location_id))

}


# Get run versions --------------------------------------------------------

list_versions <- list(

  parent =
    demInternal::get_parent_child(
      "full life table estimate", args$version, "parent"
    )[parent_process_name %like% "^pop", .(parent_process_name, parent_run_id)] |>
    tibble::deframe() |>
    rlang::set_names(\(x) map_proc[x]) |>
    as.list(),

  child =
    demInternal::get_parent_child(
      "full life table estimate", args$version, "child"
    )[, .(child_process_name, child_run_id)] |>
    tibble::deframe() |>
    rlang::set_names(\(x) map_proc[x]) |>
    as.list(),

  external =
    mortdb::get_external_input_map(
      "full life table estimate", args$version
    )[, .(external_input_name, external_input_version)] |>
    tibble::deframe() |>
    as.list()

)


# Create directories ------------------------------------------------------

fs::dir_create(
  c(
    dir$main,
    fs::path(dir$finalizer, list_versions$child$wsdn)
  ),
  mode = "775"
)

main_subdir <- c(
  "cache",
  "output",
  "logs",
  "diagnostics",
  "upload"
)

subdir_output <- c(
  "most_detailed_lt",
  "agg_full_lt",
  "final_full_lt",
  "final_abridged_lt_env",
  "final_full_lt_summary",
  "final_abridged_lt_summary",
  "final_abridged_env_summary",
  "final_report_u5_lt_env"
)

subdir_logs <- c(
  "low_terminal_mx",
  "high_with_shock_qx",
  "bad_most_detailed_qx",
  "qx_ax_change",
  "high_ex",
  "big_abridged_ax",
  "qx_ax_change_abridged",
  "mean_outside_ui_abr"
)

fs::dir_create(fs::path(dir$main, main_subdir), mode = "775")
fs::dir_create(fs::path(dir$main, "output", subdir_output), mode = "775")
fs::dir_create(fs::path(dir$main, "logs", subdir_logs), mode = "775")

subdir_agg_full_lt <- fs::path(dir$main, "output/agg_full_lt", paste0("location_id=", all_locs))
fs::dir_create(subdir_agg_full_lt, mode = "775")


# Save versioning ---------------------------------------------------------

yaml::write_yaml(list_versions, fs::path(dir$main, "versions.yml"))


# Create workflow ---------------------------------------------------------

wf_tool <-
  jobmonr::tool(name = "Canonize Mortality") |>
  jobmonr::set_default_tool_resources(
    default_cluster_name = "slurm",
    resources = list(
      project = "proj_mortenvelope",
      queue = "all.q",
      stdout = dir$log_out,
      stderr = dir$log_err
    )
  )

wf <- jobmonr::workflow(
  tool = wf_tool,
  workflow_args = args$workflow_args,
  name = "Canonize Mortality"
)

source(fs::path(dir$code, "00a-create_workflow.R"))

task_cache_inputs <- gen_task_cache_inputs(wf_tool)

jobmonr::add_tasks(wf, list(task_cache_inputs))

task_array_canonical_lt <- gen_task_array_canonical_lt(
  wf_tool = wf_tool,
  loc_ids = map_loc_lowest$location_id,
  upstream = list(task_cache_inputs)
)

jobmonr::add_tasks(wf, task_array_canonical_lt)

task_array_agg_lt <- gen_task_array_agg_lt(
  wf_tool = wf_tool,
  year_ids = run_years,
  upstream = task_array_canonical_lt
)

jobmonr::add_tasks(wf, task_array_agg_lt)

task_array_abr_lt <- gen_task_array_abr_lt(
  wf_tool = wf_tool,
  loc_ids = all_locs,
  upstream = task_array_agg_lt
)

jobmonr::add_tasks(wf, task_array_abr_lt)

if (args$upload_lt_env) {

  tasks_upload <- list_versions$child |>
    purrr::imap(\(x, i) gen_task_upload_lt_env(
      wf_tool = wf_tool,
      process_code = i,
      run_id = x,
      upstream = task_array_abr_lt
    )) |>
    unname()

  jobmonr::add_tasks(wf, tasks_upload)

}


# Run workflow ------------------------------------------------------------

wfr <- wf$run(resume = TRUE, fail_fast = TRUE)

if (wfr == "D") {
  message("Workflow Completed!")
} else {
  stop("An issue occurred.")
}
