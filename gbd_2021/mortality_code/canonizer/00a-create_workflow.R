
# Meta --------------------------------------------------------------------

# Define functions for adding tasks to a workflow

# This script should be sourced within the script that actually handles
# launching the workflow, and thus these functions rely on many variables
# already existing in their parent environment.

# The only task variables passed explicitly to these functions are the
# workflow tool, any upstream task dependencies, and any node arguments that
# define task parallelization.


# Helper functions --------------------------------------------------------

create_template <- function(name, tool, node_args = list(), task_args = list(), cmd_prefix = c()) {

  all_vars <- c(names(node_args), names(task_args))
  command_args <- paste0("--", all_vars, " {", all_vars, "}")
  command_template <- trimws(paste(
    cmd_prefix,
    "{shell} -i {image} -s {script}",
    paste0(command_args, collapse = " ")
  ))

  jobmonr::task_template(
    tool = tool,
    template_name = trimws(name),
    command_template = command_template,
    node_args = as.list(names(node_args)),
    task_args = as.list(names(task_args)),
    op_args = list("shell", "image", "script")
  )

}


# Create tasks ------------------------------------------------------------

gen_task_cache_inputs <- function(wf_tool, upstream = list()) {

  path_script <- fs::path(dir$code, "01-cache_db.R")
  stopifnot(fs::file_exists(path_script))

  task_args <- list(
    version = as.integer(args$version),
    run_id_pop = as.integer(list_versions$parent$pop),
    run_id_pop_sy = as.integer(list_versions$parent$pop_sy),
    gbd_year = as.integer(args$gbd_year),
    special_aggregates = as.logical(args$special_aggregates),
    code_dir = dir$code
  )

  template <- create_template(
    "Cache Inputs",
    wf_tool,
    task_args = task_args
  )

  rlang::inject(jobmonr::task(
    task_template = template,
    name = "cache_inputs",
    upstream_tasks = unname(upstream),
    compute_resources = list(memory = "5G", cores = 4, runtime = "10m"),
    shell = cfg$shell,
    image = cfg$image,
    script = path_script,
    !!!task_args
  ))

}

gen_task_array_canonical_lt <- function(wf_tool, loc_ids, upstream = list()) {

  path_script <- fs::path(dir$code, "02-build_canonical_lt.R")
  stopifnot(fs::file_exists(path_script))

  node_args <- list(loc_id = loc_ids)

  task_args <- list(
    version = as.integer(args$version),
    version_mlt = as.integer(list_versions$parent$mlt),
    version_u5 = as.integer(list_versions$parent$u5),
    version_shocks = as.integer(list_versions$external$shock_aggregator),
    version_hiv = list_versions$external$hiv,
    gbd_year = as.integer(args$gbd_year),
    start_year = as.integer(args$start_year),
    end_year = as.integer(args$end_year),
    code_dir = dir$code
  )

  template <- create_template(
    "Build Canonical Life Table",
    wf_tool,
    node_args = node_args,
    task_args = task_args
  )

  rlang::inject(jobmonr::array_tasks(
    task_template = template,
    upstream_tasks = unname(upstream),
    max_attempts = 2,
    compute_resources = list(memory = "14G", cores = 6, runtime = "20m"),
    shell = cfg$shell,
    image = cfg$image,
    script = path_script,
    !!!task_args,
    !!!node_args
  ))

}

gen_task_array_agg_lt <- function(wf_tool, year_ids, upstream = list()) {

  path_script <- fs::path(dir$code, "03-aggregate_location_sex.R")
  stopifnot(fs::file_exists(path_script))

  node_args <- list(agg_year = year_ids)

  task_args <- list(
    version = as.integer(args$version),
    special_aggregates = as.logical(args$special_aggregates),
    gbd_year = as.integer(args$gbd_year),
    start_year = as.integer(args$start_year),
    end_year = as.integer(args$end_year),
    code_dir = here::here()
  )

  template <- create_template(
    "Aggregate Life Tables",
    wf_tool,
    node_args = node_args,
    task_args = task_args
  )

  rlang::inject(jobmonr::array_tasks(
    task_template = template,
    upstream_tasks = unname(upstream),
    max_attempts = 2,
    compute_resources = list(memory = "96G", cores = 4, runtime = "1h"),
    shell = cfg$shell,
    image = cfg$image,
    script = path_script,
    !!!task_args,
    !!!node_args
  ))

}

gen_task_array_abr_lt <- function(wf_tool, loc_ids, upstream = list()) {

  path_script <- fs::path(dir$code, "04-abridge_lt.R")
  stopifnot(fs::file_exists(path_script))

  node_args <- list(loc_id = loc_ids)

  task_args <- list(
    version = as.integer(args$version),
    gbd_year = as.integer(args$gbd_year),
    code_dir = here::here()
  )

  template <- create_template(
    "Abridge Life Tables",
    wf_tool,
    node_args = node_args,
    task_args = task_args
  )

  rlang::inject(jobmonr::array_tasks(
    task_template = template,
    upstream_tasks = unname(upstream),
    max_attempts = 2,
    compute_resources = list(memory = "48G", cores = 8, runtime = "30m"),
    shell = cfg$shell,
    image = cfg$image,
    script = path_script,
    !!!task_args,
    !!!node_args
  ))

}

gen_task_array_hiv_deaths <- function(wf_tool, loc_ids, upstream = list()) {

  path_script <- fs::path(dir$code, "04-hiv_deaths.R")
  stopifnot(fs::file_exists(path_script))

  node_args <- list(loc_id = loc_ids)

  task_args <- list(
    version = as.integer(args$version),
    start_year = as.integer(cfg$hiv_year_start),
    end_year = as.integer(args$end_year),
    code_dir = here::here()
  )

  template <- create_template(
    "Save HIV Deaths",
    wf_tool,
    node_args = node_args,
    task_args = task_args
  )

  rlang::inject(jobmonr::array_tasks(
    task_template = template,
    upstream_tasks = unname(upstream),
    max_attempts = 2,
    compute_resources = list(memory = "2G", cores = 4, runtime = "5m"),
    shell = cfg$shell,
    image = cfg$image,
    script = path_script,
    !!!task_args,
    !!!node_args
  ))

}
