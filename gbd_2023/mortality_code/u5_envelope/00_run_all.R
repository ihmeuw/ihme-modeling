################################################################################
##                                                                            ##
## U5 Envelope Run All                                                        ##
##                                                                            ##
################################################################################

library(argparse)
library(data.table)
library(mortdb, lib.loc = "FILEPATH")
library(jobmonr)

## Read in arguments and inputs ------------------------------------------------

if (interactive()) {

  version_id <-
  username <- Sys.getenv("USER")
  gbd_year <-
  with_shocks <- ""
  code_dir <- "FILEPATH"
  births_draws_version <-
  version_age_sex_id <-
  workflow_args <- format(Sys.time(), "%Y-%m-%d-%H-%M")

} else {

  parser <- argparse::ArgumentParser()

  parser$add_argument(
    "--version_id", type = "integer", required = TRUE,
    help = "U5 envelope version id"
  )
  parser$add_argument(
    "--username", type = "character", required = TRUE,
    help = "user conducting model run"
  )
  parser$add_argument(
    "--gbd_year", type = "integer", required = TRUE,
    help = "GBD year"
  )
  parser$add_argument(
    "--end_year", type = "integer", required = TRUE,
    help = "last year to make estimates for"
  )
  parser$add_argument(
    "--births_draws_version", type = "character", required = TRUE,
    help = "Births draws version"
  )
  parser$add_argument(
    "--with_shocks", type = "character", required = TRUE, default = "no",
    help = "whether to read in with shocks or no shocks"
  )
  parser$add_argument(
    "--code_dir", type = "character", required = TRUE,
    help = "directory where code is cloned"
  )
  parser$add_argument(
    "--workflow_args", type = "character", required = TRUE,
    action = "store", help = "workflow name"
  )

  args <- parser$parse_args()
  list2env(args, .GlobalEnv)

  births_draws_version <- as.integer(
    mortdb::get_proc_version(
      model_name = "birth",
      model_type = "estimate",
      run_id = births_draws_version
    )
  )

  proc_lineage <- mortdb::get_proc_lineage(
    model_name = "u5_envelope",
    model_type = "estimate",
    run_id = version_id
  )

  version_age_sex_id <- as.integer(
    proc_lineage[parent_process_name == "age sex estimate"]$parent_run_id
  )

}

# Load mortality production config file
config <- config::get(
  file = fs::path("FILEPATH"),
  use_parent = FALSE
)

list2env(config, .GlobalEnv)

code_dir <- paste0("FILEPATH")

# Set filepaths and create directories
main_dir <- paste0("FILEPATH")
draws_dir <- paste0("FILEPATH")

dir.create(main_dir)
dir.create(draws_dir)

## Get location hierarchy ------------------------------------------------------

location_hierarchy <- mortdb::get_locations(
  gbd_year = gbd_year,
  level = "estimate"
)

parent_locations <- location_hierarchy[level_1 == 1 & level_2 == 0]

# Save location hierarchy to avoid parallelizing db query
readr::write_csv(location_hierarchy, fs::path("FILEPATH"))

## Create Jobmon workflow, task templates, and tasks ---------------------------

# Create new workflow
wf_tool <- tool(name = "mort_u5")

wf_tool <- set_default_tool_resources(
  tool = wf_tool,
  default_cluster_name = jobmon_cluster_name,
  resources = list("project" = submission_project_name)
)

# Create task templates
template_u5 <- task_template(
  tool = wf_tool,
  template_name = "u5",
  command_template = paste(
    ""
  ),
  op_args = list("script_path", "conda_path", "conda_env", "code_path"),
  task_args = list(),
  node_args = list("end_year", "location_id", "ihme_loc_id", "version_id",
                   "version_age_sex_id", "births_draws_version", "with_shocks")
)

template_scaling_u5 <- task_template(
  tool = wf_tool,
  template_name = "scaling_u5",
  command_template = paste(
    ""
  ),
  op_args = list("shell_path", "image_path", "script_path"),
  task_args = list(),
  node_args = list("location_id", "version_id", "gbd_year")
)

template_summarizing_u5 <- task_template(
  tool = wf_tool,
  template_name = "summarizing_u5",
  command_template = paste(
    ""
  ),
  op_args = list("script_path", "conda_path", "conda_env", "code_path"),
  task_args = list(),
  node_args = list("location_id", "version_id")
)

# Create tasks and add to workflow
wf <- workflow(
  tool = wf_tool,
  workflow_args = paste0("u5_", workflow_args)
)

wf$workflow_attributes <- list()

tasks_u5 <- lapply(1:nrow(location_hierarchy), function(i) {

  task_u5 <- task(
    task_template = template_u5,
    name = paste0("u5_", version_id, "_", location_hierarchy[i]$location_id),
    upstream_tasks = list(),
    max_attempts = 2,
    code_path = glue::glue("FILEPATH"),
    conda_path = conda_path,
    conda_env = conda_env,
    script_path = glue::glue("FILEPATH"),
    location_id = as.integer(location_hierarchy[i]$location_id),
    ihme_loc_id = location_hierarchy[i]$ihme_loc_id,
    version_id = version_id,
    version_age_sex_id = version_age_sex_id,
    births_draws_version = births_draws_version,
    end_year = end_year,
    with_shocks = with_shocks,
    compute_resources = list(
      ""
    )
  )

  return(task_u5)

})

wf <- add_tasks(
  workflow = wf,
  tasks = tasks_u5
)

tasks_scaling_u5 <- lapply(1:nrow(parent_locations), function(i) {

  task_scaling_u5 <- task(
    task_template = template_scaling_u5,
    name = paste0("scaling_u5_", version_id, "_", location_hierarchy[i]$location_id),
    upstream_tasks = tasks_u5,
    max_attempts = 2,
    shell_path = shell_path_r,
    image_path = image_path,
    script_path = glue::glue("FILEPATH"),
    version_id = version_id,
    location_id = as.integer(location_hierarchy[i]$location_id),
    gbd_year = gbd_year,
    compute_resources = list(
      ""
    )
  )

  return(task_scaling_u5)

})

wf <- add_tasks(
  workflow = wf,
  tasks = tasks_scaling_u5
)

tasks_summarizing_u5 <- lapply(1:nrow(location_hierarchy), function(i) {

  task_summarizing_u5 <- task(
    task_template = template_summarizing_u5,
    name = paste0("summarize_u5_", version_id, "_", location_hierarchy[i]$location_id),
    upstream_tasks = tasks_scaling_u5,
    max_attempts = 2,
    code_path = glue::glue("FILEPATH"),
    conda_path = conda_path,
    conda_env = conda_env,
    script_path = glue::glue("FILEPATH"),
    version_id = version_id,
    location_id = as.integer(location_hierarchy[i]$location_id),
    compute_resources = list(
      ""
    )
  )

  return(task_summarizing_u5)

})

wf <- add_tasks(
  workflow = wf,
  tasks = tasks_summarizing_u5
)

## Run workflow ----------------------------------------------------------------

wfr <- wf$run(resume = TRUE)

## Upload to database or report model failure ----------------------------------

if (wfr == "D") {

  print("U5 Envelope completed!")

  mortdb::update_status(
    model_name = "u5 envelope",
    model_type = "estimate",
    run_id = version_id,
    new_status = "completed",
    send_slack = TRUE
  )

} else {

  mortdb::send_slack_message(
    message = paste0(""),
    channel = paste0(""),
    icon = "",
    botname = ""
  )

  # Stop process
  stop("U5 Envelope failed!")

}
