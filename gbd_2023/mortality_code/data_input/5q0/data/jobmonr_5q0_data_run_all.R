################################################################################
##
## Description: Runs all steps of 5q0 data pipeline
##
################################################################################

## Settings --------------------------------------------------------------------

library(argparse)
library(data.table)
library(jobmonr)
library(mortdb)


## Read in arguments and inputs ------------------------------------------------

# Load arguments from mortpipeline run-all
if(!interactive()) {

  parser <- argparse::ArgumentParser()
  parser$add_argument("--version_id", type = "integer", required = TRUE,
                      help = "5q0 version id")
  parser$add_argument("--username", type = "character", required = TRUE,
                      help = "user conducting model run")
  parser$add_argument("--gbd_year", type = "integer", required = TRUE,
                      help = "GBD year")
  parser$add_argument("--mark_best", type = "character", required = TRUE,
                      default = FALSE,
                      help = "TRUE/FALSE mark run as best")
  parser$add_argument("--workflow_args", type = "character", required = TRUE,
                      action = "store", help = "workflow name")

  args <- parser$parse_args()
  list2env(args, .GlobalEnv)

} else {

  version_id <-
  username <- "USERNAME"
  gbd_year <-
  workflow_args <- ""
  mark_best <-

}

code_dir <- fs::path("FILEPATH")
configs_dir <- fs::path("FILEPATH")

# Load mortality production config file
config <- config::get(
  file = fs::path("FILEPATH"),
  use_parent = FALSE
)
list2env(config, .GlobalEnv)

# Set file paths
run_folder <- fs::path("FILEPATH")
input_folder <- fs::path("FILEPATH")
output_folder <- fs::path("FILEPATH")

for(dir in c(run_folder, input_folder, output_folder)) {

  dir.create(dir)

}

# Pull lineage
version_lineage <- get_proc_lineage(
  model_name = "5q0",
  model_type = "data",
  run_id = version_id)
ddm_version_id <- version_lineage[
  parent_process_name == "ddm estimate", parent_run_id
]

# Save locations
loc_map <- get_locations(
  level = "all",
  gbd_type = "ap_old",
  gbd_year = gbd_year
)

loc_map_2015 <- get_locations(
  level = "all",
  gbd_year = 2015
)[, .(location_id, ihme_loc_id)]

loc_map_2016 <- get_locations(
  level = "all",
  gbd_type = "ap_old",
  gbd_year = 2016
)[, .(location_id, ihme_loc_id, location_name)]

readr::write_csv(loc_map, fs::path("FILEPATH"))
readr::write_csv(loc_map_2015, fs::path("FILEPATH"))
readr::write_csv(loc_map_2016, fs::path("FILEPATH"))

# Save sources and methods
source_type_ids <- get_mort_ids(type = "source_type")[, .(source_type_id, type_short)]
source_type_ids[, type_short := tolower(type_short)]

method_ids <- get_mort_ids(type = "method")[, .(method_id, method_short)]

readr::write_csv(source_type_ids, fs::path("FILEPATH"))
readr::write_csv(method_ids, fs::path("FILEPATH"))

# Save births
births <- get_mort_outputs(
  model_name = "birth",
  model_type = "estimate",
  age_group_ids = 169,
  demographic_metadata = TRUE,
  gbd_year = gbd_year
)
births <- births[location_id != 6]
births[, source := "get_mort_outputs"]
births <- births[, .(sex = tolower(sex),  sex_id,
                     location_id, ihme_loc_id, location_name, source,
                     year = year_id, births = mean)]

readr::write_csv(births, fs::path("FILEPATH"))

## Create Jobmon workflow, task templates, and tasks ---------------------------

# Create workflow
wf_tool <- tool(name = "5q0_data")

set_default_tool_resources(
  tool = wf_tool,
  default_cluster_name = jobmon_cluster_name,
  resources = list("project" = submission_project_name,
                   "constraints" = "archive")
)

# Create templates
combine_all_child_template <- task_template(
  tool = wf_tool,
  template_name = "01_combine_all_child",
  command_template = paste(
    ""
  ),
  op_args = list("shell_path", "script_path", "image_path"),
  task_args = list(),
  node_args = list("version_id", "ddm_version_id")
)

generic_ver_template <- task_template(
  tool = wf_tool,
  template_name = "02_add_nids",
  command_template = paste(
    ""
  ),
  op_args = list("shell_path", "image_path"),
  task_args = list(),
  node_args = list("version_id", "script_path")
)

upload_template <- task_template(
  tool = wf_tool,
  template_name = "04_upload",
  command_template = paste(
    ""
  ),
  op_args = list("shell_path", "script_path", "image_path"),
  task_args = list(),
  node_args = list("version_id", "mark_best")
)

# Create workflow
wf <- workflow(
  tool = wf_tool,
  workflow_args = paste0("data_5q0_", workflow_args)
)

wf$workflow_attributes = list()

# Create tasks
combine_all_child_task <- task(
  task_template = combine_all_child_template,
  name = "combine_all_child_sources",
  upstream_tasks = list(),
  max_attempts = 3,
  shell_path = shell_path_r,
  image_path = image_path,
  script_path = fs::path("FILEPATH"),
  version_id = as.integer(version_id),
  ddm_version_id = as.integer(ddm_version_id),
  compute_resources = list(
    ""
  )
)

wf <- add_tasks(workflow = wf, tasks = list(combine_all_child_task))

add_nids_task <- task(
  task_template = generic_ver_template,
  name = "add_nids",
  upstream_tasks = list(combine_all_child_task),
  max_attempts = 3,
  shell_path = shell_path_r,
  image_path = image_path,
  script_path = fs::path("FILEPATH"),
  version_id = as.integer(version_id),
  compute_resources = list(
    ""
  )
)

wf <- add_tasks(workflow = wf, tasks = list(add_nids_task))

format_task <- task(
  task_template = generic_ver_template,
  name = "prep_for_upload",
  upstream_tasks = list(add_nids_task),
  max_attempts = 3,
  shell_path = shell_path_r,
  image_path = image_path,
  script_path = fs::path("FILEPATH"),
  version_id = as.integer(version_id),
  compute_resources = list(
    ""
  )
)

wf <- add_tasks(workflow = wf, tasks = list(format_task))

upload_task <- task(
  task_template = upload_template,
  name = "upload",
  upstream_tasks = list(format_task),
  max_attempts = 3,
  shell_path = shell_path_r,
  image_path = image_path,
  script_path = fs::path("FILEPATH"),
  version_id = as.integer(version_id),
  mark_best = mark_best,
  compute_resources = list(
    ""
  )
)

wf <- add_tasks(workflow = wf, tasks = list(upload_task))

## Run -------------------------------------------------------------------------

wfr <- wf$run(resume = TRUE)


if(wfr != "D") {

  # Send failure notification
  send_slack_message(
    message = paste0(""),
    channel = paste0(""),
    icon = "",
    botname = ""
  )

  # Stop process
  stop("5q0 data failed!")

} else {

  message("5q0 data completed!")

}

