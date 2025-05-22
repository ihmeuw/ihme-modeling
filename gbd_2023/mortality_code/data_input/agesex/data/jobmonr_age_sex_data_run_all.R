################################################################################
##
## Description: Runs all steps of age sex data pipeline
##
################################################################################

## Settings --------------------------------------------------------------------

library(argparse)
library(data.table)
library(jobmonr)
library(mortdb, lib.loc = "FILEPATH")


## Read in arguments and inputs ------------------------------------------------

# Load arguments from mortpipeline run-all
if(!interactive()) {

  parser <- argparse::ArgumentParser()
  parser$add_argument("--version_id", type = "integer", required = TRUE,
                      help = "age sex version id")
  parser$add_argument("--username", type = "character", required = TRUE,
                      help = "user conducting model run")
  parser$add_argument("--gbd_year", type = "integer", required = TRUE,
                      help = "GBD year")
  parser$add_argument("--mark_best", type = "character", required = TRUE,
                      default = FALSE,
                      help = "TRUE/FALSE mark run as best")
  parser$add_argument("--workflow_args", type = "character", required = TRUE,
                      action = "store",
                      help = "workflow name")

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
  model_name = "age sex",
  model_type = "data",
  run_id = version_id
)
births_version_id <- version_lineage[
  parent_process_name == "birth estimate", parent_run_id
]
pop_est_version_id <- version_lineage[
  parent_process_name == "population estimate", parent_run_id
]
model_5q0_version_id <- version_lineage[
  parent_process_name == "5q0 estimate", parent_run_id
]
data_5q0_version_id <- version_lineage[
  parent_process_name == "5q0 data", parent_run_id
]
ddm_version_id <- version_lineage[
  parent_process_name == "ddm estimate", parent_run_id
]

# Get locations
loc_map <- get_locations(
  level = "all",
  gbd_type = "ap_old",
  gbd_year = gbd_year
)

readr::write_csv(loc_map, fs::path("FILEPATH"))

# Get subnat map
subnat_loc_map <- copy(loc_map)
subnat_loc_map <- subnat_loc_map[level >= 4, .(ihme_loc_id, level, parent_id)]

readr::write_csv(subnat_loc_map, fs::path("FILEPATH"))

# Get source type map
source_type_ids <- get_mort_ids(type = "source_type")
source_type_ids <- source_type_ids[, .(source_type_id, type_short = tolower(type_short))]

readr::write_csv(source_type_ids, fs::path("FILEPATH"))

# Get method ids
method_ids <- get_mort_ids(type = "method")
method_ids <- method_ids[, .(method_id, method_short)]

readr::write_csv(method_ids, fs::path("FILEPATH"))

# Get births
births_cov <- get_mort_outputs(
  model_name = "birth",
  model_type = "estimate",
  run_id = births_version_id,
  age_group_ids = 169,
  demographic_metadata = TRUE
)

births_cov <- births_cov[location_id != 6]
births_cov[sex_id == 1, sex := "male"]
births_cov[sex_id == 2, sex := "female"]
births_cov[sex_id == 3, sex := "both"]

births_cov <- births_cov[, .(age_group_id = 22, source = "Shared function",
                             year = year_id, births = mean,
                             location_id, ihme_loc_id, sex_id, sex)]

readr::write_csv(births_cov, fs::path("FILEPATH"))

# Get population
pop <- get_mort_outputs(
  model_name = "population",
  model_type = "estimate",
  run_id = pop_est_version_id
)

pop <- pop[, .(population = mean, location_id, sex_id, year_id, age_group_id)]

readr::write_csv(pop, fs::path("FILEPATH"))

# Get 5q0 estimates
est_5q0 <- get_mort_outputs(
  model_name = "5q0",
  model_type = "estimate",
  run_id = model_5q0_version_id,
  estimate_stage_ids = 3
)

est_5q0 <- est_5q0[, .(ihme_loc_id, year = year_id, mean)]

readr::write_csv(est_5q0, fs::path("FILEPATH"))

# Copy star data
file.copy(
  "FILEPATH",
  fs::path("FILEPATH"),
  overwrite = TRUE
)

## Create Jobmon workflow, task templates, and tasks ---------------------------

# Create workflow
wf_tool <- tool(name = "age_sex_data")

set_default_tool_resources(
  tool = wf_tool,
  default_cluster_name = jobmon_cluster_name,
  resources = list("project" = submission_project_name,
                   "constraints" = "archive")
)

# Create templates
cbh_template <- task_template(
  tool = wf_tool,
  template_name = "cbh",
  command_template = paste(
    ""
  ),
  op_args = list("shell_path", "script_path", "image_path"),
  task_args = list(),
  node_args = list("version_id")
)


generic_template <- task_template(
  tool = wf_tool,
  template_name = "generic_template",
  command_template = paste(
    ""
  ),
  op_args = list("shell_path", "image_path"),
  task_args = list(),
  node_args = list("version_id", "ddm_version_id", "gbd_year", "script_path")
)

outliering_template <- task_template(
  tool = wf_tool,
  template_name = "outliering",
  command_template = paste(
    ""
  ),
  op_args = list("shell_path", "image_path", "script_path"),
  task_args = list(),
  node_args = list("version_id", "ddm_version_id", "gbd_year", "mark_best",
                   "data_5q0_version_id", "model_5q0_version_id")
)

# Create workflow
wf <- workflow(
  tool = wf_tool,
  workflow_args = paste0("data_age_sex_", workflow_args)
)

wf$workflow_attributes = list()

# Create tasks
cbh_task <- task(
  task_template = cbh_template,
  name = "prep_cbh",
  upstream_tasks = list(),
  max_attempts = 3,
  shell_path = shell_path_r,
  image_path = image_path,
  script_path = fs::path("FILEPATH"),
  version_id = as.integer(version_id),
  compute_resources = list(
    ""
  )
)

wf <- add_tasks(workflow = wf, tasks = list(cbh_task))

noncod_vr_task <- task(
  task_template = generic_template,
  name = "prep_noncod_vr",
  upstream_tasks = list(),
  max_attempts = 3,
  shell_path = shell_path_r,
  image_path = image_path,
  script_path = fs::path("FILEPATH"),
  version_id = as.integer(version_id),
  gbd_year = as.integer(gbd_year),
  ddm_version_id = as.integer(ddm_version_id),
  compute_resources = list(
    ""
  )
)

wf <- add_tasks(workflow = wf, tasks = list(noncod_vr_task))

compile_task <- task(
  task_template = generic_template,
  name = "compile",
  upstream_tasks = list(noncod_vr_task, cbh_task),
  max_attempts = 3,
  shell_path = shell_path_r,
  image_path = image_path,
  script_path = fs::path("FILEPATH"),
  version_id = as.integer(version_id),
  gbd_year = as.integer(gbd_year),
  ddm_version_id = as.integer(ddm_version_id),
  compute_resources = list(
    ""
  )
)

wf <- add_tasks(workflow = wf, tasks = list(compile_task))

outliering_task <- task(
  task_template = outliering_template,
  name = "outliering",
  upstream_tasks = list(compile_task),
  max_attempts = 3,
  shell_path = shell_path_r,
  image_path = image_path,
  script_path = fs::path("FILEPATH"),
  version_id = as.integer(version_id),
  gbd_year = as.integer(gbd_year),
  ddm_version_id = as.integer(ddm_version_id),
  model_5q0_version_id = as.integer(model_5q0_version_id),
  data_5q0_version_id = as.integer(data_5q0_version_id),
  mark_best = mark_best,
  compute_resources = list(
    ""
  )
)

wf <- add_tasks(workflow = wf, tasks = list(outliering_task))

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
  stop("Age sex data failed!")

} else {

  message("Age sex data completed!")

}

