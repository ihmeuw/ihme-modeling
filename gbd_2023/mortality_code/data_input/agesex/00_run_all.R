################################################################################
##                                                                            ##
## Age-Sex Run All                                                            ##
##                                                                            ##
################################################################################

library(argparse)
library(data.table)
library(jobmonr)
library(mortdb, lib.loc = "FILEPATH")

## Read in arguments and inputs ------------------------------------------------

if (interactive()) {

  version_id <-
  username <- Sys.getenv("USER")
  mark_best <- ""
  code_dir <- "FILEPATH"
  config_dir <- paste0("FILEPATH")
  workflow_args <- format(Sys.time(), "%Y-%m-%d-%H-%M")

} else {

  parser <- argparse::ArgumentParser()

  parser$add_argument(
    "--version_id", type = "integer", required = TRUE,
    help = "age sex estimate version id"
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
    "--mark_best", type = "character", required = TRUE,
    help = "true/false to mark run as best"
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

}

# Load mortality production config file
config <- config::get(
  file = fs::path(code_dir, "FILEPATH"),
  use_parent = FALSE
)

list2env(config, .GlobalEnv)

# Additional variables
sexes <- c("male", "female")
ages <- c("enn", "lnn", "pnn", "pna", "pnb", "inf", "ch", "cha", "chb")
ages_and_sexes <- data.table(sex = rep(sexes, length(ages)), age_group_name = rep(ages, length(sexes)))

# Pull proc lineage
proc_lineage <- mortdb::get_proc_lineage(
  model_name = "age sex",
  model_type = "estimate",
  run_id = version_id
)

version_data_id <- as.integer(proc_lineage[parent_process_name == "age sex data"]$parent_run_id)
version_ddm_id <- as.integer(proc_lineage[parent_process_name == "ddm estimate"]$parent_run_id)
version_5q0_id <- as.integer(proc_lineage[parent_process_name == "5q0 estimate"]$parent_run_id)

# Pull external inputs
external_inputs <- mortdb::get_external_input_map(
  process_name = "age sex estimate",
  run_id = version_id
)

version_hiv_id <- external_inputs[external_input_name == "hiv"]$external_input_version

# Update code_dir
code_dir <- fs::path(code_dir, "age-sex")

# Set filepaths and create directories
input_dir <- paste0("FILEPATH")
output_dir <- paste0("FILEPATH")
output_5q0_dir <- paste0("FILEPATH")

if (!dir.exists(output_dir)) dir.create(output_dir)

## Get location hierarchy ------------------------------------------------------

location_data <- mortdb::get_locations(
  gbd_year = gbd_year,
  gbd_type = "mortality",
  level = "estimate"
)

location_data <- location_data[is_estimate != 0]

## Create Jobmon workflow, task templates, and tasks ---------------------------

# Create new workflow
wf_tool <- tool(name = "age_sex_model")

wf_tool <- set_default_tool_resources(
  tool = wf_tool,
  default_cluster_name = jobmon_cluster_name,
  resources = list("project" = submission_project_name)
)

# Create task templates
template_save_inputs <- task_template(
  tool = wf_tool,
  template_name = "save_inputs",
  command_template = paste(
    ""
  ),
  op_args = list("shell_path", "image_path", "script_path"),
  task_args = list("gbd_year"),
  node_args = list("version_id")
)

template_compile_hiv_free_ratios <- task_template(
  tool = wf_tool,
  template_name = "compile_hiv_free_ratios",
  command_template = paste(
    ""
  ),
  op_args = list("script_path", "conda_path", "conda_env", "code_path"),
  task_args = list(),
  node_args = list("version_id", "hiv_version")
)

template_fit_models <- task_template(
  tool = wf_tool,
  template_name = "fit_models",
  command_template = paste(
    ""
  ),
  op_args = list("shell_path", "image_path", "script_path"),
  task_args = list(),
  node_args = list("version_id", "version_5q0_id")
)


template_sex_stage_1 <- task_template(
  tool = wf_tool,
  template_name = "sex_stage_1",
  command_template = paste(
    ""
  ),
  op_args = list("shell_path", "image_path", "script_path"),
  task_args = list("gbd_year", "end_year"),
  node_args = list("version_id", "version_5q0_id")
)

template_sex_stage_2 <- task_template(
  tool = wf_tool,
  template_name = "sex_stage_2",
  command_template = paste(
    ""
  ),
  op_args = list("shell_path", "image_path", "script_path"),
  task_args = list("gbd_year", "end_year"),
  node_args = list("version_id", "version_5q0_id", "version_ddm_id", "code_dir")
)

template_sex_gpr <- task_template(
  tool = wf_tool,
  template_name = "sex_gpr",
  command_template = paste(
    ""
  ),
  op_args = list("script_path", "conda_path", "conda_env", "code_path"),
  task_args = list(),
  node_args = list("version_id", "ihme_loc_id", "code_dir")
)

template_age_model_1 <- task_template(
  tool = wf_tool,
  template_name = "age_model_1",
  command_template = paste(
    ""
  ),
  op_args = list("shell_path", "image_path", "script_path"),
  task_args = list("gbd_year", "end_year"),
  node_args = list("version_id", "sex", "version_5q0_id", "version_ddm_id", "code_dir")
)

template_age_model_2 <- task_template(
  tool = wf_tool,
  template_name = "age_model_2",
  command_template = paste(
    ""
  ),
  op_args = list("shell_path", "image_path", "script_path"),
  task_args = list("gbd_year", "end_year"),
  node_args = list("version_id", "sex", "age_group_name", "version_ddm_id", "code_dir")
)

template_age_model_format <- task_template(
  tool = wf_tool,
  template_name = "age_model_format",
  command_template = paste(
    ""
  ),
  op_args = list("shell_path", "image_path", "script_path"),
  task_args = list(),
  node_args = list("version_id")
)

template_age_gpr <- task_template(
  tool = wf_tool,
  template_name = "age_gpr",
  command_template = paste(
    ""
  ),
  op_args = list("script_path", "conda_path", "conda_env", "code_path"),
  task_args = list(),
  node_args = list("version_id", "ihme_loc_id", "code_dir")
)

template_scale <- task_template(
  tool = wf_tool,
  template_name = "scale",
  command_template = paste(
    ""
  ),
  op_args = list("shell_path", "image_path", "script_path"),
  task_args = list(),
  node_args = list("version_id", "version_5q0_id", "location_id", "ihme_loc_id")
)

template_graph <- task_template(
  tool = wf_tool,
  template_name = "graph",
  command_template = paste(
    ""
  ),
  op_args = list("shell_path", "image_path", "script_path"),
  task_args = list("gbd_year", "start_year"),
  node_args = list("version_id", "code_dir")
)

template_upload <- task_template(
  tool = wf_tool,
  template_name = "upload",
  command_template = paste(
    ""
  ),
  op_args = list("shell_path", "image_path", "script_path"),
  task_args = list(),
  node_args = list("version_id", "age_sex_data_version", "ddm_estimate_version",
                   "estimate_5q0_version", "mark_best")
)

# Create tasks and add to workflow
wf <- workflow(
  tool = wf_tool,
  workflow_args = paste0("age_sex_model_", workflow_args)
)

wf$workflow_attributes <- list()

task_save_inputs <- task(
  task_template = template_save_inputs,
  name = paste0("save_inputs_agesex_", version_id),
  max_attempts = 3,
  shell_path = shell_path_r,
  image_path = image_path,
  script_path = glue::glue("FILEPATH"),
  version_id = version_id,
  gbd_year = gbd_year,
  compute_resources = list(
    ""
  )
)

wf <- add_tasks(
  workflow = wf,
  tasks = list(task_save_inputs)
)

task_compile_hiv_free_ratios <- task(
  task_template = template_compile_hiv_free_ratios,
  name = paste0("compile_hiv_free_ratios_agesex_", version_id),
  upstream_tasks = list(task_save_inputs),
  max_attempts = 3,
  code_path = glue::glue("FILEPATH"),
  conda_path = conda_path,
  conda_env = conda_env,
  script_path = glue::glue("FILEPATH"),
  version_id = version_id,
  hiv_version = version_hiv_id,
  compute_resources = list(
    ""
  )
)

wf <- add_tasks(
  workflow = wf,
  tasks = list(task_compile_hiv_free_ratios)
)

task_fit_models <- task(
  task_template = template_fit_models,
  name = paste0("fit_models_agesex_", version_id),
  upstream_tasks = list(task_compile_hiv_free_ratios),
  max_attempts = 3,
  shell_path = shell_path_r,
  image_path = image_path,
  script_path = glue::glue("FILEPATH"),
  version_id = version_id,
  version_5q0_id = version_5q0_id,
  compute_resources = list(
    ""
  )
)

wf <- add_tasks(
  workflow = wf,
  tasks = list(task_fit_models)
)

task_sex_stage_1 <- task(
  task_template = template_sex_stage_1,
  name = paste0("sex_1_stage_agesex_", version_id),
  upstream_tasks = list(task_fit_models),
  max_attempts = 3,
  shell_path = shell_path_r,
  image_path = image_path,
  script_path = glue::glue("FILEPATH"),
  version_id = version_id,
  version_5q0_id = version_5q0_id,
  gbd_year = gbd_year,
  end_year = end_year,
  compute_resources = list(
    ""
  )
)

wf <- add_tasks(
  workflow = wf,
  tasks = list(task_sex_stage_1)
)

task_sex_stage_2 <- task(
  task_template = template_sex_stage_2,
  name = paste0("sex_2_stage_agesex_", version_id),
  upstream_tasks = list(task_sex_stage_1),
  max_attempts = 3,
  shell_path = shell_path_r,
  image_path = image_path,
  script_path = glue::glue("FILEPATH"),
  version_id = version_id,
  version_5q0_id = version_5q0_id,
  version_ddm_id = version_ddm_id,
  gbd_year = gbd_year,
  end_year = end_year,
  code_dir = code_dir,
  compute_resources = list(
    ""
  )
)

wf <- add_tasks(
  workflow = wf,
  tasks = list(task_sex_stage_2)
)

tasks_sex_gpr <- lapply(unique(location_data$ihme_loc_id), function(i) {

  task_sex_gpr <- task(
    task_template = template_sex_gpr,
    name = paste0("sex_gpr_agesex_", version_id),
    upstream_tasks = list(task_sex_stage_2),
    max_attempts = 3,
    code_path = glue::glue("FILEPATH"),
    conda_path = conda_path,
    conda_env = conda_env,
    script_path = glue::glue("FILEPATH"),
    version_id = version_id,
    ihme_loc_id = i,
    code_dir = code_dir,
    compute_resources = list(
      ""
    )
  )

  return(task_sex_gpr)

})

wf <- add_tasks(
  workflow = wf,
  tasks = tasks_sex_gpr
)

tasks_age_model_1 <- lapply(c("male", "female"), function(i) {

  task_age_model_1 <- task(
    task_template = template_age_model_1,
    name = paste0("age_1_model_agesex_", version_id),
    upstream_tasks = tasks_sex_gpr,
    max_attempts = 3,
    shell_path = shell_path_r,
    image_path = image_path,
    script_path = glue::glue("FILEPATH"),
    version_id = version_id,
    sex = i,
    version_5q0_id = version_5q0_id,
    version_ddm_id = version_ddm_id,
    gbd_year = gbd_year,
    end_year = end_year,
    code_dir = code_dir,
    compute_resources = list(
      ""
    )
  )

  return(task_age_model_1)

})

wf <- add_tasks(
  workflow = wf,
  tasks = tasks_age_model_1
)

tasks_age_model_2 <- lapply(1:nrow(ages_and_sexes), function(i) {

  task_age_model_2 <- task(
    task_template = template_age_model_2,
    name = paste0("age_2_model_agesex_", version_id),
    upstream_tasks = tasks_age_model_1,
    max_attempts = 3,
    shell_path = shell_path_r,
    image_path = image_path,
    script_path = glue::glue("FILEPATH"),
    version_id = version_id,
    sex = ages_and_sexes[i]$sex,
    age_group_name = ages_and_sexes[i]$age_group_name,
    version_ddm_id = version_ddm_id,
    gbd_year = gbd_year,
    end_year = end_year,
    code_dir = code_dir,
    compute_resources = list(
      ""
    )
  )

  return(task_age_model_2)

})

wf <- add_tasks(
  workflow = wf,
  tasks = tasks_age_model_2
)

task_age_model_format <- task(
  task_template = template_age_model_format,
  name = paste0("age_format_model_agesex_", version_id),
  upstream_tasks = tasks_age_model_2,
  max_attempts = 3,
  shell_path = shell_path_r,
  image_path = image_path,
  script_path = glue::glue("FILEPATH"),
  version_id = version_id,
  compute_resources = list(
    ""
  )
)

wf <- add_tasks(
  workflow = wf,
  tasks = list(task_age_model_format)
)

tasks_age_gpr <- lapply(unique(location_data$ihme_loc_id), function(i) {

  task_age_gpr <- task(
    task_template = template_age_gpr,
    name = paste0("age_gpr_agesex_", version_id),
    upstream_tasks = list(task_age_model_format),
    max_attempts = 3,
    code_path = glue::glue("FILEPATH"),
    conda_path = conda_path,
    conda_env = conda_env,
    script_path = glue::glue("FILEPATH"),
    version_id = version_id,
    ihme_loc_id = i,
    code_dir = code_dir,
    compute_resources = list(
      ""
    )
  )

  return(task_age_gpr)

})

wf <- add_tasks(
  workflow = wf,
  tasks = tasks_age_gpr
)

tasks_scale <- lapply(unique(location_data$ihme_loc_id), function(i) {

  task_scale <- task(
    task_template = template_scale,
    name = paste0("scale_agesex_", version_id),
    upstream_tasks = tasks_age_gpr,
    max_attempts = 3,
    shell_path = shell_path_r,
    image_path = image_path,
    script_path = glue::glue("FILEPATH"),
    version_id = version_id,
    version_5q0_id = version_5q0_id,
    location_id = location_data[ihme_loc_id == i]$location_id,
    ihme_loc_id = i,
    compute_resources = list(
      ""
    )
  )

  return(task_scale)

})

wf <- add_tasks(
  workflow = wf,
  tasks = tasks_scale
)

task_graph <- task(
  task_template = template_graph,
  name = paste0("graph_agesex_", version_id),
  upstream_tasks = tasks_scale,
  max_attempts = 3,
  shell_path = shell_path_r,
  image_path = run_all_image_path,
  script_path = glue::glue("FILEPATH"),
  version_id = version_id,
  gbd_year = gbd_year,
  code_dir = code_dir,
  start_year = start_year,
  compute_resources = list(
    ""
  )
)

wf <- add_tasks(
  workflow = wf,
  tasks = list(task_graph)
)


## Run workflow ----------------------------------------------------------------

wfr <- wf$run(resume = TRUE)

## Upload to database or report model failure ----------------------------------

if (wfr == "D") {

  print("Age-sex model completed!")

  # Update status
  mortdb::update_status(
    model_name = "age sex",
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
  stop("Age-sex model failed!")

}
