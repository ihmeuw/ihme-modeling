
## Meta ------------------------------------------------------------------------

# Description: Submit Jobmon workflow to compile handoffs

# Steps:
#   1. Initial setup
#   2. Create and run Jobmon workflow
#   3. Check for outputs

# Load libraries ---------------------------------------------------------------

library(argparse)
library(data.table)
library(jobmonr)

# Setup ------------------------------------------------------------------------

parser <- argparse::ArgumentParser()
parser$add_argument(
  "--main_dir",
  type = "character",
  required = !interactive(),
  help = "base directory for this version run"
)
args <- parser$parse_args()
list2env(args, .GlobalEnv)

# Define the main directory here
if (interactive()) {
  main_dir <- "INSERT_PATH_HERE"
  if (!fs::dir_exists(main_dir)) stop("specify valid 'main_dir'")
}

# get config
config <- config::get(
  file = fs::path("FILEPATH"),
  use_parent = FALSE
)
list2env(config$default, .GlobalEnv)

# update code_dir
if (interactive()) {
  code_dir <- paste0("FILEPATH")
}

# Create and run Jobmon workflow -----------------------------------------------

# create new workflow
wf_tool_mort_data <- jobmonr::tool(name = "gbd_mortality_data_processing")

jobmonr::set_default_tool_resources(
  tool = wf_tool_mort_data,
  default_cluster_name = jobmon_cluster_name,
  resources = list(
    "constraints" = "archive",
    "project" = submission_project_name
  )
)

wf_mort_data <- jobmonr::workflow(
  tool = wf_tool_mort_data,
  workflow_args = format(Sys.time(), "%Y-%m-%d-%H-%M")
)

# create task templates
template_mort_data <- jobmonr::task_template(
  tool = wf_tool_mort_data,
  template_name = "template_mort_data",
  command_template = paste(""),
  node_args = list("script_path"),
  op_args = list("shell_path", "image_path"),
  task_args = list("main_dir")
)

# create compute_resources objects
compute_resources <- list("")

compute_resources_diagnostics <- list("")

# create tasks
if (raw_data_processing) {

  task_process_pop <- list(jobmonr::task(
    task_template = template_mort_data,
    name = "process_pop",
    upstream_tasks = list(),
    max_attempts = jobmon_max_attempts,
    shell_path = image_shell_path_r,
    image_path = image_path,
    script_path = fs::path("FILEPATH"),
    main_dir = main_dir,
    compute_resources = compute_resources
  ))

  wf_mort_data <- jobmonr::add_tasks(
    workflow = wf_mort_data,
    tasks = task_process_pop
  )

  task_process_cod_noncod <- list(jobmonr::task(
    task_template = template_mort_data,
    name = "process_cod_noncod",
    upstream_tasks = list(),
    max_attempts = jobmon_max_attempts,
    shell_path = image_shell_path_r,
    image_path = image_path,
    script_path = fs::path("FILEPATH"),
    main_dir = main_dir,
    compute_resources = compute_resources
  ))

  wf_mort_data <- jobmonr::add_tasks(
    workflow = wf_mort_data,
    tasks = task_process_cod_noncod
  )

  task_generate_VR <- list(jobmonr::task(
    task_template = template_mort_data,
    name = "generate_VR",
    upstream_tasks = c(task_process_pop, task_process_cod_noncod),
    max_attempts = jobmon_max_attempts,
    shell_path = image_shell_path_r,
    image_path = image_path,
    script_path = fs::path("FILEPATH"),
    main_dir = main_dir,
    compute_resources = compute_resources
  ))

  wf_mort_data <- jobmonr::add_tasks(
    workflow = wf_mort_data,
    tasks = task_generate_VR
  )

  task_generate_census_survey_gbd_ages <- list(jobmonr::task(
    task_template = template_mort_data,
    name = "generate_census_survey_gbd_ages",
    upstream_tasks = c(task_process_pop, task_process_cod_noncod),
    max_attempts = jobmon_max_attempts,
    shell_path = image_shell_path_r,
    image_path = image_path,
    script_path = fs::path("FILEPATH"),
    main_dir = main_dir,
    compute_resources = compute_resources
  ))

  wf_mort_data <- jobmonr::add_tasks(
    workflow = wf_mort_data,
    tasks = task_generate_census_survey_gbd_ages
  )

  task_generate_census_survey_nonstandard_ages <- list(jobmonr::task(
    task_template = template_mort_data,
    name = "generate_census_survey_nonstandard_ages",
    upstream_tasks = c(task_process_pop, task_process_cod_noncod),
    max_attempts = jobmon_max_attempts,
    shell_path = image_shell_path_r,
    image_path = image_path,
    script_path = fs::path("FILEPATH"),
    main_dir = main_dir,
    compute_resources = compute_resources
  ))

  wf_mort_data <- jobmonr::add_tasks(
    workflow = wf_mort_data,
    tasks = task_generate_census_survey_nonstandard_ages
  )

  task_generate_CBH_gbd_ages <- list(jobmonr::task(
    task_template = template_mort_data,
    name = "generate_CBH_gbd_ages",
    upstream_tasks = list(),
    max_attempts = jobmon_max_attempts,
    shell_path = image_shell_path_r,
    image_path = image_path,
    script_path = fs::path("FILEPATH"),
    main_dir = main_dir,
    compute_resources = compute_resources
  ))

  wf_mort_data <- jobmonr::add_tasks(
    workflow = wf_mort_data,
    tasks = task_generate_CBH_gbd_ages
  )

  task_generate_CBH_nonstandard_ages <- list(jobmonr::task(
    task_template = template_mort_data,
    name = "generate_CBH_nonstandard_ages",
    upstream_tasks = c(task_generate_CBH_gbd_ages, task_generate_census_survey_gbd_ages),
    max_attempts = jobmon_max_attempts,
    shell_path = image_shell_path_r,
    image_path = image_path,
    script_path = fs::path("FILEPATH"),
    main_dir = main_dir,
    compute_resources = compute_resources
  ))

  wf_mort_data <- jobmonr::add_tasks(
    workflow = wf_mort_data,
    tasks = task_generate_CBH_nonstandard_ages
  )

  task_generate_SBH_nonstandard_ages <- list(jobmonr::task(
    task_template = template_mort_data,
    name = "generate_SBH_nonstandard_ages",
    upstream_tasks = list(),
    max_attempts = jobmon_max_attempts,
    shell_path = image_shell_path_r,
    image_path = image_path,
    script_path = fs::path("FILEPATH"),
    main_dir = main_dir,
    compute_resources = compute_resources
  ))

  wf_mort_data <- jobmonr::add_tasks(
    workflow = wf_mort_data,
    tasks = task_generate_SBH_nonstandard_ages
  )

  task_generate_SIBS_gbd_ages <- list(jobmonr::task(
    task_template = template_mort_data,
    name = "generate_SIBS_gbd_ages",
    upstream_tasks = list(),
    max_attempts = jobmon_max_attempts,
    shell_path = image_shell_path_r,
    image_path = image_path,
    script_path = fs::path("FILEPATH"),
    main_dir = main_dir,
    compute_resources = compute_resources
  ))

  wf_mort_data <- jobmonr::add_tasks(
    workflow = wf_mort_data,
    tasks = task_generate_SIBS_gbd_ages
  )

  task_generate_SIBS_nonstandard_ages <- list(jobmonr::task(
    task_template = template_mort_data,
    name = "generate_SIBS_nonstandard_ages",
    upstream_tasks = list(),
    max_attempts = jobmon_max_attempts,
    shell_path = image_shell_path_r,
    image_path = image_path,
    script_path = fs::path("FILEPATH"),
    main_dir = main_dir,
    compute_resources = compute_resources
  ))

  wf_mort_data <- jobmonr::add_tasks(
    workflow = wf_mort_data,
    tasks = task_generate_SIBS_nonstandard_ages
  )

  task_compile_gbd_ages <- list(jobmonr::task(
    task_template = template_mort_data,
    name = "compile_gbd_ages_data",
    upstream_tasks = c(
      task_generate_VR, task_generate_census_survey_gbd_ages,
      task_generate_CBH_gbd_ages, task_generate_SIBS_gbd_ages
    ),
    max_attempts = jobmon_max_attempts,
    shell_path = image_shell_path_r,
    image_path = image_path,
    script_path = fs::path("FILEPATH"),
    main_dir = main_dir,
    compute_resources = compute_resources
  ))

  wf_mort_data <- jobmonr::add_tasks(
    workflow = wf_mort_data,
    tasks = task_compile_gbd_ages
  )

  task_compile_survey_nonstandard_ages <- list(jobmonr::task(
    task_template = template_mort_data,
    name = "compile_nonstandard_ages_data",
    upstream_tasks = c(
      task_generate_census_survey_nonstandard_ages, task_generate_CBH_nonstandard_ages,
      task_generate_SBH_nonstandard_ages, task_generate_SIBS_nonstandard_ages
    ),
    max_attempts = jobmon_max_attempts,
    shell_path = image_shell_path_r,
    image_path = image_path,
    script_path = fs::path("FILEPATH"),
    main_dir = main_dir,
    compute_resources = compute_resources
  ))

  wf_mort_data <- jobmonr::add_tasks(
    workflow = wf_mort_data,
    tasks = task_compile_survey_nonstandard_ages
  )

  task_VR_diagnostics <- list(jobmonr::task(
    task_template = template_mort_data,
    name = "generate_VR_diagnostics",
    upstream_tasks = c(task_compile_gbd_ages, task_compile_survey_nonstandard_ages),
    max_attempts = jobmon_max_attempts,
    shell_path = image_shell_path_r,
    image_path = image_path,
    script_path = fs::path("FILEPATH"),
    main_dir = main_dir,
    compute_resources = compute_resources_diagnostics
  ))

  wf_mort_data <- jobmonr::add_tasks(
    workflow = wf_mort_data,
    tasks = task_VR_diagnostics
  )

  task_CBH_SIBS_diagnostics <- list(jobmonr::task(
    task_template = template_mort_data,
    name = "generate_CBH_SIBS_diagnostics",
    upstream_tasks = c(task_compile_gbd_ages, task_compile_survey_nonstandard_ages),
    max_attempts = jobmon_max_attempts,
    shell_path = image_shell_path_r,
    image_path = image_path,
    script_path = fs::path("FILEPATH"),
    main_dir = main_dir,
    compute_resources = compute_resources_diagnostics
  ))

  wf_mort_data <- jobmonr::add_tasks(
    workflow = wf_mort_data,
    tasks = task_CBH_SIBS_diagnostics
  )

}

if (age_sex_splitting) {

  # VR
  if (raw_data_processing) {
    upstream_08a <- c(task_generate_VR, task_compile_gbd_ages)
  } else {
    upstream_08a <- list()
  }

  task_run_VR_splitting_prep_and_pattern_selection <- list(jobmonr::task(
    task_template = template_mort_data,
    name = "run_VR_splitting_prep_and_pattern_selection",
    upstream_tasks = upstream_08a,
    max_attempts = jobmon_max_attempts,
    shell_path = image_shell_path_r,
    image_path = image_path,
    script_path = fs::path("FILEPATH"),
    main_dir = main_dir,
    compute_resources = compute_resources
  ))

  wf_mort_data <- jobmonr::add_tasks(
    workflow = wf_mort_data,
    tasks = task_run_VR_splitting_prep_and_pattern_selection
  )

  task_run_VR_sex_splitting <- list(jobmonr::task(
    task_template = template_mort_data,
    name = "run_VR_sex_splitting",
    upstream_tasks = c(task_run_VR_splitting_prep_and_pattern_selection),
    max_attempts = jobmon_max_attempts,
    shell_path = image_shell_path_r,
    image_path = image_path,
    script_path = fs::path("FILEPATH"),
    main_dir = main_dir,
    compute_resources = compute_resources
  ))

  wf_mort_data <- jobmonr::add_tasks(
    workflow = wf_mort_data,
    tasks = task_run_VR_sex_splitting
  )

  task_run_VR_age_splitting <- list(jobmonr::task(
    task_template = template_mort_data,
    name = "run_VR_age_splitting",
    upstream_tasks = c(task_run_VR_sex_splitting),
    max_attempts = jobmon_max_attempts,
    shell_path = image_shell_path_r,
    image_path = image_path,
    script_path = fs::path("FILEPATH"),
    main_dir = main_dir,
    compute_resources = compute_resources
  ))

  wf_mort_data <- jobmonr::add_tasks(
    workflow = wf_mort_data,
    tasks = task_run_VR_age_splitting
  )

  task_run_VR_compile_split_data <- list(jobmonr::task(
    task_template = template_mort_data,
    name = "run_VR_compile_split_data",
    upstream_tasks = c(task_run_VR_age_splitting),
    max_attempts = jobmon_max_attempts,
    shell_path = image_shell_path_r,
    image_path = image_path,
    script_path = fs::path("FILEPATH"),
    main_dir = main_dir,
    compute_resources = compute_resources
  ))

  wf_mort_data <- jobmonr::add_tasks(
    workflow = wf_mort_data,
    tasks = task_run_VR_compile_split_data
  )

  # survey
  if (raw_data_processing) {
    upstream_09a <- c(task_compile_survey_nonstandard_ages, task_compile_gbd_ages)
  } else {
    upstream_09a <- list()
  }

  task_run_survey_splitting_prep_and_pattern_selection <- list(jobmonr::task(
    task_template = template_mort_data,
    name = "run_survey_splitting_prep_and_pattern_selection",
    upstream_tasks = upstream_09a,
    max_attempts = jobmon_max_attempts,
    shell_path = image_shell_path_r,
    image_path = image_path,
    script_path = fs::path("FILEPATH"),
    main_dir = main_dir,
    compute_resources = compute_resources
  ))

  wf_mort_data <- jobmonr::add_tasks(
    workflow = wf_mort_data,
    tasks = task_run_survey_splitting_prep_and_pattern_selection
  )

  task_run_survey_sex_splitting <- list(jobmonr::task(
    task_template = template_mort_data,
    name = "run_survey_sex_splitting",
    upstream_tasks = c(task_run_survey_splitting_prep_and_pattern_selection),
    max_attempts = jobmon_max_attempts,
    shell_path = image_shell_path_r,
    image_path = image_path,
    script_path = fs::path("FILEPATH"),
    main_dir = main_dir,
    compute_resources = compute_resources
  ))

  wf_mort_data <- jobmonr::add_tasks(
    workflow = wf_mort_data,
    tasks = task_run_survey_sex_splitting
  )

  task_run_survey_age_splitting <- list(jobmonr::task(
    task_template = template_mort_data,
    name = "run_survey_age_splitting",
    upstream_tasks = c(task_run_survey_sex_splitting),
    max_attempts = jobmon_max_attempts,
    shell_path = image_shell_path_r,
    image_path = image_path,
    script_path = fs::path("FILEPATH"),
    main_dir = main_dir,
    compute_resources = compute_resources
  ))

  wf_mort_data <- jobmonr::add_tasks(
    workflow = wf_mort_data,
    tasks = task_run_survey_age_splitting
  )

}

if (dataset_generation_for_modeling) {

  if (age_sex_splitting) {
    upstream_10a <- c(task_run_VR_compile_split_data)
  } else {
    upstream_10a <- list()
  }

  task_compile_final_VR <- list(jobmonr::task(
    task_template = template_mort_data,
    name = "compile_final_VR",
    upstream_tasks = upstream_10a,
    max_attempts = jobmon_max_attempts,
    shell_path = image_shell_path_r,
    image_path = image_path,
    script_path = fs::path("FILEPATH"),
    main_dir = main_dir,
    compute_resources = compute_resources
  ))

  wf_mort_data <- jobmonr::add_tasks(
    workflow = wf_mort_data,
    tasks = task_compile_final_VR
  )

  if (age_sex_splitting) {
    upstream_10b <- c(task_run_survey_age_splitting)
  } else {
    upstream_10b <- list()
  }

  task_compile_final_survey <- list(jobmonr::task(
    task_template = template_mort_data,
    name = "compile_final_survey",
    upstream_tasks = upstream_10b,
    max_attempts = jobmon_max_attempts,
    shell_path = image_shell_path_r,
    image_path = image_path,
    script_path = fs::path("FILEPATH"),
    main_dir = main_dir,
    compute_resources = compute_resources
  ))

  wf_mort_data <- jobmonr::add_tasks(
    workflow = wf_mort_data,
    tasks = task_compile_final_survey
  )

  task_compile_final_dataset <- list(jobmonr::task(
    task_template = template_mort_data,
    name = "compile_final_dataset",
    upstream_tasks = c(task_compile_final_VR, task_compile_final_survey),
    max_attempts = jobmon_max_attempts,
    shell_path = image_shell_path_r,
    image_path = image_path,
    script_path = fs::path("FILEPATH"),
    main_dir = main_dir,
    compute_resources = compute_resources
  ))

  wf_mort_data <- jobmonr::add_tasks(
    workflow = wf_mort_data,
    tasks = task_compile_final_dataset
  )

  task_VR_diagnostics_final <- list(jobmonr::task(
    task_template = template_mort_data,
    name = "generate_VR_diagnostics_final",
    upstream_tasks = c(task_compile_final_dataset),
    max_attempts = jobmon_max_attempts,
    shell_path = image_shell_path_r,
    image_path = image_path,
    script_path = fs::path("FILEPATH"),
    main_dir = main_dir,
    compute_resources = compute_resources_diagnostics
  ))

  wf_mort_data <- jobmonr::add_tasks(
    workflow = wf_mort_data,
    tasks = task_VR_diagnostics_final
  )

  task_survey_diagnostics_final <- list(jobmonr::task(
    task_template = template_mort_data,
    name = "generate_survey_diagnostics_final",
    upstream_tasks = c(task_compile_final_dataset),
    max_attempts = jobmon_max_attempts,
    shell_path = image_shell_path_r,
    image_path = image_path,
    script_path = fs::path("FILEPATH"),
    main_dir = main_dir,
    compute_resources = compute_resources_diagnostics
  ))

  wf_mort_data <- jobmonr::add_tasks(
    workflow = wf_mort_data,
    tasks = task_survey_diagnostics_final
  )

  task_other_diagnostics_final <- list(jobmonr::task(
    task_template = template_mort_data,
    name = "generate_other_diagnostics_final",
    upstream_tasks = c(task_compile_final_dataset),
    max_attempts = jobmon_max_attempts,
    shell_path = image_shell_path_r,
    image_path = image_path,
    script_path = fs::path("FILEPATH"),
    main_dir = main_dir,
    compute_resources = compute_resources_diagnostics
  ))

  wf_mort_data <- jobmonr::add_tasks(
    workflow = wf_mort_data,
    tasks = task_other_diagnostics_final
  )

}

wfr_mort_data <- wf_mort_data$run(resume = TRUE)

# Check for outputs ------------------------------------------------------------

# check for handoff files
if (raw_data_processing) {

  assertable::check_files(
    filenames = c("FILEPATH"),
    folder = fs::path("FILEPATH"),
    continual = TRUE,
    sleep_time = 60, # seconds
    sleep_end = 60 # minutes
  )

}

if (dataset_generation_for_modeling) {

  assertable::check_files(
    filenames = c("FILEPATH"),
    folder = fs::path("FILEPATH"),
    continual = TRUE,
    sleep_time = 60, # seconds
    sleep_end = 60 # minutes
  )

  # send slack message
  message <- paste0(
    "Input files to the mortality model are ready at: ", main_dir
  )
  mortdb::send_slack_message(
    channel = "",
    message = message,
    icon = "",
    botname = ""
  )

}
