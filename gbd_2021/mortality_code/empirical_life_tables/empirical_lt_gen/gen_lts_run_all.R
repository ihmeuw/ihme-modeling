################################################################################
##                                                                            ##
## Empirical Life Tables (ELT) Run All                                        ##
##                                                                            ##
################################################################################

library(argparse)
library(data.table)
library(jobmonr)
library(mortdb, lib.loc ="FILEPATH")

## Read in arguments and inputs ------------------------------------------------

if (interactive()) {
  
  version_id <- "VERSION"
  username <- Sys.getenv("USER")
  mark_best <- F
  apply_outliers <- F
  code_dir <- "FILEPATH"
  workflow_args <- format(Sys.time(), "%Y-%m-%d-%H-%M")
  
} else {
  
  parser <- argparse::ArgumentParser()
  
  parser$add_argument(
    "--mark_best", type = "character", required = TRUE,
    action = "store", help = "True/False mark run as best"
  )
  parser$add_argument(
    "--gbd_year", type = "integer", required = TRUE,
    action = "store", help = "GBD Year"
  )
  parser$add_argument(
    "--username", type = "character", required = TRUE,
    action = "store", help = "User conducting the run"
  )
  parser$add_argument(
    "--version_id", type = "integer", required = TRUE,
    action = "store", help = "Version id of empirical lt run"
  )
  parser$add_argument(
    "--apply_outliers", type = "character", required = TRUE,
    action = "store", help = "True/False apply outliers"
  )
  parser$add_argument(
    "--code_dir", type = "character", required = TRUE,
    action = "store", help = "Directory where code is cloned"
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
  file = fs::path(code_dir, "/jobmon/mortality_production.yml"),
  use_parent = FALSE
)

list2env(config, .GlobalEnv)

# Additional variables
run_mv <- F
covid_years <- paste(2020:2021L, collapse = " ") # scripts expect and integer

# Update code_dir
code_dir <- fs::path(code_dir, "empirical_life_tables")

## Get location hierarchy ------------------------------------------------------

model_locations <- mortdb::get_locations(
  gbd_year = gbd_year,
  gbd_type = "ap_old",
  level = "estimate"
)

model_locations <- model_locations$ihme_loc_id

## Create Jobmon workflow, task templates, and tasks ---------------------------

# Create new workflow
wf_tool <- tool(name = "elt")

wf_tool <- set_default_tool_resources(
  tool = wf_tool,
  default_cluster_name = jobmon_cluster_name,
  resources = list("project" = submission_project_name)
)

wf <- workflow(
  tool = wf_tool,
  workflow_args = paste0("elt_", workflow_args)
)
wf$workflow_attributes <- list()

# Setup task
template_setup_task <- task_template(
  tool = wf_tool,
  template_name = "setup_task",
  command_template = paste(
    "PATH=/opt/singularity/bin/:$PATH OMP_NUM_THREADS=4",
    "{shell_path} -i {image_path} -s {script_path}",
    "--version_id {version_id}"
  ),
  op_args = list("shell_path", "image_path", "script_path"),
  task_args = list(),
  node_args = list("version_id")
)

task_setup_task <- task(
  task_template = template_setup_task,
  name = paste0("setup_task_", version_id),
  max_attempts = 3,
  shell_path = shell_path_r,
  image_path = lt_image_path,
  script_path = glue::glue("{code_dir}/empirical_lt_gen/00_elt_setup.R"),
  version_id = version_id,
  compute_resources = list(
    "memory" = "4G",
    "cores" = 2L,
    "queue" = queue,
    "constraints" = "archive",
    "runtime" = "1800S"
  )
)

tasks <- list(task_setup_task)

# Prep terminal ages

template_terminal_ages_task <- task_template(
  tool = wf_tool,
  template_name = "prep_terminal_ages_task",
  command_template = paste(
    "PATH=/opt/singularity/bin/:$PATH OMP_NUM_THREADS=4",
    "{shell_path} -i {image_path} -s {script_path}",
    "--version_id {version_id}",
    "--gbd_year {gbd_year}"
  ),
  op_args = list("shell_path", "image_path", "script_path"),
  task_args = list(),
  node_args = list("version_id", "gbd_year")
)

task_terminal_ages_task <- task(
  task_template = template_terminal_ages_task,
  name = paste0("prep_terminal_ages_task_", version_id),
  max_attempts = 3,
  shell_path = shell_path_r,
  upstream_tasks = list(task_setup_task),
  image_path = lt_image_path,
  script_path = glue::glue("{code_dir}/empirical_lt_gen/01a_prep_terminal_ages.R"),
  version_id = version_id,
  gbd_year = gbd_year,
  compute_resources = list(
    "memory" = "4G",
    "cores" = 2L,
    "queue" = queue,
    "constraints" = "archive",
    "runtime" = "600S"
  )
)
tasks <- c(tasks, task_terminal_ages_task)

# Prep VR task
template_prep_task <- task_template(
  tool = wf_tool,
  template_name = "prep_task",
  command_template = paste(
    "PATH=/opt/singularity/bin/:$PATH OMP_NUM_THREADS=8",
    "{shell_path} -i {image_path} -s {script_path}",
    "--version_id {version_id}",
    "--gbd_year {gbd_year}",
    "--covid_years {covid_years}",
    "--code_dir {code_dir}"
  ),
  op_args = list("shell_path", "image_path", "script_path"),
  task_args = list("version_id", "gbd_year", "covid_years", "code_dir"),
  node_args = list()
)

task_prep_task <- task(
  task_template = template_prep_task,
  name = paste0("prep_task_", version_id),
  upstream_tasks = list(task_terminal_ages_task),
  max_attempts = 3,
  shell_path = shell_path_r,
  image_path = lt_image_path,
  script_path = glue::glue("{code_dir}/empirical_lt_gen/01b_prep_vr.R"),
  version_id = version_id,
  gbd_year = gbd_year,
  covid_years = covid_years,
  code_dir = code_dir,
  compute_resources = list(
    "memory" = "8G",
    "cores" = 4L,
    "queue" = queue,
    "runtime" = "1800S",
    "constraints" = "archive"
  )
)
tasks <- c(tasks, task_prep_task)

# Gen ELT tasks, by location
template_gen_elt_task <- task_template(
  tool = wf_tool,
  template_name = "gen_elt_task",
  command_template = paste(
    "PATH=/opt/singularity/bin/:$PATH OMP_NUM_THREADS=8",
    "{shell_path} -i {image_path} -s {script_path}",
    "--version_id {version_id}",
    "--covid_years {covid_years}",
    "--code_dir {code_dir}",
    "--ihme_loc_id {ihme_loc_id}"
  ),
  op_args = list("shell_path", "image_path", "script_path"),
  task_args = list("version_id", "covid_years", "code_dir"),
  node_args = list("ihme_loc_id")
)

task_gen_elt_task <- lapply(model_locations, function(x) {
  
  task <- task(
    task_template = template_gen_elt_task,
    name = paste0("gen_elt_task_", version_id),
    upstream_tasks = list(task_prep_task),
    max_attempts = 3,
    shell_path = shell_path_r,
    image_path = lt_image_path,
    script_path = glue::glue("{code_dir}/empirical_lt_gen/02_gen_elts.R"),
    version_id = version_id,
    covid_years = covid_years,
    code_dir = code_dir,
    ihme_loc_id = x,
    compute_resources = list(
      "memory" = "20G",
      "cores" = 4L,
      "queue" = queue,
      "runtime" = "1800S"
    )
  )
  
  return(task)
  
})

tasks <- c(tasks, task_gen_elt_task)

# optional machine vision prediction tasks
if (run_mv) {
  
  template_mv_plots_task <- task_template(
    tool = wf_tool,
    template_name = "mv_plots_task",
    command_template = paste(
      "PATH=/opt/singularity/bin/:$PATH OMP_NUM_THREADS=8",
      "{shell_path} -i {image_path} -s {script_path}",
      "--version_id {version_id}",
      "--loc {loc}"
    ),
    op_args = list("shell_path", "image_path", "script_path"),
    node_args = list("loc"),
    task_args = list("version_id")
  )
  
  task_mv_plots_task <- lapply(model_locations, function(x) {
    
    task <- task(
      task_template = template_mv_plots_task,
      name = paste0("mv_plots_task_", x),
      upstream_tasks = task_gen_elt_task,
      max_attempts = 3,
      shell_path = shell_path_r,
      image_path = lt_image_path,
      script_path = glue::glue("{code_dir}/empirical_lt_gen/mv_input_plots_child.R"),
      loc = x,
      version_id = version_id,
      compute_resources = list(
        "memory" = "10G",
        "cores" = 4L,
        "queue" = queue,
        "constraints" = "archive",
        "runtime" = "10000S"
      )
    )
    
    return(task)
    
  })
  
  tasks <- c(tasks, task_mv_plots_task)
  
  template_run_mv_task <- task_template(
    tool = wf_tool,
    template_name = "run_mv_task",
    command_template = paste(
      "PYTHONPATH= PATH=/opt/singularity/bin/:$PATH OMP_NUM_THREADS=4",
      "{script_path} {code_path} {conda_path} {conda_env}",
      "{version_id} {ihme_loc_id}"
    ),
    op_args = list("script_path", "code_path", "conda_path", "conda_env"),
    node_args = list("ihme_loc_id"),
    task_args = list("version_id")
  )
  
  task_run_mv_task <- lapply(model_locations, function(x) {
    
    task <- task(
      task_template = template_run_mv_task,
      name = paste0("run_mv_task_", x),
      upstream_tasks = task_mv_plots_task,
      max_attempts = 3,
      script_path = glue::glue("{code_dir}/empirical_lt_gen/predict_machine_vision.sh"),
      code_path = glue::glue("{code_dir}/empirical_lt_gen/predict_machine_vision.py"),
      conda_path = conda_path,
      conda_env = conda_env,
      ihme_loc_id = x,
      version_id = version_id,
      compute_resources = list(
        "memory" = "10G",
        "cores" = 4L,
        "queue" = queue,
        "runtime" = "10000S"
      )
    )
    
    return(task)
    
  })
  
  tasks <- c(tasks, task_run_mv_task)
  
  select_lts_task_upstream <- task_run_mv_task
  
} else {
  
  select_lts_task_upstream <- task_gen_elt_task
  
}

template_select_lts_task <- task_template(
  tool = wf_tool,
  template_name = "select_lts_task",
  command_template = paste(
    "PATH=/opt/singularity/bin/:$PATH OMP_NUM_THREADS=8",
    "{shell_path} -i {image_path} -s {script_path}",
    "--version_id {version_id}",
    "--mark_best {mark_best}",
    "--run_mv {run_mv}",
    "--gbd_year {gbd_year}",
    "--covid_years {covid_years}",
    "--code_dir {code_dir}"
  ),
  op_args = list("shell_path", "image_path", "script_path"),
  task_args = list("covid_years"),
  node_args = list("version_id", "gbd_year", "code_dir", "run_mv", "mark_best")
)

task_select_lts_task <- task(
  task_template = template_select_lts_task,
  name = "select_lts_task",
  upstream_tasks = select_lts_task_upstream,
  max_attempts = 3,
  shell_path = shell_path_r,
  image_path = lt_image_path,
  script_path = glue::glue("{code_dir}/empirical_lt_gen/03_select_lts.R"),
  version_id = version_id,
  gbd_year = gbd_year,
  covid_years = covid_years,
  code_dir = code_dir,
  run_mv = run_mv,
  mark_best = mark_best,
  compute_resources = list(
    "memory" = "50G",
    "cores" = 4L,
    "queue" = queue,
    "runtime" = "1800S"
  )
)

tasks <- c(tasks, task_select_lts_task)

# Run vetting comparison diagnostics on uploaded data
template_lt_diagnostics_task <- task_template(
  tool = wf_tool,
  template_name = "lt_diagnostics_task",
  command_template = paste(
    "PATH=/opt/singularity/bin/:$PATH OMP_NUM_THREADS=8",
    "{shell_path} -i {image_path} -s {script_path}",
    "--version_id {version_id}",
    "--gbd_year {gbd_year}",
    "--code_dir {code_dir}"
  ),
  op_args = list("shell_path", "image_path", "script_path"),
  task_args = list(),
  node_args = list("version_id", "gbd_year", "code_dir")
)

task_lt_diagnostics_task <- task(
  task_template = template_lt_diagnostics_task,
  name = "lt_diagnostics_task",
  upstream_tasks = list(task_select_lts_task),
  max_attempts = 3,
  shell_path = shell_path_r,
  image_path = lt_image_path,
  script_path = glue::glue("{code_dir}/empirical_lt_gen/04_lt_diagnostics.R"),
  version_id = version_id,
  gbd_year = gbd_year,
  code_dir = code_dir,
  compute_resources = list(
    "memory" = "12G",
    "cores" = 4L,
    "queue" = queue,
    "runtime" = "1800S"
  )
)

tasks <- c(tasks, task_lt_diagnostics_task)

wf <- jobmonr::add_tasks(
  workflow = wf,
  tasks = tasks
)

## Run workflow ----------------------------------------------------------------

wfr <- wf$run(resume = TRUE)

## Report model success or failure ---------------------------------------------

if (wfr == "D") {
  
  print("Empirical life table generation succeeded!")
  
} else {
  
  mortdb::send_slack_message(
    message = paste0("Empirical life tables ", version_id, " failed."),
    channel = paste0("@", username),
    icon = ":thinking_face:",
    botname = "ELT_Bot"
  )
  
  # Stop process
  stop("Empirical life table generation failed!")
  
}
