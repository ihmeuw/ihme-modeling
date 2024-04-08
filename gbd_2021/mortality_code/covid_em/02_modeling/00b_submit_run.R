
# Meta --------------------------------------------------------------------

# Submit the covid excess mortality process jobs using Jobmon

# Steps:
#  * Build jobmon workflow object with correct tasks
#  * Submit workflow
#  * Slack upon completion
# Inputs:
#  * {main_dir}/covid_em_detailed.yml: detailed configuration file
#  * {main_dir}/process_locations.csv: locations to be submitted for parallel steps
#  * {main_dir}/process_sexes.csv: sexes to be submitted for parallel steps


# Load packages -----------------------------------------------------------

library(argparse)
library(data.table)
library(jobmonr)


# Read inputs -------------------------------------------------------------

parser <- argparse::ArgumentParser()

parser$add_argument(
  "--main_dir",
  type = "character",
  required = !interactive(),
  default = "FILEPATH",
  help = "main versioned directory for this model run"
)

args <- parser$parse_args()
main_dir <- args$main_dir

config <- config::get(
  config = "default",
  file = fs::path(main_dir, "covid_em_detailed.yml"),
  use_parent = FALSE
)

list2env(config, .GlobalEnv)


# Load mappings -----------------------------------------------------------

loc_table <- fread(fs::path(main_dir, "inputs", "process_locations.csv"))
loc_table <- unique(loc_table[(is_estimate_1)], by = "location_id")

sex_table <- fread(fs::path(main_dir, "inputs", "process_sexes.csv"))
sex_table <- unique(sex_table[(is_estimate)], by = "sex")


# Create workflow ---------------------------------------------------------

# Steps:
# 1) Define tool
# 2) Define task templates
# 3) Instantiate workflow
# 4) Add workflow tasks


## Define tool ------------------------------------------------------------

wf_tool <- jobmonr::tool(name = "covid_em_estimation")

wf_tool <- jobmonr::set_default_tool_resources(
  tool = wf_tool,
  default_cluster_name = jobmon_cluster_name,
  resources = list(
    "project" = submission_project_name,
    "queue" = queue
  )
)


## Define task templates --------------------------------------------------

templates <- list()

templates$download_inputs_model <- jobmonr::task_template(
  tool = wf_tool,
  template_name = "download_model_inputs",
  command_template = paste(
    "{shell_path} -i {image_path} -s {script_path}",
    "--main_dir {main_dir}"
  ),
  task_args = list("main_dir"),
  op_args = list("shell_path", "image_path", "script_path")
)

templates$download_inputs_other <- jobmonr::task_template(
  tool = wf_tool,
  template_name = "download_other_inputs",
  command_template = paste(
    "{shell_path} -i {image_path} -s {script_path}",
    "--main_dir {main_dir}"
  ),
  task_args = list("main_dir"),
  op_args = list("shell_path", "image_path", "script_path")
)

templates$prep_regmod <- jobmonr::task_template(
  tool = wf_tool,
  template_name = "prep_regmod_metadata",
  command_template = paste(
    "{shell_path} -i {image_path} -s {script_path}",
    "--main_dir {main_dir}",
    "--ts {ts}"
  ),
  node_args = list("ts"),
  task_args = list("main_dir"),
  op_args = list("shell_path", "image_path", "script_path")
)

templates$model_poisson <- jobmonr::task_template(
  tool = wf_tool,
  template_name = "model_poisson",
  command_template = paste(
    "{shell_path} -i {image_path} -s {script_path}",
    "--main_dir {main_dir}",
    "--loc_id {loc_id}"
  ),
  node_args = list("loc_id"),
  task_args = list("main_dir"),
  op_args = list("shell_path", "image_path", "script_path")
)

templates$model_regmod <- jobmonr::task_template(
  tool = wf_tool,
  template_name = "model_regmod",
  command_template = paste(
    "{shell_path} {script_path}",
    "--main_dir {main_dir}",
    "--ihme_loc {ihme_loc_id}",
    "--ts {ts}"
  ),
  node_args = list("ihme_loc_id", "ts"),
  task_args = list("main_dir"),
  op_args = list("shell_path", "script_path")
)

templates$complete_models <- jobmonr::task_template(
  tool = wf_tool,
  template_name = "complete_models",
  command_template = paste(
    "{shell_path} -i {image_path} -s {script_path}",
    "--main_dir {main_dir}",
    "--loc_id {loc_id}"
  ),
  node_args = list("loc_id"),
  task_args = list("main_dir"),
  op_args = list("shell_path", "image_path", "script_path")
)

templates$diagnostics_summary <- jobmonr::task_template(
  tool = wf_tool,
  template_name = "summary_diagnostics",
  command_template = paste(
    "{shell_path} -i {image_path} -s {script_path}",
    "--main_dir {main_dir}"
  ),
  task_args = list("main_dir"),
  op_args = list("shell_path", "image_path", "script_path")
)

templates$diagnostics_loc_specific <- jobmonr::task_template(
  tool = wf_tool,
  template_name = "location_specific_diagnostics",
  command_template = paste(
    "{shell_path} -i {image_path} -s {script_path}",
    "--main_dir {main_dir}",
    "--loc_id {loc_id}"
  ),
  node_args = list("loc_id"),
  task_args = list("main_dir"),
  op_args = list("shell_path", "image_path", "script_path")
)

templates$diagnostics_compile <- jobmonr::task_template(
  tool = wf_tool,
  template_name = "compile_diagnostics",
  command_template = paste(
    "{shell_path} -i {image_path} -s {script_path}",
    "--main_dir {main_dir}"
  ),
  task_args = list("main_dir"),
  op_args = list("shell_path", "image_path", "script_path")
)


## Instantiate workflow ---------------------------------------------------

wf <- jobmonr::workflow(
  tool = wf_tool,
  workflow_args = run_id_covid_em_estimate,
  name = "covid_em-model-stage_1"
)


## Add workflow tasks -----------------------------------------------------

task_inputs_model <- jobmonr::task(
  task_template = templates$download_inputs_model,
  name = paste0("download_model_inputs-", run_id_covid_em_estimate),
  max_attempts = jobmon_max_attempts,
  compute_resources = list(
    "memory" = "20G",
    "cores" = 2L,
    "runtime" = "3H"
  ),
  shell_path = image_shell_path_r,
  image_path = image_path,
  script_path = fs::path(code_dir, "01a_download_model_inputs.R"),
  main_dir = main_dir

)

task_inputs_other <- jobmonr::task(
  task_template = templates$download_inputs_other,
  name = paste0("download_other_inputs-", run_id_covid_em_estimate),
  max_attempts = jobmon_max_attempts,
  compute_resources = list(
    "memory" = "20G",
    "cores" = 2L,
    "runtime" = "3H"
  ),
  shell_path = image_shell_path_r,
  image_path = image_path,
  script_path = fs::path(code_dir, "01b_download_other_inputs.R"),
  main_dir = main_dir

)

wf <- jobmonr::add_tasks(wf, list(task_inputs_model, task_inputs_other))

if (isTRUE("poisson" %in% run_model_types)) {

  task_model_poisson <- jobmonr::array_tasks(
    task_template = templates$model_poisson,
    name = paste0("fit_poisson-", run_id_covid_em_estimate),
    upstream_tasks = list(task_inputs_model),
    max_attempts = jobmon_max_attempts,
    compute_resources = list(
      "memory" = "120G",
      "cores" = 3L,
      "runtime" = "3H"
    ),
    shell_path = image_shell_path_r,
    image_path = image_path,
    script_path = fs::path(code_dir, "02_fit_model.R"),
    main_dir = main_dir,
    loc_id = as.list(loc_table$location_id)
  )

  wf <- jobmonr::add_tasks(wf, task_model_poisson)

}

if (isTRUE("regmod" %in% run_model_types)) {

  tasks_regmod <- lapply(tail_size_month, function(tail_size) {

    tail_size <- as.integer(tail_size)

    task_prep_regmod <- jobmonr::task(
      task_template = templates$prep_regmod,
      name = glue::glue("prep_meta-ts_{tail_size}-{run_id_covid_em_estimate}"),
      upstream_tasks = list(task_inputs_model),
      max_attempts = jobmon_max_attempts,
      compute_resources = list(
        "memory" = "10G",
        "cores" = 2L,
        "runtime" = "3H"
      ),
      shell_path = image_shell_path_r,
      image_path = image_path,
      script_path = fs::path(code_dir, "01c_prep_meta.R"),
      main_dir = main_dir,
      ts = tail_size
    )

    task_model_regmod <- lapply(loc_table$ihme_loc_id, function(ihme_loc) {

      jobmonr::task(
        task_template = templates$model_regmod,
        name = glue::glue("fit_regmod-ts_{tail_size}-{ihme_loc}-{run_id_covid_em_estimate}"),
        upstream_tasks = list(task_prep_regmod),
        max_attempts = jobmon_max_attempts,
        compute_resources = list(
          "memory" = "30G",
          "cores" = 2L,
          "runtime" = "3H"
        ),
        shell_path = fs::path(code_dir, "regmod_shell.sh"),
        script_path = fs::path(code_dir, "02b_fit_regmod_model.py"),
        main_dir = main_dir,
        ihme_loc_id = ihme_loc,
        ts = tail_size
      )

    })

    list(prep = task_prep_regmod, model = task_model_regmod)

  })

  tasks_regmod <- purrr::transpose(tasks_regmod)
  wf <- jobmonr::add_tasks(wf, tasks_regmod$prep)
  wf <- jobmonr::add_tasks(wf, purrr::flatten(tasks_regmod$model))

}


tasks_complete_models <- lapply(loc_table$location_id, function(loc_id) {

  ihme_loc_id <- loc_table[location_id == loc_id, ihme_loc_id]

  upstream_tasks <- list()

  if (isTRUE("poisson" %in% run_model_types)) {
    upstream_tasks <- c(
      upstream_tasks,
      jobmonr::get_tasks_by_node_args(wf, "model_poisson", loc_id = loc_id)
    )
  }

  if (isTRUE("regmod" %in% run_model_types)) {
    upstream_tasks <- c(
      upstream_tasks,
      jobmonr::get_tasks_by_node_args(wf, "model_regmod", ihme_loc_id = ihme_loc_id)
    )
  }

  jobmonr::task(
    task_template = templates$complete_models,
    name = glue::glue("complete_stage1-{ihme_loc_id}-{run_id_covid_em_estimate}"),
    upstream_tasks = upstream_tasks,
    max_attempts = jobmon_max_attempts,
    compute_resources = list(
      "memory" = "80G",
      "cores" = 5L,
      "runtime" = "3H"
    ),
    shell_path = image_shell_path_r,
    image_path = image_path,
    script_path = fs::path(code_dir, "03_complete_stage1.R"),
    main_dir = main_dir,
    loc_id = loc_id
  )

})

wf <- jobmonr::add_tasks(wf, tasks_complete_models)


# Run workflow ------------------------------------------------------------

wfr <- wf$run(resume = TRUE)

if (wfr == "D") {

  message("Workflow Completed!")

} else {


  logs <- demInternal::get_jobmon_errors(
    wf$workflow_id,
    odbc_section = jobmon_odbc_section
  )

  logs <- unique(logs)
  logs <- logs[order(error_message)]

  logs_fname <- paste0("jobmon_logs_", wf$workflow_id, ".csv")
  readr::write_csv(logs, file = fs::path(main_dir, "logs", logs_fname))

  stop("An issue occurred.")

}
