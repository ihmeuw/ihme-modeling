################################################################################
##                                                                            ##
## Description:                                                               ##
##   * Submit the covid excess mortality data process jobs using jobmonr      ##
## Steps:                                                                     ##
##   * Build jobmon workflow object with correct tasks                        ##
##   * Submit workflow                                                        ##
##   * Slack upon failure                                                     ##
## Inputs:                                                                    ##
##   * detailed configuration file                                            ##
##   * locations to be submitted for                                          ##
##       parallel steps                                                       ##
##                                                                            ##
################################################################################

library(argparse)
library(data.table)
library(demInternal)
library(jobmonr)

## Read in arguments and inputs ------------------------------------------------

if (interactive()) {

  main_dir <- ""

} else {

  parser <- argparse::ArgumentParser()

  parser$add_argument(
    "--main_dir", type = "character", required = TRUE,
    help = "main versioned directory for this data run"
  )

  args <- parser$parse_args()
  list2env(args, .GlobalEnv)

}

# load config file
config <- config::get(
  file = fs::path(),
  use_parent = FALSE
)

list2env(config, .GlobalEnv)

## Get location hierarchy ------------------------------------------------------

loc_table <- fread(fs::path())
loc_table <- loc_table[is_estimate_1 == TRUE | is_estimate_2 == TRUE | "ihme_loc_id" == "Mumbai"]
loc_list <- unique(loc_table$location_id)

## Create Jobmon workflow, task templates, and tasks ---------------------------

# Create new workflow
wf_tool <- tool(name = paste0("covid_em_data_", run_id_covid_em_data))

wf_tool <- set_default_tool_resources(
  tool = wf_tool,
  default_cluster_name = jobmon_cluster_name,
  resources = list("project" = submission_project_name)
)

# Create task templates
template_inputs <- jobmonr::task_template(
  tool = wf_tool,
  template_name = paste0("download_covid_em_data_inputs_", run_id_covid_em_data),
  command_template = paste(
    "PYTHONPATH= PATH=",
    "{shell_path} -i {image_path} -s {script_path}",
    "--main_dir {main_dir}"
  ),
  op_args = list("shell_path", "image_path", "script_path"),
  task_args = list(),
  node_args = list("main_dir")
)

template_processing <- jobmonr::task_template(
  tool = wf_tool,
  template_name = paste0("process_covid_em_", run_id_covid_em_data),
  command_template = paste(
    "PATH= $PATH OMP_NUM_THREADS=0",
    "{shell_path} -i {image_path} -s {script_path}",
    "--main_dir {main_dir}",
    "--loc_id {loc_id}"
  ),
  op_args = list("shell_path", "image_path", "script_path"),
  task_args = list(),
  node_args = list("main_dir", "loc_id")
)

template_diagnostics <- jobmonr::task_template(
  tool = wf_tool,
  template_name = paste0("diagnostics_covid_em_data_", run_id_covid_em_data),
  command_template = paste(
    "PATH= $PATH OMP_NUM_THREADS=0",
    "{shell_path} -i {image_path} -s {script_path}",
    "--main_dir {main_dir}"
  ),
  op_args = list("shell_path", "image_path", "script_path"),
  task_args = list(),
  node_args = list("main_dir")
)

# Create tasks and add to workflow
wf <- jobmonr::workflow(
  tool = wf_tool,
  workflow_args = run_id_covid_em_data
)

wf$workflow_attributes <- list()

task_inputs <- jobmonr::task(
  task_template = template_inputs,
  name = paste0("download_covid_em_data_inputs_", run_id_covid_em_data),
  max_attempts = 3,
  shell_path = image_shell_path_r,
  image_path = image_path,
  script_path = glue::glue(""),
  main_dir = main_dir,
  compute_resources = list(
    "memory" = "25G",
    "cores" = 2L,
    "queue" = queue,
    "constraints" = "archive",
    "runtime" = "10800S"
  )
)

wf <- jobmonr::add_tasks(
  workflow = wf,
  tasks = list(task_inputs)
)

tasks_processing <- lapply(loc_list, function(i) {

  ihme_loc <- loc_table[location_id == i]$ihme_loc_id

  task_processing <- jobmonr::task(
    task_template = template_processing,
    name = paste0("process_covid_em_", ihme_loc, "_", run_id_covid_em_data),
    upstream_tasks = list(task_inputs),
    max_attempts = 3,
    shell_path = image_shell_path_r,
    image_path = image_path,
    script_path = glue::glue(""),
    main_dir = main_dir,
    loc_id = i,
    compute_resources = list(
      "memory" = "25G",
      "cores" = 5L,
      "queue" = queue,
      "constraints" = "archive",
      "runtime" = "10800S"
    )
  )

  return(task_processing)

})

wf <- jobmonr::add_tasks(
  workflow = wf,
  tasks = tasks_processing
)

task_diagnostics <- jobmonr::task(
  task_template = template_diagnostics,
  name = paste0("diagnostics_covid_em_data_", run_id_covid_em_data),
  upstream_tasks = tasks_processing,
  max_attempts = 3,
  shell_path = image_shell_path_r,
  image_path = image_path,
  script_path = glue::glue(""),
  main_dir = main_dir,
  compute_resources = list(
    "memory" = "50G",
    "cores" = 2L,
    "queue" = queue,
    "runtime" = "10800S"
  )
)

wf <- jobmonr::add_tasks(
  workflow = wf,
  tasks = list(task_diagnostics)
)

## Run workflow ----------------------------------------------------------------

wfr <- wf$run(resume = TRUE)

## Report model success or failure ---------------------------------------------

if (wfr == "D") {

  print("Completed!")

} else {

  demInternal::send_slack_message(
    message = paste0("COVID excess mortality data with run comment ", run_comment, " failed."),
    channel = slack_channel,
    icon = "",
    botname = ""
  )

  # Stop process
  stop("An issue occurred.")

}
