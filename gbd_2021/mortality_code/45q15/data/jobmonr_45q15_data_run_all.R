################################################################################
##                                                                      
## Description: Runs all steps of 45q15 data pipeline
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
                      help = "45q15 version id")
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
  
  version_id <- x
  username <- ""
  gbd_year <- 2020
  workflow_args <- ""
  mark_best <- FALSE
  workflow_args <- ""
  
}

code_dir <- fs::path("FILEPATH")
configs_dir <- fs::path(
  "FILEPATH", 
  paste0("Mort_Pipeline_", workflow_args), 
  "jobmon"
)

# Load mortality production config file
config <- config::get(
  file = fs::path(configs_dir, "mortality_production.yml"),
  use_parent = FALSE
)
list2env(config, .GlobalEnv)

# Set file paths
run_folder <- fs::path("FILEPATH", version_id)
input_folder <- fs::path(run_folder, "inputs")
output_folder <- fs::path(run_folder, "outputs")

for(dir in c(run_folder, input_folder, output_folder)) {
  
  dir.create(dir)
  
}

# Pull lineage
version_lineage <- get_proc_lineage(
  model_name = "45q15",
  model_type = "data",
  run_id = version_id
)
ddm_version_id <- version_lineage[
  parent_process_name == "ddm estimate", parent_run_id
]

# Save locations
loc_map <- get_locations(
  level = "all", 
  gbd_type = "ap_old", 
  gbd_year = gbd_year
)
loc_map <- loc_map[level >= 3]
loc_map[is.na(local_id_2013), local_id_2013 := ihme_loc_id]
loc_map <- loc_map[, .(iso3 = local_id_2013, country = location_name,
                       ihme_loc_id, location_id)]

readr::write_csv(loc_map, fs::path(input_folder, "loc_map.csv"))

## Create Jobmon workflow, task templates, and tasks ---------------------------

# Create workflow
wf_tool <- tool(name = "45q15_data")

set_default_tool_resources(
  tool = wf_tool,
  default_cluster_name = jobmon_cluster_name,
  resources = list("project" = submission_project_name,
                   "constraints" = "archive")
)

# Create templates
stata_template <- task_template(
  tool = wf_tool,
  template_name = "stata_template",
  command_template = paste(
    "PATH=FILEPATH:$PATH",
    "OMP_NUM_THREADS=0 python {script_path}",
    "--run_file {code_path}",
    "--kwargs {kwargs}"
  ),
  op_args = list("script_path"),
  task_args = list(),
  node_args = list("code_path", "kwargs")
)

upload_template <- task_template(
  tool = wf_tool,
  template_name = "upload",
  command_template = paste(
    "PATH=FILEPATH:$PATH",
    "OMP_NUM_THREADS=0",
    "{shell_path}",
    "-i {image_path}",
    "-s {script_path}",
    "--version_id {version_id}",
    "--mark_best {mark_best}"
  ),
  op_args = list("shell_path", "script_path", "image_path"),
  task_args = list(),
  node_args = list("version_id", "mark_best")
)

# Create workflow
wf <- workflow(
  tool = wf_tool,
  workflow_args = paste0("data_45q15_", workflow_args)
)

wf$workflow_attributes = list()

# Create tasks
compile_sources_task <- task(
  task_template = stata_template,
  name = "compile_adult_sources",
  upstream_tasks = list(),
  max_attempts = 3,
  script_path = script_path_stata,
  code_path = fs::path(code_dir, "01_compile_all_adult_sources.do"),
  kwargs = paste(version_id, ddm_version_id, sep = ","),
  compute_resources = list(
    "memory" = "2G",
    "cores" = 2L,
    "queue" = queue,
    "runtime" = "300S"
  )
)

wf <- add_tasks(workflow = wf, tasks = list(compile_sources_task))

format_task <- task(
  task_template = stata_template,
  name = "format_data",
  upstream_tasks = list(compile_sources_task),
  max_attempts = 3,
  script_path = script_path_stata,
  code_path = fs::path(code_dir, "02_format_data.do"),
  kwargs = paste0(version_id),
  compute_resources = list(
    "memory" = "2G",
    "cores" = 2L,
    "queue" = queue,
    "runtime" = "300S"
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
  script_path = fs::path(code_dir, "03_upload_45q15.R"),
  version_id = as.integer(version_id),
  mark_best = mark_best,
  compute_resources = list(
    "memory" = "1G",
    "cores" = 2L,
    "queue" = queue,
    "runtime" = "600S"
  )
)

wf <- add_tasks(workflow = wf, tasks = list(upload_task))

## Run -------------------------------------------------------------------------

wfr <- wf$run(resume = TRUE)


if(wfr != "D") {
  
  # Send failure notification
  send_slack_message(
    message = paste0("45q15 data ", version_id, " failed."),
    channel = paste0("@", Sys.getenv("USER")),
    icon = ":thinking_face:",
    botname = "MortBot"
  )  
  
  # Stop process
  stop("45q15 data failed!")
  
} else {
  
  message("45q15 data completed!")
  
}

