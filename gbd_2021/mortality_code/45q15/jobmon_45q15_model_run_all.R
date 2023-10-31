################################################################################
##                                                                            ##
## 45q15 Run All                                                              ##
##                                                                            ##
################################################################################

library(argparse)
library(data.table)
library(jobmonr)
library(mortdb, lib.loc = "FILEPATH")

## Read in arguments and inputs ------------------------------------------------

if (interactive()) {
  
  version_id <- x
  username <- Sys.getenv("USER")
  mark_best <- F
  code_dir <- "FILEPATH"
  config_dir <- paste0("FILEPATH")
  workflow_args <- format(Sys.time(), "%Y-%m-%d-%H-%M")
  
} else {
  
  parser <- argparse::ArgumentParser()
  
  parser$add_argument(
    "--version_id", type = "integer", required = TRUE,
    help = "45q15 estimate version id"
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
  file = fs::path("FILEPATH"),
  use_parent = FALSE
)

list2env(config, .GlobalEnv)

# Additional variables
hiv_draws <- 1L
hiv_uncert <- 0L
hiv_update <- 0L
hiv_scalars <- TRUE

# Get GBD round id
gbd_round_id <- mortdb::get_gbd_round(gbd_year = gbd_year)

# Pull proc lineage
proc_lineage <- mortdb::get_proc_lineage(
  model_name = "45q15",
  model_type = "estimate",
  run_id = version_id
)

data_45q15_version <- as.integer(proc_lineage[parent_process_name == "45q15 data"]$parent_run_id)
ddm_estimate_version <- as.integer(proc_lineage[parent_process_name == "ddm estimate"]$parent_run_id)
m5q0_version_id <- as.integer(proc_lineage[parent_process_name == "5q0 estimate"]$parent_run_id)
population_estimate_version <- as.integer(proc_lineage[parent_process_name == "population estimate"]$parent_run_id)

# Pull external inputsf
external_inputs <- mortdb::get_external_input_map(
  process_name = "45q15 estimate",
  run_id = version_id
)

edu_model_version_id <- as.integer(external_inputs[external_input_name == "mean_edu"]$external_input_version)
hiv_cdr_model_version_id <- as.integer(external_inputs[external_input_name == "hiv_adult_cdr"]$external_input_version)
ldi_model_version_id <- as.integer(external_inputs[external_input_name == "ldi_pc"]$external_input_version)

# Update code_dir
code_dir <- fs::path("FILEPATH")

# Set filepaths and create directories
output_dir <- paste0("FILEPATH")
subdirs <- c("compiled_gpr", "data", "draws", "gpr", "graphs", "logs", "stage_1", "stage_2")

for (dir in subdirs) {
  
  if (!dir.exists(paste0(output_dir, dir, "/"))) dir.create(paste0(output_dir, dir, "/"), recursive = TRUE)
  
}

## Get location hierarchy ------------------------------------------------------

locations <- mortdb::get_locations(
  gbd_year = gbd_year,
  gbd_type = "ap_old",
  level = "estimate"
)

locations <- locations[level_all == 1 | location_id == 44849]

locations[, region_name := gsub(" ", "_", region_name)]
locations[, region_name := gsub("-", "_", region_name)]
locations[, region_name := gsub(",", "_", region_name)]

locations[, tmp := 1]

temp <- data.table(tmp = c(1, 1), sex = c("male", "female"))

locations <- merge(
  locations,
  temp,
  by = "tmp",
  all = TRUE,
  allow.cartesian = TRUE
)

locations <- locations[order(region_name, ihme_loc_id, sex)]

parent_locations <- locations[level_1 == 1 & level_2 == 0]

## Create Jobmon workflow, task templates, and tasks ---------------------------

# Create new workflow
wf_tool <- tool(name = "m45q15")

wf_tool <- set_default_tool_resources(
  tool = wf_tool,
  default_cluster_name = jobmon_cluster_name,
  resources = list("project" = submission_project_name)
)

# Create task templates
template_save_input_data <- jobmonr::task_template(
  tool = wf_tool,
  template_name = "save_input_data",
  command_template = paste(
    "PATH=FILEPATH:$PATH OMP_NUM_THREADS=0",
    "{shell_path} -i {image_path} -s {script_path}",
    "--version_id {version_id}",
    "--gbd_year {gbd_year}",
    "--end_year {end_year}",
    "--hiv_cdr_model_version_id {hiv_cdr_model_version_id}",
    "--edu_model_version_id {edu_model_version_id}",
    "--ldi_model_version_id {ldi_model_version_id}",
    "--decomp_step {decomp_step}",
    "--population_estimate_version {population_estimate_version}"
  ),
  op_args = list("shell_path", "image_path", "script_path"),
  task_args = list("gbd_year", "end_year"),
  node_args = list("version_id", "hiv_cdr_model_version_id", "edu_model_version_id",
                   "ldi_model_version_id", "decomp_step", "population_estimate_version")
)

template_format_data <- jobmonr::task_template(
  tool = wf_tool,
  template_name = "format_data",
  command_template = paste(
    "PATH=FILEPATH:$PATH OMP_NUM_THREADS=0",
    "{shell_path} -i {image_path} -s {script_path}",
    "--version_id {version_id}",
    "--gbd_year {gbd_year}",
    "--end_year {end_year}",
    "--data_45q15_version {data_45q15_version}",
    "--ddm_estimate_version {ddm_estimate_version}",
    "--m5q0_version_id {m5q0_version_id}"
  ),
  op_args = list("shell_path", "image_path", "script_path"),
  task_args = list("gbd_year", "end_year"),
  node_args = list("version_id", "data_45q15_version", "ddm_estimate_version", "m5q0_version_id")
)

template_calculate_data_density_select_parameters <- jobmonr::task_template(
  tool = wf_tool,
  template_name = "calculate_data_density_select_parameters",
  command_template = paste(
    "PYTHONPATH= PATH=FILEPATH:$PATH OMP_NUM_THREADS=2",
    "{script_path} {code_path} {conda_path} {conda_env}",
    "{version_id} {ddm_estimate_version} {code_dir}"
  ),
  op_args = list("script_path", "conda_path", "conda_env", "code_path"),
  task_args = list(),
  node_args = list("version_id", "ddm_estimate_version", "code_dir")
)

template_fit_prediction_model <- jobmonr::task_template(
  tool = wf_tool,
  template_name = "fit_prediction_model",
  command_template = paste(
    "PATH=FILEPATH:$PATH OMP_NUM_THREADS=0",
    "{shell_path} -i {image_path} -s {script_path}",
    "--version_id {version_id}",
    "--gbd_year {gbd_year}",
    "--sim {sim}",
    "--hivsims {hivsims}",
    "--code_dir {code_dir}"
  ),
  op_args = list("shell_path", "image_path", "script_path"),
  task_args = list("gbd_year"),
  node_args = list("version_id", "sim", "hivsims", "code_dir")
)

template_fit_second_stage_model <- jobmonr::task_template(
  tool = wf_tool,
  template_name = "fit_second_stage_model",
  command_template = paste(
    "PATH=FILEPATH:$PATH OMP_NUM_THREADS=0",
    "{shell_path} -i {image_path} -s {script_path}",
    "--version_id {version_id}",
    "--gbd_year {gbd_year}",
    "--end_year {end_year}",
    "--sim {sim}",
    "--hivsims {hivsims}",
    "--code_dir {code_dir}"
  ),
  op_args = list("shell_path", "image_path", "script_path"),
  task_args = list("gbd_year", "end_year"),
  node_args = list("version_id", "sim", "hivsims", "code_dir")
)

template_gpr <- jobmonr::task_template(
  tool = wf_tool,
  template_name = "gpr",
  command_template = paste(
    "PYTHONPATH= PATH=FILEPATH:$PATH OMP_NUM_THREADS=3",
    "{script_path} {code_path} {conda_path} {conda_env}",
    "{version_id} {ihme_loc_id} {code_dir}"
  ),
  op_args = list("script_path", "conda_path", "conda_env", "code_path"),
  task_args = list(),
  node_args = list("version_id", "ihme_loc_id", "code_dir")
)

template_rake_gpr <- jobmonr::task_template(
  tool = wf_tool,
  template_name = "rake_gpr",
  command_template = paste(
    "PATH=FILEPATH:$PATH OMP_NUM_THREADS=0",
    "{shell_path} -i {image_path} -s {script_path}",
    "--version_id {version_id}",
    "--ihme_loc_id {ihme_loc_id}",
    "--gbd_year {gbd_year}",
    "--gbd_round_id {gbd_round_id}",
    "--hiv_uncert {hiv_uncert}"
  ),
  op_args = list("shell_path", "image_path", "script_path"),
  task_args = list("gbd_year", "gbd_round_id"),
  node_args = list("version_id", "ihme_loc_id", "hiv_uncert")
)

template_compile_unraked_gpr <- jobmonr::task_template(
  tool = wf_tool,
  template_name = "compile_unraked_gpr",
  command_template = paste(
    "PATH=FILEPATH:$PATH OMP_NUM_THREADS=0",
    "{shell_path} -i {image_path} -s {script_path}",
    "--version_id {version_id}",
    "--ihme_loc_id {ihme_loc_id}"
  ),
  op_args = list("shell_path", "image_path", "script_path"),
  task_args = list(),
  node_args = list("version_id", "ihme_loc_id")
)

template_compile_all <- jobmonr::task_template(
  tool = wf_tool,
  template_name = "compile_all",
  command_template = paste(
    "PATH=FILEPATH:$PATH OMP_NUM_THREADS=0",
    "{shell_path} -i {image_path} -s {script_path}",
    "--version_id {version_id}",
    "--hiv_uncert {hiv_uncert}"
  ),
  op_args = list("shell_path", "image_path", "script_path"),
  task_args = list(),
  node_args = list("version_id", "hiv_uncert")
)

template_graphing <- jobmonr::task_template(
  tool = wf_tool,
  template_name = "graphing",
  command_template = paste(
    "PATH=FILEPATH:$PATH OMP_NUM_THREADS=0",
    "{shell_path} -i {image_path} -s {script_path}",
    "--version_id {version_id}",
    "--gbd_year {gbd_year}",
    "--end_year {end_year}",
    "--data_45q15_version {data_45q15_version}",
    "--ddm_estimate_version {ddm_estimate_version}"
  ),
  op_args = list("shell_path", "image_path", "script_path"),
  task_args = list("gbd_year", "end_year"),
  node_args = list("version_id", "data_45q15_version", "ddm_estimate_version")
)

template_upload <- jobmonr::task_template(
  tool = wf_tool,
  template_name = "upload",
  command_template = paste(
    "PATH=FILEPATH:$PATH OMP_NUM_THREADS=0",
    "{shell_path} -i {image_path} -s {script_path}",
    "--version_id {version_id}",
    "--gbd_year {gbd_year}",
    "--end_year {end_year}",
    "--data_45q15_version {data_45q15_version}",
    "--ddm_estimate_version {ddm_estimate_version}",
    "--population_estimate_version {population_estimate_version}",
    "--mark_best {mark_best}"
  ),
  op_args = list("shell_path", "image_path", "script_path"),
  task_args = list("gbd_year", "end_year"),
  node_args = list("version_id", "data_45q15_version", "ddm_estimate_version",
                   "population_estimate_version", "mark_best")
)

# Create tasks and add to workflow
wf <- jobmonr::workflow(
  tool = wf_tool,
  workflow_args = paste0("m45q15_", workflow_args)
)

wf$workflow_attributes <- list()

task_save_input_data <- jobmonr::task(
  task_template = template_save_input_data,
  name = paste0("save_input_data_45q15_", version_id),
  max_attempts = 3,
  shell_path = shell_path_r,
  image_path = image_path,
  script_path = glue::glue("{code_dir}/00_save_inputs.R"),
  version_id = version_id,
  gbd_year = gbd_year,
  end_year = end_year,
  hiv_cdr_model_version_id = hiv_cdr_model_version_id,
  edu_model_version_id = edu_model_version_id,
  ldi_model_version_id = ldi_model_version_id,
  decomp_step = decomp_step,
  population_estimate_version = population_estimate_version,
  compute_resources = list(
    "memory" = "6G",
    "cores" = 3L,
    "queue" = queue,
    "constraints" = "archive",
    "runtime" = "1000S"
  )
)

wf <- jobmonr::add_tasks(
  workflow = wf,
  tasks = list(task_save_input_data)
)

task_format_data <- jobmonr::task(
  task_template = template_format_data,
  name = paste0("format_data_45q15_", version_id),
  upstream_tasks = list(task_save_input_data),
  max_attempts = 3,
  shell_path = shell_path_r,
  image_path = image_path,
  script_path = glue::glue("{code_dir}/01_format_data.r"),
  version_id = version_id,
  gbd_year = gbd_year,
  end_year = end_year,
  data_45q15_version = data_45q15_version,
  ddm_estimate_version = ddm_estimate_version,
  m5q0_version_id = m5q0_version_id,
  compute_resources = list(
    "memory" = "2G",
    "cores" = 2L,
    "queue" = queue,
    "runtime" = "600S"
  )
)

wf <- jobmonr::add_tasks(
  workflow = wf,
  tasks = list(task_format_data)
)

task_calculate_data_density_select_parameters <- jobmonr::task(
  task_template = template_calculate_data_density_select_parameters,
  name = paste0("calculate_data_density_select_parameters_45q15_", version_id),
  upstream_tasks = list(task_format_data),
  max_attempts = 3,
  code_path = glue::glue("{code_dir}/02_calculate_data_density_select_parameters.py"),
  conda_path = conda_path,
  conda_env = conda_env,
  script_path = glue::glue("{code_dir}/02_calculate_data_density_select_parameters.sh"),
  version_id = version_id,
  ddm_estimate_version = ddm_estimate_version,
  code_dir = code_dir,
  compute_resources = list(
    "memory" = "2G",
    "cores" = 2L,
    "queue" = queue,
    "runtime" = "300S"
  )
)

wf <- jobmonr::add_tasks(
  workflow = wf,
  tasks = list(task_calculate_data_density_select_parameters)
)

task_fit_prediction_model <- jobmonr::task(
  task_template = template_fit_prediction_model,
  name = paste0("fit_prediction_model_45q15_", version_id),
  upstream_tasks = list(task_calculate_data_density_select_parameters),
  max_attempts = 3,
  shell_path = shell_path_r,
  image_path = image_path,
  script_path = glue::glue("{code_dir}/03_fit_prediction_model.r"),
  version_id = version_id,
  gbd_year = gbd_year,
  sim = hiv_draws,
  hivsims = hiv_uncert,
  code_dir = code_dir,
  compute_resources = list(
    "memory" = "4G",
    "cores" = 2L,
    "queue" = queue,
    "runtime" = "3600S"
  )
)

wf <- jobmonr::add_tasks(
  workflow = wf,
  tasks = list(task_fit_prediction_model)
)

task_fit_second_stage_model <- jobmonr::task(
  task_template = template_fit_second_stage_model,
  name = paste0("fit_second_stage_model_45q15_", version_id),
  upstream_tasks = list(task_fit_prediction_model),
  max_attempts = 3,
  shell_path = shell_path_r,
  image_path = image_path,
  script_path = glue::glue("{code_dir}/04_fit_second_stage.R"),
  version_id = version_id,
  gbd_year = gbd_year,
  end_year = end_year,
  sim = hiv_draws,
  hivsims = hiv_uncert,
  code_dir = code_dir,
  compute_resources = list(
    "memory" = "4G",
    "cores" = 2L,
    "queue" = queue,
    "runtime" = "3600S"
  )
)

wf <- jobmonr::add_tasks(
  workflow = wf,
  tasks = list(task_fit_second_stage_model)
)

tasks_gpr <- lapply(unique(locations$ihme_loc_id), function(i) {
  
  task_gpr <- jobmonr::task(
    task_template = template_gpr,
    name = paste0("gpr_", i, "_45q15_", version_id),
    upstream_tasks = list(task_fit_second_stage_model),
    max_attempts = 3,
    code_path = glue::glue("{code_dir}/05_fit_gpr.py"),
    conda_path = conda_path,
    conda_env = conda_env,
    script_path = glue::glue("{code_dir}/05_fit_gpr.sh"),
    ihme_loc_id = i,
    version_id = version_id,
    code_dir = code_dir,
    compute_resources = list(
      "memory" = "6G",
      "cores" = 3L,
      "queue" = queue,
      "constraints" = "archive",
      "runtime" = "720S"
    )
  )
  
  return(task_gpr)
  
})

wf <- jobmonr::add_tasks(
  workflow = wf,
  tasks = tasks_gpr
)

tasks_rake_gpr <- lapply(unique(parent_locations$ihme_loc_id), function(i) {
  
  task_rake_gpr <- jobmonr::task(
    task_template = template_rake_gpr,
    name = paste0("raking_", i, "_45q15_", version_id),
    upstream_tasks = tasks_gpr,
    max_attempts = 3,
    shell_path = shell_path_r,
    image_path = image_path,
    script_path = glue::glue("{code_dir}/06_rake_gpr_results.R"),
    version_id = version_id,
    ihme_loc_id = i,
    gbd_year = gbd_year,
    gbd_round_id = gbd_round_id,
    hiv_uncert = hiv_uncert,
    compute_resources = list(
      "memory" = "12G",
      "cores" = 5L,
      "queue" = queue,
      "runtime" = "900S"
    )
  )
  
  return(task_rake_gpr)
  
})

wf <- jobmonr::add_tasks(
  workflow = wf,
  tasks = tasks_rake_gpr
)

tasks_compile_unraked_gpr <- lapply(unique(locations$ihme_loc_id), function(i) {
  
  task_compile_unraked_gpr <- jobmonr::task(
    task_template = template_compile_unraked_gpr,
    name = paste0("compile_unraked_", i, "_45q15_", version_id),
    upstream_tasks = tasks_rake_gpr,
    max_attempts = 3,
    shell_path = shell_path_r,
    image_path = image_path,
    script_path = glue::glue("{code_dir}/07a_compile_unraked_gpr_results.R"),
    version_id = version_id,
    ihme_loc_id = i,
    compute_resources = list(
      "memory" = "5G",
      "cores" = 1L,
      "queue" = queue,
      "runtime" = "360S"
    )
  )
  
  return(task_compile_unraked_gpr)
  
})

wf <- jobmonr::add_tasks(
  workflow = wf,
  tasks = tasks_compile_unraked_gpr
)

task_compile_all <- jobmonr::task(
  task_template = template_compile_all,
  name = paste0("compile_all_45q15_", version_id),
  upstream_tasks = tasks_compile_unraked_gpr,
  max_attempts = 3,
  shell_path = shell_path_r,
  image_path = image_path,
  script_path = glue::glue("{code_dir}/07b_compile_all.r"),
  version_id = version_id,
  hiv_uncert = hiv_uncert,
  compute_resources = list(
    "memory" = "10G",
    "cores" = 5L,
    "queue" = queue,
    "runtime" = "10000S"
  )
)

wf <- jobmonr::add_tasks(
  workflow = wf,
  tasks = list(task_compile_all)
)

task_graphing <- jobmonr::task(
  task_template = template_graphing,
  name = paste0("graphing_45q15_", version_id),
  upstream_tasks = list(task_compile_all),
  max_attempts = 3,
  shell_path = shell_path_r,
  image_path = image_path,
  script_path = glue::glue("{code_dir}/08_graph_all_stages_plus_opposite_sex.r"),
  version_id = version_id,
  gbd_year = gbd_year,
  end_year = end_year,
  data_45q15_version = data_45q15_version,
  ddm_estimate_version = ddm_estimate_version,
  compute_resources = list(
    "memory" = "4G",
    "cores" = 2L,
    "queue" = queue,
    "constraints" = "archive",
    "runtime" = "1000S"
  )
)

wf <- jobmonr::add_tasks(
  workflow = wf,
  tasks = list(task_graphing)
)

task_upload_results <- jobmonr::task(
  task_template = template_upload,
  name = paste0("upload_results_45q15_", version_id),
  upstream_tasks = list(task_graphing),
  max_attempts = 3,
  shell_path = shell_path_r,
  image_path = image_path,
  script_path = glue::glue("{code_dir}/10_upload_results.R"),
  version_id = version_id,
  gbd_year = gbd_year,
  end_year = end_year,
  data_45q15_version = data_45q15_version,
  ddm_estimate_version = ddm_estimate_version,
  population_estimate_version = population_estimate_version,
  mark_best = mark_best,
  compute_resources = list(
    "memory" = "4G",
    "cores" = 2L,
    "queue" = queue,
    "constraints" = "archive",
    "runtime" = "1000S"
  )
)

wf <- jobmonr::add_tasks(
  workflow = wf,
  tasks = list(task_upload_results)
)

## Run workflow ----------------------------------------------------------------

wfr <- wf$run(resume = TRUE)

## Upload to database or report model failure ----------------------------------

if (wfr == "D") {
  
  print("45q15 model completed!")
  
  # Save hiv_covariates.csv for Empirical LT data prep
  model_45q15_input <- fread(fs::path("FILEPATH"))
  setnames(model_45q15_input, "hiv", "adult_hiv_cdr")
  model_45q15_input <- unique(model_45q15_input[, c("ihme_loc_id", "year", "sex", "adult_hiv_cdr")])
  readr::write_csv(model_45q15_input, fs::path("FILEPATH"))
  
} else {
  
  mortdb::send_slack_message(
    message = paste0("45q15 model ", version_id, " failed."),
    channel = paste0("@", username),
    icon = ":thinking_face:",
    botname = "45q15ModelBot"
  )
  
  # Stop process
  stop("45q15 model failed!")
  
}
