################################################################################
##                                                                            ##
## 5q0 Run All                                                                ##
##                                                                            ##
################################################################################

library(argparse)
library(data.table)
library(mortdb)
library(jobmonr)

## Read in arguments and inputs ------------------------------------------------

if (interactive()) {
  
  version_id <- x
  username <- Sys.getenv("USER")
  mark_best <- F
  code_dir <- "FILEPATH"
  config_dir <- "FILEPATH"
  workflow_args <- format(Sys.time(), "%Y-%m-%d-%H-%M")
  
} else {
  
  parser <- argparse::ArgumentParser()
  
  parser$add_argument(
    "--version_id", type = "integer", required = TRUE,
    help = "5q0 estimate version id"
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

# Update code_dir
code_dir <- fs::path(code_dir, "child-mortality")

# Load 5q0 config file
config_5q0 <- rjson::fromJSON(
  file = fs::path("FILEPATH")
)

num_rakings <- length(config_5q0$rakings)

# Additional variables
st_loess <- 1L

# Get GBD round id
gbd_round_id <- as.integer(mortdb::get_gbd_round(gbd_year = gbd_year))

# Pull proc lineage
proc_lineage <- mortdb::get_proc_lineage(
  model_name = "5q0",
  model_type = "estimate",
  run_id = version_id
)

data_5q0_version <- as.integer(proc_lineage[parent_process_name == "5q0 data"]$parent_run_id)
population_estimate_version <- as.integer(proc_lineage[parent_process_name == "population estimate"]$parent_run_id)
emp_death_version <- as.integer(proc_lineage[parent_process_name == "death number empirical data"]$parent_run_id)

# Set filepaths and create directories
output_dir <- paste0("FILEPATH")
subdirs <- c("data", "model", "draws", "summaries", "graphs",
             sapply(0:(num_rakings - 1), function(i) { paste0("FILEPATH")}))

for (dir in subdirs) {
  
  if (!dir.exists("FILEPATH")) dir.create("FILEPATH", recursive = TRUE)
  
}

## Get location hierarchy ------------------------------------------------------

locations <- mortdb::get_locations(
  gbd_year = gbd_year,
  gbd_type = "ap_old",
  level = "estimate"
)

## Update json -----------------------------------------------------------------

# Update version/round arguments
config_5q0$version_id <- version_id
config_5q0$end_year <- end_year
config_5q0$gbd_round_id <- gbd_round_id

# Save version specific json
config_5q0_json <- rjson::toJSON(config_5q0)
write(config_5q0_json, fs::path(
  output_dir, 
  paste0("FILEPATH"))
)

## Create Jobmon workflow, task templates, and tasks ---------------------------

# Create new workflow
wf_tool <- tool(name = "mort5q0")

wf_tool <- set_default_tool_resources(
  tool = wf_tool,
  default_cluster_name = jobmon_cluster_name,
  resources = list("project" = submission_project_name)
)

# Create task templates
template_get_input <- jobmonr::task_template(
  tool = wf_tool,
  template_name = "get_input",
  command_template = paste(
    "PATH=FILEPATH:$PATH OMP_NUM_THREADS=0",
    "{shell_path} -i {image_path} -s {script_path}",
    "--version_id {version_id}",
    "--gbd_year {gbd_year}",
    "--start_year {start_year}",
    "--end_year {end_year}",
    "--data_5q0_version {data_5q0_version}",
    "--code_dir {code_dir}"
  ),
  op_args = list("shell_path", "image_path", "script_path"),
  task_args = list("gbd_year", "start_year", "end_year"),
  node_args = list("version_id", "data_5q0_version", "code_dir")
)

template_assess_vr_bias <- jobmonr::task_template(
  tool = wf_tool,
  template_name = "assess_vr_bias",
  command_template = paste(
    "PYTHONPATH= OMP_NUM_THREADS=1",
    "{script_path} {code_path} {conda_path} {conda_env}",
    "{version_id}"
  ),
  op_args = list("script_path", "conda_path", "code_path", "conda_env"),
  task_args = list(),
  node_args = list("version_id")
)

template_format_data <- jobmonr::task_template(
  tool = wf_tool,
  template_name = "format_data",
  command_template = paste(
    "PATH=FILEPATH:$PATH OMP_NUM_THREADS=0",
    "{shell_path} -i {image_path} -s {script_path}",
    "--version_id {version_id}",
    "--start_year {start_year}",
    "--end_year {end_year}",
    "--gbd_round_id {gbd_round_id}",
    "--population_estimate_version {population_estimate_version}",
    "--code_dir {code_dir}"
  ),
  op_args = list("shell_path", "image_path", "script_path"),
  task_args = list("start_year", "end_year", "gbd_round_id"),
  node_args = list("version_id", "population_estimate_version", "code_dir")
)

template_generate_hyperparameters <- jobmonr::task_template(
  tool = wf_tool,
  template_name = "generate_hyperparameters",
  command_template = paste(
    "PYTHONPATH= OMP_NUM_THREADS=2 ",
    "{script_path} {code_path} {conda_path} {conda_env}",
    "{version_id} {gbd_year} {emp_death_version} {code_dir}"
  ),
  op_args = list("script_path", "conda_path", "conda_env", "code_path"),
  task_args = list("gbd_year", "emp_death_version"),
  node_args = list("version_id", "code_dir")
)

template_fit_submodel_a <- jobmonr::task_template(
  tool = wf_tool,
  template_name = "fit_submodel_a",
  command_template = paste(
    "PATH=FILEPATH:$PATH OMP_NUM_THREADS=0",
    "{shell_path} -i {image_path} -s {script_path}",
    "--version_id {version_id}",
    "--gbd_round_id {gbd_round_id}",
    "--start_year {start_year}",
    "--end_year {end_year}",
    "--st_loess {st_loess}",
    "--code_dir {code_dir}"
  ),
  op_args = list("shell_path", "image_path", "script_path"),
  task_args = list("start_year", "end_year", "gbd_round_id"),
  node_args = list("version_id", "st_loess", "code_dir")
)

template_fit_submodel_b <- jobmonr::task_template(
  tool = wf_tool,
  template_name = "fit_submodel_b",
  command_template = paste(
    "PATH=FILEPATH:$PATH OMP_NUM_THREADS=0",
    "{shell_path} -i {image_path} -s {script_path}",
    "--version_id {version_id}",
    "--gbd_round_id {gbd_round_id}",
    "--start_year {start_year}",
    "--end_year {end_year}",
    "--st_loess {st_loess}",
    "--code_dir {code_dir}"
  ),
  op_args = list("shell_path", "image_path", "script_path"),
  task_args = list("start_year", "end_year", "gbd_round_id"),
  node_args = list("version_id", "st_loess", "code_dir")
)

template_fit_submodel_c <- jobmonr::task_template(
  tool = wf_tool,
  template_name = "fit_submodel_c",
  command_template = paste(
    "PATH=FILEPATH:$PATH OMP_NUM_THREADS=0",
    "{shell_path} -i {image_path} -s {script_path}",
    "--version_id {version_id}",
    "--gbd_round_id {gbd_round_id}",
    "--start_year {start_year}",
    "--end_year {end_year}",
    "--st_loess {st_loess}",
    "--code_dir {code_dir}"
  ),
  op_args = list("shell_path", "image_path", "script_path"),
  task_args = list("start_year", "end_year", "gbd_round_id"),
  node_args = list("version_id", "st_loess", "code_dir")
)

template_submodel_variance <- jobmonr::task_template(
  tool = wf_tool,
  template_name = "submodel_variance",
  command_template = paste(
    "python {script_path} --run_file {code_path} --kwargs {kwargs}"
  ),
  op_args = list("script_path", "code_path"),
  task_args = list(),
  node_args = list("kwargs")
)

template_submodel_gpr <- jobmonr::task_template(
  tool = wf_tool,
  template_name = "submodel_gpr",
  command_template = paste(
    "PYTHONPATH= OMP_NUM_THREADS=5",
    "{script_path} {code_path} {conda_path} {conda_env}",
    "{version_id} {location_id} {ihme_loc_id} {code_dir}"
  ),
  op_args = list("script_path", "conda_path", "conda_env", "code_path"),
  task_args = list(),
  node_args = list("version_id", "location_id", "ihme_loc_id", "code_dir")
)

template_submodel_gpr_compile <- jobmonr::task_template(
  tool = wf_tool,
  template_name = "submodel_gpr_compile",
  command_template = paste(
    "PYTHONPATH= OMP_NUM_THREADS=5",
    "{script_path} {code_path} {conda_path} {conda_env}",
    "{version_id}"
  ),
  op_args = list("script_path", "conda_path", "conda_env", "code_path"),
  task_args = list(),
  node_args = list("version_id")
)

template_raking <- jobmonr::task_template(
  tool = wf_tool,
  template_name = "raking",
  command_template = paste(
    "PATH=FILEPATH:$PATH OMP_NUM_THREADS=0",
    "{shell_path} -i {image_path} -s {script_path}",
    "--version_id {version_id}",
    "--start_year {start_year}",
    "--end_year {end_year}",
    "--gbd_round_id {gbd_round_id}",
    "--raking_id {raking_id}",
    "--parent_id {parent_id}",
    "--child_ids {child_ids}",
    "--code_dir {code_dir}"
  ),
  op_args = list("shell_path", "image_path", "script_path"),
  task_args = list("gbd_round_id", "start_year", "end_year"),
  node_args = list("version_id", "raking_id", "parent_id", "child_ids", "code_dir")
)

template_save_draws <- jobmonr::task_template(
  tool = wf_tool,
  template_name = "save_draws",
  command_template = paste(
    "PYTHONPATH= OMP_NUM_THREADS=1",
    "{script_path} {code_path} {conda_path} {conda_env}",
    "{version_id} {location_id}"
  ),
  op_args = list("script_path", "conda_path", "conda_env", "code_path"),
  task_args = list(),
  node_args = list("version_id", "location_id")
)

template_upload_prep <- jobmonr::task_template(
  tool = wf_tool,
  template_name = "upload_prep",
  command_template = paste(
    "PYTHONPATH= OMP_NUM_THREADS=4",
    "{script_path} {code_path} {conda_path} {conda_env}",
    "{version_id}"
  ),
  op_args = list("script_path", "conda_path", "conda_env", "code_path"),
  task_args = list(),
  node_args = list("version_id")
)

template_upload <- jobmonr::task_template(
  tool = wf_tool,
  template_name = "upload",
  command_template = paste(
    "PATH=FILEPATH:$PATH OMP_NUM_THREADS=0",
    "{shell_path} -i {image_path} -s {script_path}",
    "--version_id {version_id}",
    "--gbd_year {gbd_year}",
    "--mark_best {mark_best}",
    "--data_5q0_version {data_5q0_version}"
  ),
  op_args = list("shell_path", "image_path", "script_path"),
  task_args = list("gbd_year"),
  node_args = list("version_id", "mark_best", "data_5q0_version")
)

template_comparison <- jobmonr::task_template(
  tool = wf_tool,
  template_name = "comparison",
  command_template = paste(
    "PYTHONPATH= OMP_NUM_THREADS=5",
    "{script_path} {code_path} {conda_path} {conda_env}",
    "{version_id} {gbd_year} {end_year} {code_dir}"
  ),
  op_args = list("script_path", "conda_path", "conda_env", "code_path"),
  task_args = list("gbd_year", "end_year"),
  node_args = list("version_id", "code_dir")
)

template_graph <- jobmonr::task_template(
  tool = wf_tool,
  template_name = "graph",
  command_template = paste(
    "PATH=FILEPATH:$PATH OMP_NUM_THREADS=0",
    "{shell_path} -i {image_path} -s {script_path}",
    "--version_id {version_id}",
    "--gbd_year {gbd_year}",
    "--end_year {end_year}"
  ),
  op_args = list("shell_path", "image_path", "script_path"),
  task_args = list("gbd_year", "end_year"),
  node_args = list("version_id")
)

template_diagnostic_graph <- jobmonr::task_template(
  tool = wf_tool,
  template_name = "diagnostic_graph",
  command_template = paste(
    "PATH=FILEPATH:$PATH OMP_NUM_THREADS=0",
    "{shell_path} -i {image_path} -s {script_path}",
    "--version_id {version_id}",
    "--gbd_year {gbd_year}",
    "--end_year {end_year}"
  ),
  op_args = list("shell_path", "image_path", "script_path"),
  task_args = list("gbd_year", "end_year"),
  node_args = list("version_id")
)

template_compile_graph <- jobmonr::task_template(
  tool = wf_tool,
  template_name = "compile_graph",
  command_template = paste(
    "PYTHONPATH= OMP_NUM_THREADS=2",
    "{script_path} {code_path} {conda_path} {conda_env}",
    "{version_id} {gbd_year}"
  ),
  op_args = list("script_path", "conda_path", "conda_env", "code_path"),
  task_args = list("gbd_year"),
  node_args = list("version_id")
)

# Create tasks and add to workflow
wf <- jobmonr::workflow(
  tool = wf_tool,
  workflow_args = paste0("m5q0_", workflow_args)
)

wf$workflow_attributes <- list()

task_get_input <- jobmonr::task(
  task_template = template_get_input,
  name = paste0("get_data_5q0_", version_id),
  max_attempts = 3,
  shell_path = shell_path_r,
  image_path = image_path,
  script_path = glue::glue("{code_dir}/01_input.R"),
  version_id = version_id,
  gbd_year = gbd_year,
  start_year = start_year,
  end_year = end_year,
  data_5q0_version = data_5q0_version,
  code_dir = code_dir,
  compute_resources = list(
    "memory" = "2G",
    "cores" = 1L,
    "queue" = queue,
    "constraints" = "archive",
    "runtime" = "360S"
  )
)

wf <- jobmonr::add_tasks(
  workflow = wf,
  tasks = list(task_get_input)
)

task_assess_vr_bias <- jobmonr::task(
  task_template = template_assess_vr_bias,
  name = paste0("assess_vr_bias_5q0_", version_id),
  upstream_tasks = list(task_get_input),
  max_attempts = 3,
  code_path = glue::glue("{code_dir}/02_assess_vr_bias.py"),
  conda_path = conda_path,
  conda_env = conda_env,
  script_path = glue::glue("{code_dir}/02_assess_vr_bias.sh"),
  version_id = version_id,
  compute_resources = list(
    "memory" = "2G",
    "cores" = 1L,
    "queue" = queue,
    "runtime" = "360S"
  )
)

wf <- jobmonr::add_tasks(
  workflow = wf,
  tasks = list(task_assess_vr_bias)
)

task_format_data <- jobmonr::task(
  task_template = template_format_data,
  name = paste0("format_data_5q0_", version_id),
  upstream_tasks = list(task_assess_vr_bias),
  max_attempts = 3,
  shell_path = shell_path_r,
  image_path = image_path,
  script_path = glue::glue("{code_dir}/03_format_covariates_for_prediction_models.R"),
  version_id = version_id,
  gbd_round_id = gbd_round_id,
  start_year = start_year,
  end_year = end_year,
  population_estimate_version = population_estimate_version,
  code_dir = code_dir,
  compute_resources = list(
    "memory" = "2G",
    "cores" = 1L,
    "queue" = queue,
    "constraints" = "archive",
    "runtime" = "600S"
  )
)

wf <- jobmonr::add_tasks(
  workflow = wf,
  tasks = list(task_format_data)
)

task_generate_hyperparameters <- jobmonr::task(
  task_template = template_generate_hyperparameters,
  name = paste0("generate_hyperparameters_5q0_", version_id),
  upstream_tasks = list(task_format_data),
  max_attempts = 3,
  code_path = glue::glue("{code_dir}/03_generate_hyperparameters.py"),
  conda_path = conda_path,
  conda_env = conda_env,
  script_path = glue::glue("{code_dir}/03_generate_hyperparameters.sh"),
  version_id = version_id,
  gbd_year = gbd_year,
  emp_death_version = emp_death_version,
  code_dir = code_dir,
  compute_resources = list(
    "memory" = "2G",
    "cores" = 2L,
    "queue" = queue,
    "runtime" = "360S"
  )
)

wf <- jobmonr::add_tasks(
  workflow = wf,
  tasks = list(task_generate_hyperparameters)
)

task_fit_submodel_a <- jobmonr::task(
  task_template = template_fit_submodel_a,
  name = paste0("a_submodel_5q0_", version_id),
  upstream_tasks = list(task_generate_hyperparameters),
  max_attempts = 3,
  shell_path = shell_path_r,
  image_path = image_path,
  script_path = glue::glue("{code_dir}/04a_fit_submodel.R"),
  version_id = version_id,
  gbd_round_id = gbd_round_id,
  start_year = start_year,
  end_year = end_year,
  st_loess = st_loess,
  code_dir = code_dir,
  compute_resources = list(
    "memory" = "8G",
    "cores" = 4L,
    "queue" = queue,
    "runtime" = "3600S"
  )
)

wf <- jobmonr::add_tasks(
  workflow = wf,
  tasks = list(task_fit_submodel_a)
)

task_fit_submodel_b <- jobmonr::task(
  task_template = template_fit_submodel_b,
  name = paste0("b_submodel_5q0_", version_id),
  upstream_tasks = list(task_fit_submodel_a),
  max_attempts = 3,
  shell_path = shell_path_r,
  image_path = image_path,
  script_path = glue::glue("{code_dir}/04b_fit_submodel.R"),
  version_id = version_id,
  gbd_round_id = gbd_round_id,
  start_year = start_year,
  end_year = end_year,
  st_loess = st_loess,
  code_dir = code_dir,
  compute_resources = list(
    "memory" = "4G",
    "cores" = 2L,
    "queue" = queue,
    "runtime" = "360S"
  )
)

wf <- jobmonr::add_tasks(
  workflow = wf,
  tasks = list(task_fit_submodel_b)
)

task_fit_submodel_c <- jobmonr::task(
  task_template = template_fit_submodel_c,
  name = paste0("c_submodel_5q0_", version_id),
  upstream_tasks = list(task_fit_submodel_b),
  max_attempts = 3,
  shell_path = shell_path_r,
  image_path = image_path,
  script_path = glue::glue("{code_dir}/04c_fit_submodel.R"),
  version_id = version_id,
  gbd_round_id = gbd_round_id,
  start_year = start_year,
  end_year = end_year,
  st_loess = st_loess,
  code_dir = code_dir,
  compute_resources = list(
    "memory" = "8G",
    "cores" = 4L,
    "queue" = queue,
    "runtime" = "3600S"
  )
)

wf <- jobmonr::add_tasks(
  workflow = wf,
  tasks = list(task_fit_submodel_c)
)

task_submodel_variance <- jobmonr::task(
  task_template = template_submodel_variance,
  name = paste0("variance_5q0_", version_id),
  upstream_tasks = list(task_fit_submodel_c),
  max_attempts = 3,
  code_path = glue::glue("{code_dir}/05_calculate_data_variance.do"),
  kwargs = paste0(version_id, ",", code_dir),
  script_path = script_path_stata,
  compute_resources = list(
    "memory" = "4",
    "cores" = 2L,
    "queue" = queue,
    "constraints" = "archive",
    "runtime" = "360S"
  )
)

wf <- jobmonr::add_tasks(
  workflow = wf,
  tasks = list(task_submodel_variance)
)

tasks_submodel_gpr <- lapply(unique(locations$location_id), function(i) {
  
  task_submodel_gpr <- jobmonr::task(
    task_template = template_submodel_gpr,
    name = paste0("gpr_5q0_", version_id),
    upstream_tasks = list(task_submodel_variance),
    max_attempts = 3,
    code_path = glue::glue("{code_dir}/06_fit_gpr.py"),
    conda_path = conda_path,
    conda_env = conda_env,
    script_path = glue::glue("{code_dir}/06_fit_gpr.sh"),
    version_id = version_id,
    location_id = i,
    ihme_loc_id = locations[location_id == i]$ihme_loc_id,
    code_dir = code_dir,
    compute_resources = list(
      "memory" = "10G",
      "cores" = 5L,
      "queue" = queue,
      "runtime" = "720S"
    )
  )
  
  return(task_submodel_gpr)
  
})

wf <- jobmonr::add_tasks(
  workflow = wf,
  tasks = tasks_submodel_gpr
)

task_submodel_gpr_compile <- jobmonr::task(
  task_template = template_submodel_gpr_compile,
  name = paste0("compile_gpr_5q0_", version_id),
  upstream_tasks = tasks_submodel_gpr,
  max_attempts = 3,
  code_path = glue::glue("{code_dir}/07_append_gpr.py"),
  conda_path = conda_path,
  conda_env = conda_env,
  script_path = glue::glue("{code_dir}/07_append_gpr.sh"),
  version_id = version_id,
  compute_resources = list(
    "memory" = "10G",
    "cores" = 5L,
    "queue" = queue,
    "runtime" = "3600S"
  )
)

wf <- jobmonr::add_tasks(
  workflow = wf,
  tasks = list(task_submodel_gpr_compile)
)

tasks_raking <- lapply(1:num_rakings, function(i) {
  
  raking_id_temp <- as.integer(config_5q0[["rakings"]][[i]]$raking_id)
  parent_id_temp <- as.integer(config_5q0[["rakings"]][[i]]$parent_id)
  child_ids_temp <- paste(as.integer(config_5q0[["rakings"]][[i]]$child_ids), collapse = " ")
  
  task_raking <- jobmonr::task(
    task_template = template_raking,
    name = paste0("raking_5q0_", version_id),
    upstream_tasks = list(task_submodel_gpr_compile),
    max_attempts = 3,
    shell_path = shell_path_r,
    image_path = image_path,
    script_path = glue::glue("{code_dir}/08_rake.R"),
    version_id = version_id,
    gbd_round_id = gbd_round_id,
    start_year = start_year,
    end_year = end_year,
    raking_id = raking_id_temp,
    parent_id = parent_id_temp,
    child_ids = child_ids_temp,
    code_dir = code_dir,
    compute_resources = list(
      "memory" = "5G",
      "cores" = 2L,
      "queue" = queue,
      "constraints" = "archive",
      "runtime" = "900S"
    )
  )
  
  return(task_raking)
  
})

wf <- jobmonr::add_tasks(
  workflow = wf,
  tasks = tasks_raking
)

tasks_save_draws <- lapply(unique(locations$location_id), function(i) {
  
  task_save_draws <- jobmonr::task(
    task_template = template_save_draws,
    name = paste0("save_draws_5q0_", version_id),
    upstream_tasks = tasks_raking,
    max_attempts = 3,
    code_path = glue::glue("{code_dir}/09_save_draws.py"),
    conda_path = conda_path,
    conda_env = conda_env,
    script_path = glue::glue("{code_dir}/09_save_draws.sh"),
    version_id = version_id,
    location_id = i,
    compute_resources = list(
      "memory" = "1G",
      "cores" = 1L,
      "queue" = queue,
      "constraints" = "archive",
      "runtime" = "180S"
    )
  )
  
  return(task_save_draws)
  
})

wf <- jobmonr::add_tasks(
  workflow = wf,
  tasks = tasks_save_draws
)

task_upload_prep <- jobmonr::task(
  task_template = template_upload_prep,
  name = paste0("upload_prep_5q0_", version_id),
  upstream_tasks = tasks_save_draws,
  max_attempts = 3,
  code_path = glue::glue("{code_dir}/10_prep_upload_file.py"),
  conda_path = conda_path,
  conda_env = conda_env,
  script_path = glue::glue("{code_dir}/10_prep_upload_file.sh"),
  version_id = version_id,
  compute_resources = list(
    "memory" = "8G",
    "cores" = 4L,
    "queue" = queue,
    "constraints" = "archive",
    "runtime" = "10000S"
  )
)

wf <- jobmonr::add_tasks(
  workflow = wf,
  tasks = list(task_upload_prep)
)

task_upload <- jobmonr::task(
  task_template = template_upload,
  name = paste0("upload_5q0_", version_id),
  upstream_tasks = list(task_upload_prep),
  max_attempts = 3,
  shell_path = shell_path_r,
  image_path = image_path,
  script_path = glue::glue("{code_dir}/11_upload.R"),
  version_id = version_id,
  gbd_year = gbd_year,
  data_5q0_version = data_5q0_version,
  mark_best = mark_best,
  compute_resources = list(
    "memory" = "10G",
    "cores" = 5L,
    "queue" = queue,
    "constraints" = "archive",
    "runtime" = "10000S"
  )
)

wf <- jobmonr::add_tasks(
  workflow = wf,
  tasks = list(task_upload)
)

task_comparison <- jobmonr::task(
  task_template = template_comparison,
  name = paste0("comparison_5q0_", version_id),
  upstream_tasks = list(task_upload),
  max_attempts = 3,
  code_path = glue::glue("{code_dir}/12_mortality_5q0_data_comparison.py"),
  conda_path = conda_path,
  conda_env = conda_env,
  script_path = glue::glue("{code_dir}/12_mortality_5q0_data_comparison.sh"),
  version_id = version_id,
  gbd_year = gbd_year,
  end_year = end_year,
  code_dir = code_dir,
  compute_resources = list(
    "memory" = "10G",
    "cores" = 5L,
    "queue" = queue,
    "constraints" = "archive",
    "runtime" = "10000S"
  )
)

wf <- jobmonr::add_tasks(
  workflow = wf,
  tasks = list(task_comparison)
)

task_graph <- jobmonr::task(
  task_template = template_graph,
  name = paste0("graph_5q0_", version_id),
  upstream_tasks = list(task_comparison),
  max_attempts = 3,
  shell_path = shell_path_r,
  image_path = image_path,
  script_path = glue::glue("{code_dir}/13_graph_5q0_compare_with_previous.R"),
  version_id = version_id,
  gbd_year = gbd_year,
  end_year = end_year,
  compute_resources = list(
    "memory" = "30G",
    "cores" = 5L,
    "queue" = queue,
    "constraints" = "archive",
    "runtime" = "10000S"
  )
)

wf <- jobmonr::add_tasks(
  workflow = wf,
  tasks = list(task_graph)
)

task_diagnostic_graph <- jobmonr::task(
  task_template = template_diagnostic_graph,
  name = paste0("diagnostic_graph_5q0_", version_id),
  upstream_tasks = list(task_graph),
  max_attempts = 3,
  shell_path = shell_path_r,
  image_path = image_path,
  script_path = glue::glue("{code_dir}/15_diagnostic_graph.R"),
  version_id = version_id,
  gbd_year = gbd_year,
  end_year = end_year,
  compute_resources = list(
    "memory" = "30G",
    "cores" = 5L,
    "queue" = queue,
    "constraints" = "archive",
    "runtime" = "10000S"
  )
)

wf <- jobmonr::add_tasks(
  workflow = wf,
  tasks = list(task_diagnostic_graph)
)

task_compile_graph <- jobmonr::task(
  task_template = template_compile_graph,
  name = paste0("compile_graph_5q0_", version_id),
  upstream_tasks = list(task_diagnostic_graph),
  max_attempts = 3,
  code_path = glue::glue("{code_dir}/16_pdf_compilation.py"),
  conda_path = conda_path,
  conda_env = conda_env,
  script_path = glue::glue("{code_dir}/16_pdf_compilation.sh"),
  version_id = version_id,
  gbd_year = gbd_year,
  compute_resources = list(
    "memory" = "10G",
    "cores" = 2L,
    "queue" = queue,
    "runtime" = "1000S"
  )
)

wf <- jobmonr::add_tasks(
  workflow = wf,
  tasks = list(task_compile_graph)
)

## Run workflow ----------------------------------------------------------------

wfr <- wf$run(resume = TRUE)

## Upload to database or report model failure ----------------------------------

if (wfr == "D") {
  
  print("5q0 model completed!")
  
} else {
  
  mortdb::send_slack_message(
    message = paste0("5q0 model ", version_id, " failed."),
    channel = paste0("@", username),
    icon = ":thinking_face:",
    botname = "5q0ModelBot"
  )
  
  # Stop process
  stop("5q0 model failed!")
  
}