################################################################################
##                                                                            ##
## Demographics SRB Model Submit                                              ##
##                                                                            ##
## Steps:                                                                     ##
##   A. Read in configuration files                                           ##
##   B. Create Jobmon workflow, task templates, and tasks                     ##
##   C. Run Jobmon                                                            ##
##                                                                            ##
################################################################################

library(argparse)
library(data.table)
library(jobmonr)
library(stringr)

## A. Read in configuration files ----------------------------------------------

parser <- argparse::ArgumentParser()
parser$add_argument(
  "--main_dir", type = "character", required = !interactive(),
  help = "base directory for this version run"
)
args <- parser$parse_args()
list2env(args, .GlobalEnv)

if (interactive()) {
  version_id <- "Run id"
  main_dir <- paste0("FILEPATH", version_id)
  if (!fs::dir_exists(main_dir)) stop("specify valid 'main_dir'")
}

# Get config
config <- config::get(
  file = fs::path(main_dir, "srb_detailed.yml"), use_parent = FALSE)
list2env(config, .GlobalEnv)

loc_map <- fread(fs::path(input_dir, "loc_map.csv"))

# jobnames
jname <- c(
  "setup",
  "lmer_st",
  "gpr_array",
  "raking_array",
  "upload",
  "prep_graph",
  "graph_array",
  "append_graphs"
)
jname <- paste0(jname, "_", version_id)

## B. Create Jobmon workflow, task templates, and tasks ------------------------

# Create new workflow

wf_tool <- jobmonr::tool(name = "SRB_tool")

jobmonr::set_default_tool_resources(
  tool = wf_tool,
  default_cluster_name = jobmon_cluster_name,
  resources = list("project" = submission_project_name)
)

wf <- jobmonr::workflow(
  tool = wf_tool,
  workflow_args = paste0("SRB_Model_", format(Sys.time(), "%Y-%m-%d-%H-%M"))
)

# Create task templates
template_r <- jobmonr::task_template(
  tool = wf_tool,
  template_name = "template_r",
  command_template = paste(
    "{shell_path} -i {image_path} -s {script_path} --main_dir {main_dir}"
  ),
  node_args = list("script_path"),
  op_args = list("shell_path"),
  task_args = list("image_path", "main_dir")
)

template_py <- jobmonr::task_template(
  tool = wf_tool,
  template_name = "template_py",
  command_template = paste(
    "PYTHONPATH= {shell_path} -s {script_path}  --main_dir {main_dir}",
    "--location_id {location_id} --ihme_loc_id {ihme_loc_id}"
  ),
  node_args = list("script_path", "location_id", "ihme_loc_id"),
  op_args = list("shell_path"),
  task_args = list("main_dir")
)

template_r_array <- jobmonr::task_template(
  tool = wf_tool,
  template_name = "template_r_array",
  command_template = paste(
    "{shell_path} -i {image_path} -s {script_path} --location_id {location_id} --main_dir {main_dir}"),
  node_args = list("location_id", "script_path"),
  op_args = list("shell_path"),
  task_args = list("image_path", "main_dir")
)

# Create tasks

task_setup_save_inputs_srb <- list(jobmonr::task(
  task_template = template_r,
  name = jname[1],
  upstream_tasks = list(),
  shell_path = shell_r,
  image_path = image_r,
  script_path = fs::path(clone_dir, "FILEPATH/01_setup_save_inputs_srb.R"),
  main_dir = main_dir,
  compute_resources = list(
    "memory" = "1G",
    "cores" = 1L,
    "queue" = queue,
    "runtime" = "20m")
))

wf <- jobmonr::add_tasks(
  workflow = wf,
  tasks = task_setup_save_inputs_srb
)

task_LMER_ST_srb <- list(jobmonr::task(
  task_template = template_r,
  name = jname[2],
  upstream_tasks = task_setup_save_inputs_srb,
  shell_path = shell_r,
  image_path = image_r,
  script_path = fs::path(clone_dir, "FILEPATH/02_LMER_ST_srb.R"),
  main_dir = main_dir,
  compute_resources = list(
    "memory" = "5G",
    "cores" = 1L,
    "queue" = queue,
    "runtime" = "20m")
))

wf <- jobmonr::add_tasks(
  workflow = wf,
  tasks = task_LMER_ST_srb
)

task_srb_gpr <- lapply(unique(loc_map$location_id), function(i){
  task_srb_gpr <- jobmonr::task(
    task_template = template_py,
    name = jname[3],
    upstream_tasks = task_LMER_ST_srb,
    shell_path = path_to_shell_py,
    script_path = fs::path(clone_dir, "FILEPATH/03_srb_gpr.py"),
    location_id = i,
    ihme_loc_id = loc_map[location_id == i]$ihme_loc_id,
    main_dir = main_dir,
    compute_resources = list(
      "memory" = "1G",
      "cores" = 1L,
      "queue" = queue,
      "runtime" = "24h")
  )
  return(task_srb_gpr)
})

wf <- jobmonr::add_tasks(
  workflow = wf,
  tasks = task_srb_gpr
)

task_raking_srb <- lapply(unique(parent_ids), function(i){
  task_raking_srb <- jobmonr::task(
    task_template = template_r_array,
    name = jname[4],
    upstream_tasks = task_srb_gpr,
    shell_path = shell_r,
    image_path = image_r,
    script_path = fs::path(clone_dir, "FILEPATH/04_raking_srb.R"),
    location_id = i, 
    main_dir = main_dir,
    compute_resources = list(
      "memory" = "10G",
      "cores" = 1L,
      "queue" = queue,
      "runtime" = "24h")
  )
  return(task_raking_srb)
})

wf <- jobmonr::add_tasks(
  workflow = wf,
  tasks = task_raking_srb
)

task_upload_srb <- list(jobmonr::task(
  task_template = template_r,
  name = jname[5],
  upstream_tasks = task_raking_srb,
  shell_path = shell_r,
  image_path = image_r,
  script_path = fs::path(clone_dir, "FILEPATH/05_upload_srb.R"),
  main_dir = main_dir,
  compute_resources = list(
    "memory" = "1G",
    "cores" = 1L,
    "queue" = queue,
    "runtime" = "30m"
  )))

wf <- jobmonr::add_tasks(
  workflow = wf,
  tasks = task_upload_srb
)

task_graph_prep_srb <- list(jobmonr::task(
  task_template = template_r,
  name = jname[6],
  upstream_tasks = task_upload_srb,
  shell_path = shell_r,
  image_path = image_r,
  script_path = fs::path(clone_dir, "FILEPATH/06_graph_prep_srb.R"),
  main_dir = main_dir,
  compute_resources = list(
    "memory" = "1G",
    "cores" = 1L,
    "queue" = queue,
    "runtime" = "30m")
))

wf <- jobmonr::add_tasks(
  workflow = wf,
  tasks = task_graph_prep_srb
)

task_graph_loc_srb <- lapply(unique(loc_map$location_id), function(i){
  task_graph_loc_srb <- jobmonr::task(
    task_template = template_r_array,
    name = jname[7],
    upstream_tasks = task_graph_prep_srb,
    shell_path = shell_r,
    image_path = image_r,
    script_path = fs::path(clone_dir, "FILEPATH/07_graph_loc_srb.R"),
    location_id = i,
    main_dir = main_dir,
    compute_resources = list(
      "memory" = "5G",
      "cores" = 1L,
      "queue" = queue,
      "runtime" = "24h")
  )
  return(task_graph_loc_srb)
})

wf <- jobmonr::add_tasks(
  workflow = wf,
  tasks = task_graph_loc_srb
)

task_append_srb_graphs <- list(jobmonr::task(
  task_template = template_r,
  name = jname[8],
  upstream_tasks = task_graph_loc_srb,
  shell_path = shell_r,
  image_path = image_r,
  script_path = fs::path(clone_dir, "FILEPATH/08_append_srb_graphs.R"),
  main_dir = main_dir,
  compute_resources = list(
    "memory" = "5G",
    "cores" = 1L,
    "queue" = queue,
    "runtime" = "30m")
))

wf <- jobmonr::add_tasks(
  workflow = wf,
  tasks = task_append_srb_graphs
)

## C. Run Jobmon ---------------------------------------------------------------

wfr <- wf$run(resume = TRUE)
