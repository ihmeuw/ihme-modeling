rm(list=ls())

# Load libraries and source shared function
library(jobmonr)
pacman::p_load(openxlsx, data.table, purrr)
source("FILEPATH")

## Define the date
date <- "2022_07_19" # gsub("-", "_", Sys.Date())
user <- Sys.info()["user"]

## Set directories
out_dir <- paste0("FILEPATH")
code_dir <- paste0("FILEPATH")
rshell <- "FILEPATH"
in_dir <- "FILEPATH"

dir.create(out_dir, showWarnings = T, recursive=TRUE)

## User-specified inputs
gbd_round <- 7L # MUST be an integer, not numeric
ds <- "iterative"
locations <- get_location_metadata(location_set_id = 9, gbd_round_id = gbd_round, decomp_step = ds)[most_detailed == 1 & is_estimate == 1]$location_id 
# locations <- 33L # MUST be an integer, not numeric
jobmon_length <- 60*60*6 # 6 hour
desc <- "AMR_add_meningococcal_shock"

# Steps to run
steps <- c("04") # steps <- c("01", "02", "03", "04") 

# Get upload parameters
dim.dt <- fread(file.path(code_dir, paste0("etio_dimension.csv")))
meids_upload <- unique(dim.dt$me_id)
etiologies <- unique(dim.dt$pathogen)

# Load steps template
step_sheet <- as.data.table(read.xlsx(file.path(code_dir,paste0("etio_steps.xlsx")), sheet="steps"))

# Create functions

# Function to remove null arguments 
null_remover <- function(func, ...) {
        dots <- list(...)
        dots <-  compact(dots)
        dots <- dots[dots != ""] # Remove empty elements of length 1 in addition to elements length 0
        print(dots)
        return(do.call(func, dots))
}

# Function to make a Jobmon task template that fits these scripts
make_template <- function(name, my_tool = my_tool, rshell = rshell, node_args = node_args, task_args = list("out_dir", "code_dir", "in_dir", "ds", "gbd_round", "desc")) {
  # Create a command string for node_args
  node_command <- do.call(paste, lapply(node_args, function(n){paste0("--", n, " {", n, "}")}))
  # Create a command string for task_args
  task_command <- do.call(paste, lapply(task_args, function(n){paste0("--", n, " {", n, "}")}))
  # Create the task template
  new_template <- task_template(
  tool = my_tool,
  template_name = name,
  command_template = paste("OMP_NUM_THREADS=1 PYTHONPATH=", rshell, "-s {script}", task_command, node_command),
  task_args = task_args,
  node_args = node_args,
  op_args = list("script")
  )
  return(new_template)
}

my_tool <- tool(name="paf_new")
my_tool <- set_default_tool_resources(my_tool,
                                      default_cluster_name='slurm',
                                      resources = list(
                                        "memory" = "1G",
                                        "cores" = 2L,
                                        "runtime" = "00:30:00",
                                        "constraints" = "archive",
                                        "stdout" = paste0("FILEPATH"),
                                        "stderr" = paste0("FILEPATH"),
                                        "project" = "proj_tb",
                                        "queue" = "long.q"
                                      ))

# Make a list of templates that are pulled later
template_list <- list()
for(i in 1:length(steps)){
  s <- steps[i]
  step_row <- step_sheet[step == s]
  n <- as.list(unlist(strsplit(step_row$parallel_by, ",")))
  if (is.na(n[[1]])) n <- list()
  template_list[[i]] <- make_template(name = paste0("step_", s, "_template"), my_tool = my_tool, node_args = n, rshell = rshell)
}

task_list <- list()
for(i in 1:length(steps)){
  step_num <- steps[i]
  step_row <- step_sheet[step == step_num]
  if (step_row$parallel_by %like% "location") loc_loop <- locations else loc_loop <- ""
  # MEIDs for UPLOAD ONLY
  if (step_row$parallel_by %like% "meid") meid_loop <- meids_upload else meid_loop <- ""
  # ETIOLOGIES
  if (step_row$parallel_by %like% "etiology") etio_loop <- etiologies else etio_loop <- ""
    for (l in loc_loop){
      for(m in meid_loop){
        for (e in etio_loop){
          # Set upstreams - any task from the preceding step_
          if(i==1) upstreams <- list() else upstreams <- task_list[which(names(task_list) %like% steps[i-1])]
          task_list[[paste("step", step_num, l, m, e, sep = "_")]] <- null_remover(task,
                                       task_template = template_list[[i]],
                                       compute_resources = list(
                                         "memory" = step_row$memory,
                                         "cores" = as.integer(step_row$cores),
                                         "runtime" = step_row$runtime),
                                       max_attempts = 2,
                                       upstream_tasks = upstreams,
                                       name = paste("step", step_num, e, l, sep = "_"),
                                       script = paste0(code_dir, step_num, "_", step_row$name, ".", step_row$language),
                                       in_dir = in_dir,
                                       out_dir = out_dir,
                                       code_dir = code_dir,
                                       ds = ds,
                                       gbd_round = gbd_round,
                                       desc = desc,
                                       location = l,
                                       meid = m,
                                       etiology = e)
      }
    }
  } 
}

# Remove names from the task list so that python works
task_list <- unname(task_list)

## Name your workflow version
workflow_version <- gsub(" ", "-", as.character(Sys.time()))

## Launch it 
workflow <- workflow(tool = my_tool,
                     name = "paf_workflow",
                     workflow_args = workflow_version)

my_workflow <- add_tasks(
  workflow = workflow,
  tasks = task_list
)

status <- run(workflow = workflow, resume = FALSE, seconds_until_timeout = jobmon_length)

