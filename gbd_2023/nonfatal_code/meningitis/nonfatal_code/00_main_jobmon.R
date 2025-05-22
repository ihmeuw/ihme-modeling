rm(list=ls())

# Load libraries and source shared function
library(jobmonr)
pacman::p_load(openxlsx, data.table, purrr)
source("FILEPATH")

## Define the date
date <- gsub("-", "_", Sys.Date())
user <- Sys.info()["user"]

## Set directories
cause <- "meningitis"
code_dir <- paste0("FILEPATH")
in_dir <- paste0("FILEPATH")
out_dir <- paste0("FILEPATH")
tmp_dir <- paste0("FILEPATH")
rshell <- "FILEPATH"
py_env <- "FILEPATH"

dir.create(out_dir, showWarnings = FALSE, recursive=TRUE); dir.create(tmp_dir, showWarnings = FALSE, recursive=TRUE)

## User-specified inputs
release <- 16L # MUST be an integer, not numeric
paf_release <- 16L #This should be the same as "release". 
locations <- get_location_metadata(location_set_id = 9, release_id = release)[most_detailed == 1 & is_estimate == 1]$location_id 
# locations <- 33L # MUST be an integer, not numeric
jobmon_length <- 60*60*6 # 6 hour

# Steps to run
steps <- c("09a") # steps <- c("01a", "01b", "02a", "02b", "03a", "03b", "04a", "04b", "04c", "04d", "04e", "05a", "05b", "06a", "09a") 

# Get upload parameters
dim.dt <- fread(file.path("FILEPATH"))
dim.dt <- dim.dt[healthstate != "_parent"] 
meids_upload <- unique(dim.dt$modelable_entity_id) # healthstates_upload <- "epilepsy_any"
#meids_upload <- c(24189L, 24049L) 
meids_upload <- c(24068L)
# Etiologies
etiologies <- c("bacterial", "viral")

# Load steps template
step_sheet <- as.data.table(read.xlsx(file.path("FILEPATH"), sheet="steps"))

# Create functions

# Function to remove null arguments 
null_remover <- function(func, ...) {
        dots <- list(...)
        dots <-  compact(dots)
        dots <- dots[dots != ""] # Remove empty elements of length 1 in addition to elements length 0
        return(do.call(func, dots))
}

# Function to make a Jobmon task template that fits these scripts
make_template <- function(name, my_tool = my_tool, node_args = node_args, task_args = list("out_dir", "tmp_dir", "date", "step_num", "step_name", "code_dir", "in_dir", "release", "paf_release", "cause")) {
  # Create a command string for node_args
  node_command <- do.call(paste, lapply(node_args, function(n){paste0("--", n, " {", n, "}")}))
  # Create a command string for task_args
  task_command <- do.call(paste, lapply(task_args, function(n){paste0("--", n, " {", n, "}")}))
  # Create the task template
  new_template <- task_template(
  tool = my_tool,
  template_name = name,
  command_template = paste("OMP_NUM_THREADS=1 PYTHONPATH= {rshell} -s {script}", task_command, node_command),
  task_args = task_args,
  node_args = node_args,
  op_args = list("rshell", "script")
  )
  return(new_template)
}

my_tool <- tool(name="custom_nonfatal")
my_tool <- set_default_tool_resources(my_tool,
                                      default_cluster_name='slurm',
                                      resources = list(
                                        "memory" = "1G",
                                        "cores" = 2L,
                                        "runtime" = "00:30:00",
                                        "constraints" = "archive",
                                        "stdout" = paste0("/FILEPATH"),
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
  template_list[[i]] <- make_template(name = paste0("step_", s, "_template"), my_tool = my_tool, node_args = n)
}

task_list <- list()
for(i in 1:length(steps)){
  step_num <- steps[i]
  step_row <- step_sheet[step == step_num]
  if (step_row$parallel_by %like% "location") loc_loop <- locations else loc_loop <- ""
  # MEIDs for UPLOAD ONLY
  if (step_row$parallel_by %like% "meid") meid_loop <- meids_upload else meid_loop <- ""
  # ETIOLOGIES (bacterial or viral?)
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
                                       rshell = ifelse(step_row$language == "py", py_env, rshell), 
                                       script = paste0(code_dir, step_num, "_", step_row$name, ".", step_row$language),
                                       step_num = step_num,
                                       step_name = step_row$name,
                                       date = date,
                                       code_dir = code_dir,
                                       in_dir = in_dir,
                                       out_dir = out_dir,
                                       tmp_dir = tmp_dir,
                                       release = release,
                                       paf_release = paf_release,
  									                   cause = cause,
                                       location = l,
                                       meid = m,
                                       etiology = e)
      }
    }
  } 
}

# Remove names from the task list so that python works!
task_list <- unname(task_list)

## Name your workflow version
workflow_version <- gsub(" ", "-", as.character(Sys.time()))

## Launch it! 
workflow <- workflow(tool = my_tool,
                     name = cause,
                     workflow_args = workflow_version)

my_workflow <- add_tasks(
  workflow = workflow,
  tasks = task_list
)

status <- run(workflow = workflow, resume = FALSE, seconds_until_timeout = jobmon_length)
