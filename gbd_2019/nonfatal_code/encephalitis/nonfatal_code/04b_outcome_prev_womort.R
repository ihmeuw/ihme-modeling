#####################################################################################################################################################################################
## Purpose:		This step template should be submitted from the 00_master.do file either by submitting all steps or selecting one or more steps to run in "steps" global
## Author:		
## Last updated:	2019
## Description:	Convert etiology-specific outcome incidence to prevalence directly (for outcome without mort risk only), saves unsqueezed results for vision and epilepsy
## 				Number of output files: 37728
#####################################################################################################################################################################################
rm(list=ls())

# LOAD SETTINGS FROM MASTER CODE (NO NEED TO EDIT THIS SECTION) ----------------
# Load functions and packages
library(argparse)
library(data.table)

# Get arguments from parser
parser <- ArgumentParser()
parser$add_argument("--root_j_dir", help = "base directory on J", default = NULL, type = "character")
parser$add_argument("--root_tmp_dir", help = "base directory on clustertmp", default = NULL, type = "character")
parser$add_argument("--date", help = "timestamp of current run (i.e. 2014_01_17)", default = NULL, type = "character")
parser$add_argument("--step_num", help = "step number of this step (i.e. 01a)", default = NULL, type = "character")
parser$add_argument("--step_name", help = "name of current step (i.e. first_step_name)", default = NULL, type = "character")
parser$add_argument("--hold_steps", help = "steps to wait for before running", default = NULL, nargs = "+", type = "character")
parser$add_argument("--last_steps", help = "step numbers for final steps that you are running in the current run (i.e. 04b)", default = NULL,  nargs = "+", type = "character")
parser$add_argument("--code_dir", help = "code directory", default = NULL, type = "character")
parser$add_argument("--in_dir", help = "directory for external inputs", default = NULL, type = "character")
parser$add_argument("--ds", help = "specify decomp step", default = 'step1', type = "character")
parser$add_argument("--new_cluster", help = "specify which cluster (0 - old cluster, 1 - new cluster)", default = 0, type = "integer")

args <- parser$parse_args()
print(args)
list2env(args, environment()); rm(args)

# Source helper functions
source(paste0(code_dir, "helper_functions/qsub_new.R"))
source(paste0(code_dir, "helper_functions/argparser.R"))
source(paste0(code_dir, "helper_functions/source_functions.R"))

# directory for output on the J drive
out_dir <- paste0(root_j_dir, "/", step_num, "_", step_name)
# directory for output on clustertmp
tmp_dir <- paste0(root_tmp_dir, "/",step_num, "_", step_name)
# ------------------------------------------------------------------------------

# User specified options -------------------------------------------------------
# Set to 1 if you want to run the parallelized portion of this job
run_outcome_prev_womort <- 1
# Set to 1 if you want to save results to dismod
save_results_vision <- 0
save_results_epilepsy <- 1

# Source GBD 2019 Shared Functions
k <- # filepath
sourceDir(paste0(k, "current/r/"))

# Shell File
shell_file <- paste0(code_dir, "r_shell.sh") # Should be updated to modeler's preferred R shell file

# pull locations from CSV created in model_custom
locations <- readRDS(file.path(in_dir,"locations_temp.rds"))

functional <- "encephalitis"
grouping <- c("long_mild", "_vision")

# meids
meid_vision_encephalitis <- 9461
meid_epilepsy_encephalitis <- 2821
# ------------------------------------------------------------------------------

# Check files before submitting job --------------------------------------------
# Check for finished.txt from steps that this script was supposed to wait to be finished before running
if (!is.null(hold_steps)) {
  for (i in hold_steps) {
    sub.dir <- list.dirs(path=root_j_dir, recursive=F)
    files <- list.files(path=file.path(root_j_dir, grep(i, sub.dir, value=T)), pattern = 'finished.txt')
    if (is.null(files)) {
      stop(paste(dir, "Error"))
    }
  }
}

if (run_outcome_prev_womort == 1) {
  # Deletes step finished.txt
  if(file.exists(file.path(out_dir,"finished.txt"))) {
    file.remove(file.path(out_dir,"finished.txt"))
  }
  # Delete finished_{location_id}.txt
  check_dir <- file.path(tmp_dir,"02_temp/01_code/checks")
  datafiles <- list.files(check_dir, pattern='finished.*.txt')
  for (df in datafiles) {
    file.remove(file.path(check_dir, df))
  }
  # ------------------------------------------------------------------------------
  
  # Submit array job -------------------------------------------------------------
  # Save the parameters as a csv so then you can index the rows to find the appropriate parameters
  param_map <- expand.grid(location_id = locations)
  write.csv(param_map, file.path(code_dir, paste0(step_num, "_parameters.csv")), row.names=F)
  n_jobs <- nrow(param_map)
  
  # Submit job
  my_job_name <- paste0("child_", step_num)
  print(paste("submitting", my_job_name))
  my_project <- "proj_hiv"
  my_tasks <- paste0("1:", n_jobs)
  my_slots <- 4
  parallel_script <- paste0(code_dir, "/", step_num, "_parallel.R")
  my_args <- argparser(root_j_dir = root_j_dir,
                       root_tmp_dir = root_tmp_dir,
                       date = date,
                       step_num = step_num,
                       step_name = step_name,
                       code_dir = code_dir,
                       in_dir = in_dir,
                       out_dir = out_dir,
                       tmp_dir = tmp_dir,
                       ds = ds)
  
  qsub_new(job_name = my_job_name,
           project = my_project,
           tasks = my_tasks,
           fthread = my_slots,
           mem = "1G",
           time = "0:10:00",
           shell_file = shell_file,
           script = parallel_script,
           args = my_args)
  # ------------------------------------------------------------------------------
  
  # CHECK FILES (NO NEED TO EDIT THIS SECTION) -----------------------------------
  Sys.sleep(60)
  # Wait for jobs to finish before passing execution back to main step file
  i <- 0
  while (i == 0) {
    checks <- list.files(file.path(tmp_dir), pattern='finished.*.txt')
    count <- length(checks)
    print(paste("checking ", Sys.time(), " ", count, "of ", n_jobs, " jobs finished"))
    if (count == n_jobs) i <- i + 1 else Sys.sleep(60)
  }
  print(paste(step_num, "DisMod upload files completed (but not yet uploaded)--this is referring to save results, not the epi-uploader"))
}

# Save results -----------------------------------------------------------------
#' @Note specify save_results in the user specified options section
# (no need to parallelize, it's only 2 submission, only for vision and epilepsy)
if (save_results_vision == 1) {
  # Save results for unsqueezed vision
  save_results_epi(input_dir = file.path(tmp_dir, "03_outputs", "01_draws", "_vision", "_unsqueezed"),
                   input_file_pattern = "{measure_id}_{location_id}_{year_id}_{sex_id}.csv",
                   modelable_entity_id = meid_vision_encephalitis,
                   measure_id = 5,
                   gbd_round_id = 6,
                   sex_id = c(1,2),
                   description = paste("Custom", functional, date, "unsqueezed - custom code results for", ds),
                   decomp_step = ds,
                   mark_best = T)
  print(paste(step_num, "save_results vision files uploaded"))
}
if (save_results_epilepsy == 1) {
  # Save results for unsqueezed epilepsy
  save_results_epi(input_dir = file.path(root_tmp_dir, "04a_dismod_prep_wmort", "04_ODE", "save_results_epilepsy", "encephalitis"),
                   input_file_pattern = "{location_id}.csv",
                   modelable_entity_id = meid_epilepsy_encephalitis,
                   measure_id = 5,
                   gbd_round_id = 6,
                   sex_id = c(1,2),
                   description = paste("Custom", functional, date, "unsqueezed - custom code results for", ds),
                   decomp_step = ds,
                   mark_best = T)

  print(paste(step_num, "save_results epilepsy files uploaded"))
}
# ------------------------------------------------------------------------------

# CHECK FILES (NO NEED TO EDIT THIS SECTION) -----------------------------------
# Write check file to indicate step has finished
file.create(file.path(out_dir,"finished.txt"), overwrite=T)
