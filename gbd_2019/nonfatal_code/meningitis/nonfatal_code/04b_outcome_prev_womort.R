#####################################################################################################################################################################################
## Purpose:		This step template should be submitted from the 00_master.do file either by submitting all steps or selecting one or more steps to run in "steps" global
## Author:		
## Last updated:	9:32 AM 8/20/2014
## Description:	Convert etiology-specific outcome incidence to prevalence directly (for outcome without mort risk only), saves unsqueezed results for hearing and vision
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

# User specified options -------------------------------------------------------
# Set to 1 if you want to run the parallelized portion of this job
run_outcome_prev_womort <- 0
# Set to 1 if you want to save results
save_results <- 1
# specify which sequelae to upload to epi_viz	(upload: 1; do not upload: 0)
upload_vision  <- 0  # unsqueezed vision impairment
upload_hearing <- 0	 # unsqueezed hearing impairment
upload_epilepsy <- 1 # epilepsy

# Source GBD 2019 Shared Functions
sourceDir(paste0(k, "current/r/"))

# Shell File
shell_file <- paste0(code_dir, "r_shell.sh") # Should be updated to modeler's preferred R shell file

# pull locations from CSV created in model_custom
locations <- readRDS(file.path(in_dir,"locations_temp.rds"))

grouping <- c("_hearing", "long_mild", "_vision")

if (run_outcome_prev_womort == 1) {
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
  # Delete finished_save_{meid}.txt
  check_dir <- file.path(tmp_dir,"02_temp/01_code/checks")
  datafiles <- list.files(check_dir, pattern='finished_save_*.txt')
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
  my_slots <- 1
  my_mem <- "1G"
  my_time <- "02:00:00"
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
  
  qsub_new(
    job_name = my_job_name,
    project = my_project,
    tasks = my_tasks,
    mem = my_mem,
    fthread = my_slots,
    time = my_time,
    shell_file = shell_file,
    script = parallel_script,
    args = my_args
  )
  
  # CHECK FILES (NO NEED TO EDIT THIS SECTION) -----------------------------------
  Sys.sleep(60)
  # Wait for jobs to finish before passing execution back to main step file
  i <- 0
  while (i == 0) {
    checks <- list.files(file.path(tmp_dir, "02_temp/01_code/checks"), pattern='finished.*.txt')
    count <- length(checks)
    print(paste("checking ", Sys.time(), " ", count, "of ", n_jobs, " jobs finished"))
    if (count == n_jobs) i <- i + 1 else Sys.sleep(60)
  }
  Sys.sleep(120)
  
  print(paste(step_num, "DisMod upload files completed (but not yet uploaded)"))
}

# Save results -----------------------------------------------------------------
#' @Note: specify save_results in the user specified options section
# Upload hearing file to DisMod so that the outputs can be used for hearing/vision envelop squeeze (for _vision, do not use 'unsqueezed' sequelae anymore to avoid confusion in central squeeze)
if (save_results == 1) {
  dim.dt <- fread(file.path(in_dir, paste0(step_num, '_', step_name), 'meningitis_dimension.csv'))
  
  dim.dt <- dim.dt[healthstate %in% c("epilepsy_any", "_unsqueezed", "_parent")]
  dim.dt <- dim.dt[grouping %in% c("_epilepsy", "_vision", "_hearing")]
  
  # filter based on which groups you want to upload (see user specified options)
  for (g in c("_hearing", "_vision", "_epilepsy")) {
    if (get(paste0("upload", g)) == 0){
      dim.dt <- dim.dt[grouping != g]
    }
  }
  
  n <-  0
  
  # Erase and make directory for finished checks
  check_dir <- file.path(tmp_dir, "02_temp", "01_code", "checks")
  dir.create(check_dir, showWarnings = F)
  files <- list.files(check_dir, pattern = "finished_save.*.txt")
  for(f in files){
    file.remove(file.path(check_dir, f),showWarnings=F)
  }
  
  groupings <- unique(dim.dt[, grouping]) # hearing and vision and epilepsy
  for(g in groupings){
    healthstates <- unique(dim.dt[grouping == g, healthstate])
    for(h in healthstates){
      # Submit job
      my_job_name <- paste0("save_results_", g, '_', h)
      print(paste("submitting", my_job_name))
      my_project <- "proj_hiv"
      my_slots <- 10
      my_mem <- "50G"
      my_time <- "05:00:00"
      parallel_script <- paste0(code_dir, step_num, "_save_results.R")
      my_args <- argparser(
          root_j_dir = root_j_dir,
          root_tmp_dir = root_tmp_dir,
          date = date,
          step_num = step_num,
          step_name = step_name,
          code_dir = code_dir,
          in_dir = in_dir,
          group = g, 
          healthstate = h, 
          ds = ds
      )
      qsub_new(
        job_name = my_job_name,
        project = my_project,
        mem = my_mem,
        fthread = my_slots,
        time = my_time,
        shell_file = shell_file,
        script = parallel_script,
        args = my_args
      )
      
      n <- n + 1
    }
  }
  
  Sys.sleep(120)
  
  # Wait for jobs to finish before passing execution back to main step file
  i <- 0
  while (i == 0) {
    files <- list.files(check_dir, pattern = "finished_save.*.txt")
    count <- length(files)
    print(paste("checking ", Sys.time(), " ", count, "of ", n, " jobs finished"))
    if (count == n) i <- i + 1 else Sys.sleep(60) 
  }
  
  print(paste(step_num, "save_results files uploaded"))
}

# CHECK FILES (NO NEED TO EDIT THIS SECTION) -----------------------------------
# Write check file to indicate step has finished
file.create(file.path(out_dir,"finished.txt"), overwrite=T)
