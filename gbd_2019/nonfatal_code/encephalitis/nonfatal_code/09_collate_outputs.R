#####################################################################################################################################################################################
## Purpose:		This step template should be submitted from the 00_master.do file either by submitting all steps or selecting one or more steps to run in "steps" global
## Author:		
## Last updated:	2019/03/08
## Description:	Collate all encephalitis sequelae outputs in one place for transferring to COMO
## Number of output files: 166632 (154056 if without seizure)
## Note: When uploading to DisMod, use:
##         save_results, sequela_id(ID) subnational(yes) description(TEXT DESCRIPTION)
##                       in_dir(FILEPATH)
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
source(paste0(code_dir, "helper_functions/qsub.R"))
source(paste0(code_dir, "helper_functions/qsub_new.R"))
source(paste0(code_dir, "helper_functions/argparser.R"))
source(paste0(code_dir, "helper_functions/source_functions.R"))

# directory for output on the J drive
out_dir <- paste0(root_j_dir, "/", step_num, "_", step_name)
# directory for output on clustertmp
tmp_dir <- paste0(root_tmp_dir, "/",step_num, "_", step_name)
# ------------------------------------------------------------------------------

# User specified options -------------------------------------------------------
# specify which sequelae to upload to epi_viz	(upload: 1; do not upload: 0)
upload_long_mild   <- 1	 # long term sequelae without mortality risk
upload_long_modsev <- 1	 # long term sequelae with mortality risk
# Source GBD 2019 Shared Functions
j <- # filepath
sourceDir(j)
# Shell File
shell_file <- paste0(code_dir, "r_shell.sh") # Should be updated to modeler's preferred R shell file

functional <- "encephalitis"
measure <- 5
# ------------------------------------------------------------------------------

# Inputs -----------------------------------------------------------------------
# directories for pulling files from the previous steps
pull_dir_05b <- file.path(root_tmp_dir, "05b_sequela_split_woseiz", "03_outputs", "01_draws")
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
# Deletes step finished.txt
if(file.exists(file.path(out_dir,"finished.txt"))) {
  file.remove(file.path(out_dir,"finished.txt"))
}
# Delete finished_{location_id}.txt
check_dir <- file.path(tmp_dir)
datafiles <- list.files(check_dir, pattern='finished.*.txt')
for (df in datafiles) {
  file.remove(file.path(check_dir, df))
}
# ------------------------------------------------------------------------------


# Submit jobs for checks -------------------------------------------------------

dim.dt <- fread(file.path(in_dir, paste0(step_num, "_", step_name), "encephalitis_dimension.csv"))
dim.dt <- dim.dt[healthstate != "_parent" & !(grouping %in% c("_epilepsy", "cases", "_vision"))] # GBD 2017 code also removed "long_mild" grouping

dim.dt[grouping %in% c("long_mild", "long_modsev"), num:= "05b"]
dim.dt[grouping %in% c("long_mild", "long_modsev"), name:= "outcome_prev_womort"]

# filter based on which groups you want to upload (see user specified options)
for (g in c("long_mild", "long_modsev")) {
  if (get(paste0("upload_", g)) == 0){
    dim.dt <- dim.dt[grouping != g]
  }
}

type <- "check"
## check to see if all these files exist, and move them to upload files
n <- 0
acause <- unique(dim.dt$acause)
for (cause in acause) {
  grouping <- unique(dim.dt[acause == cause, grouping])
  for (group in grouping) {
    healthstate <- unique(dim.dt[acause == cause & grouping == group, healthstate])
    for(state in healthstate) {
      dir.create(file.path(tmp_dir, "03_outputs", "01_draws", cause, group, state), showWarnings = F, recursive = T)
      
      my_job_name <- paste(type, cause, group, state, step_num, sep = "_")
      print(paste("submitting", my_job_name))
      my_project <- "proj_hiv"
      my_queue <- "all.q"
      my_mem <- "5G"
      my_fthread <- 1
      my_time <- "05:00:00"
      parallel_script <- paste0(code_dir, step_num, "_checks.R")
      
      my_args <- argparser(
        root_j_dir   = root_j_dir,
        root_tmp_dir = root_tmp_dir,
        date         = date,
        step_num     = step_num,
        step_name    = step_name,
        code_dir     = code_dir,
        in_dir       = in_dir,
        out_dir      = out_dir,
        tmp_dir      = tmp_dir,
        etiology     = cause,
        group        = group,
        healthstate  = state
      )
      
      qsub_new(
        job_name   = my_job_name,
        project    = my_project,
        queue      = my_queue,
        mem        = my_mem,
        fthread    = my_fthread,
        time       = my_time,
        shell_file = shell_file,
        script     = parallel_script,
        args       = my_args
      )
      n <- n + 1
    }
  }
}

Sys.sleep(120)

# Wait for jobs to finish before passing execution back to main step file
i <- 0
while (i == 0) {
  checks <- list.files(file.path(tmp_dir), pattern='finished.*.txt')
  count <- length(checks)
  print(paste("checking ", Sys.time(), " ", count, "of ", n, " jobs finished"))
  if (count == n) i <- i + 1 else Sys.sleep(60) 
}

print(paste(step_num, "all files renamed and moved to correct upload files"))

# setup for parallelization
# erase and make directory for finished checks
dir.create(file.path(tmp_dir, "02_temp", "01_code", "checks"), showWarnings = F)
files <- list.files(file.path(tmp_dir, "02_temp", "01_code", "checks"))
finished.files <- grep("finished_save.*.txt", files, value = T)
for(f in finished.files){
  file.remove(file.path(tmp_dir, "02_temp", "01_code", "checks", f))
}

# # upload to DisMod (save results)
type <- "save"
n <- 0
acause <- unique(dim.dt$acause)
for (cause in acause) {
  grouping <- unique(dim.dt[acause == cause, grouping])
  for (group in grouping) {
    healthstate <- unique(dim.dt[acause == cause & grouping == group, healthstate])
    for(state in healthstate) {
      id <- unique(dim.dt[acause == cause & grouping == group & healthstate == state, sequela_id])

      my_job_name <- paste("save_results", state, id, sep = "_")
      print(paste("submitting", my_job_name))
      my_project <- "proj_hiv"
      my_queue <- "all.q"
      my_fthread <- 10
      my_mem <- "50G"
      my_time <- "05:00:00"
      parallel_script <- paste0(code_dir, step_num, "_parallel.R")

      my_args <- argparser(
        root_j_dir   = root_j_dir,
        root_tmp_dir = root_tmp_dir,
        date         = date,
        step_num     = step_num,
        step_name    = step_name,
        code_dir     = code_dir,
        in_dir       = in_dir,
        out_dir      = out_dir,
        tmp_dir      = tmp_dir,
        etiology     = cause,
        group        = group,
        healthstate  = state,
        meid         = id,
        ds           = ds
      )

      qsub_new(
        job_name   = my_job_name,
        project    = my_project,
        queue      = my_queue,
        fthread    = my_fthread,
        mem        = my_mem,
        time       = my_time,
        shell_file = shell_file,
        script     = parallel_script,
        args       = my_args
      )

      n <- n + 1
    }
  }
}

# Wait for jobs to finish before passing execution back to main step file
i <- 0
while (i == 0) {
  checks <- list.files(file.path(tmp_dir), pattern='finished_save.*.txt')
  count <- length(checks)
  print(paste("checking ", Sys.time(), " ", count, "of ", n, " jobs finished"))
  if (count == n) i <- i + 1 else Sys.sleep(60) 
}

print(paste(step_num, "save_results files uploaded"))

