#####################################################################################################################################################################################
## Purpose:		This step template should be submitted from the 00_master.do file either by submitting all steps or selecting one or more steps to run in "steps" global
## Author:		
## Last updated:	04/22/2016
## Description:	Collate all meningitis-etiology-sequelae outputs in one place for transferring to COMO
## Number of output files: 166632 (154056 if without seizure)
## Note: When uploading to DisMod, use:
##         save_results, sequela_id(ID) subnational(yes) description(TEXT DESCRIPTION)
##                       in_dir(filepath)
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
# specify if you want to collate outputs and check that all files are ready for upload
run_checks         <- 1
# specify which sequelae to upload to epi_viz	(upload: 1; do not upload: 0) / add parent meningitis best model
# you need to set these to 1 to run the rest of the code otherwise you will have an empty data table, and just comment out the save_results portion
upload_long_mild   <- 1	 # long term sequelae without mortality risk
upload_long_modsev <- 1	 # long term sequelae with mortality risk
upload_cases       <- 1  # short term sequela due to bacterial infection
upload_viral       <- 1  # short term sequela due to viral infection

# Shell File
shell_file <- paste0(code_dir, "r_shell.sh") # Should be updated to modeler's preferred R shell file

# functional
functional <- "meningitis"
# metric
metric <- 5

# Inputs -----------------------------------------------------------------------
# diectories for pulling files from the previous steps
pull_dir_04b <- file.path(root_tmp_dir, "04b_outcome_prev_womort", "03_outputs", "01_draws")
pull_dir_05b <- file.path(root_tmp_dir, "05b_sequela_split_woseiz", "03_outputs", "01_draws")
pull_dir_07  <- file.path(root_tmp_dir, "07_etiology_prev", "03_outputs", "01_draws")
pull_dir_08  <- file.path(root_tmp_dir, "08_viral_prev", "03_outputs", "01_draws")

dim.dt <- fread(file.path(in_dir, paste0(step_num, "_", step_name), "meningitis_dimension.csv"))
dim.dt <- dim.dt[healthstate != "_parent" & !(grouping %in% c("_epilepsy", "_hearing", "_vision"))]
dim.dt[, num := character()]

dim.dt[grouping %in% c("long_mild", "long_modsev"), num:= "05b"]
dim.dt[grouping == "cases", num:= "07"]
dim.dt[grouping == "viral", num:= "08"]


# filter based on which groups you want to upload (see user specified options)
for (g in c("long_mild", "long_modsev", "cases", "viral")) {
  if (get(paste0("upload_", g)) == 0){
    dim.dt <- dim.dt[grouping != g]
  }
}


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

if(run_checks == 1) {
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
  
  
  # Submit jobs for checks -----------------------------------------------------
  ## check to see if all these files exist, and move them to upload files
  n <- 0
  groupings <- unique(dim.dt[, grouping])
  for (g in groupings) {
    healthstates <- unique(dim.dt[grouping == g, healthstate])
    for(h in healthstates) {
      dir.create(file.path(tmp_dir, "03_outputs", "01_draws", g, h), showWarnings = F, recursive = T)
      
      my_job_name <- paste("check", g, h, step_num, sep = "_")
      print(paste("submitting", my_job_name))
      my_project <- "proj_hiv"
      my_mem <- "1G"
      my_fthread <- 1
      my_time <- "02:00:00"
      my_queue <- 'all.q'
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
        group        = g, 
        healthstate  = h
      )
      
      qsub_new(
        job_name = my_job_name,
        project = my_project,
        queue = my_queue,
        mem = my_mem,
        fthread = my_fthread,
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
    checks <- list.files(file.path(tmp_dir, "02_temp/01_code/checks"), pattern='finished.*.txt')
    count <- length(checks)
    print(paste("checking ", Sys.time(), " ", count, "of ", n, " jobs finished"))
    if (count == n) i <- i + 1 else Sys.sleep(60) 
  }
  
  print(paste(step_num, "all files renamed and moved to correct upload files"))
}


# Submit jobs for upload --------------------------------------------------
# setup for parallelization
# erase and make directory for finished checks
dir.create(file.path(tmp_dir, "02_temp", "01_code", "checks"), showWarnings = F)
files <- list.files(file.path(tmp_dir, "02_temp", "01_code", "checks"))
finished.files <- grep("finished_save.*.txt", files, value = T)
for(f in finished.files){
  file.remove(file.path(tmp_dir, "02_temp", "01_code", "checks", f))
}

# upload to DisMod (save results)
n <- 0
grouping <- unique(dim.dt[, grouping])
for (g in grouping) {
  healthstate <- unique(dim.dt[grouping == g, healthstate])
  for(h in healthstate) {
    id <- unique(dim.dt[grouping == g & healthstate == h, modelable_entity_id])
    print(paste(g, h, id))
    my_job_name <- paste("save_results", g, h, id, sep = "_")
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
                group        = g,
                healthstate  = h,
                meid         = id,
                ds           = ds
               )

    qsub_new(
      job_name = my_job_name,
      project = my_project,
      queue = my_queue,
      fthread = my_fthread,
      mem = my_mem,
      time = my_time,
      shell_file = shell_file,
      script = parallel_script,
      args = my_args
    )
    n <- n + 1
  }
}

# Wait for jobs to finish before passing execution back to main step file
i <- 0
while (i == 0) {
  checks <- list.files(file.path(tmp_dir, "02_temp/01_code/checks"), pattern='finished_save.*.txt')
  count <- length(checks)
  print(paste("checking ", Sys.time(), " ", count, "of ", n, " jobs finished"))
  if (count == n) i <- i + 1 else Sys.sleep(60)
}

print(paste(step_num, "save_results files uploaded"))

