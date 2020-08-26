#####################################################################################################################################################################################
## Purpose:		This sub-step template is for parallelized jobs submitted from main step code
## Author:		
## Last updated:	2019/03/08
## Description:	Parallelization of 05b_sequela_split_woseiz
#####################################################################################################################################################################################
rm(list=ls())

# LOAD SETTINGS FROM STEP CODE (NO NEED TO EDIT THIS SECTION) ------------------
# Load functions and packages
library(argparse)
library(data.table)

# Get arguments from parser
parser <- ArgumentParser()
parser$add_argument("--date", help = "timestamp of current run (i.e. 2014_01_17)", default = NULL, type = "character")
parser$add_argument("--step_num", help = "step number of this step (i.e. 01a)", default = NULL, type = "character")
parser$add_argument("--step_name", help = "name of current step (i.e. first_step_name)", default = NULL, type = "character")
parser$add_argument("--code_dir", help = "code directory", default = NULL, type = "character")
parser$add_argument("--in_dir", help = "directory for external inputs", default = NULL, type = "character")
parser$add_argument("--out_dir", help = "directory for this steps checks", default = NULL, type = "character")
parser$add_argument("--tmp_dir", help = "directory for this steps intermediate draw files", default = NULL, type = "character")
parser$add_argument("--root_j_dir", help = "base directory on J", default = NULL, type = "character")
parser$add_argument("--root_tmp_dir", help = "base directory on clustertmp", default = NULL, type = "character")
parser$add_argument("--ds", help = "specify decomp step", default = 'step1', type = "character")

args <- parser$parse_args()
print(args)
list2env(args, environment()); rm(args)

# Get location from parameter map
task_id <- as.integer(Sys.getenv("SGE_TASK_ID"))
parameters <- fread(paste0(code_dir, step_num, "_parameters.csv"))
location <- parameters[task_id, location_id]
# ------------------------------------------------------------------------------

# User specified options -------------------------------------------------------
# Source GBD 2019 Shared Functions
source(paste0(code_dir, "helper_functions/source_functions.R"))
k <- # filepath
sourceDir(paste0(k, "current/r/"))

# pull demographics from RDS created in model_custom
demographics <- readRDS(file.path(in_dir,"demographics_temp.rds"))

functional <- "encephalitis"
groupings <- c("long_mild", "long_modsev")
# ------------------------------------------------------------------------------


# Inputs -----------------------------------------------------------------------
# directory for pulling files from previous step
pull_dir_04b <- file.path(root_tmp_dir, "04b_outcome_prev_womort", "03_outputs", "01_draws")
pull_dir_04a <- file.path(root_tmp_dir, "04a_dismod_prep_wmort", "04_ODE", "prev_results")
# split
pull_dir_05a <- file.path(root_tmp_dir, "05a_sequela_prop", "03_outputs", "01_draws")
# ------------------------------------------------------------------------------

# Run job ----------------------------------------------------------------------
for (y in years) {
  for (s in sexes) {
    for (g in groupings) {
      if (g == "long_mild") {
        dt <- readRDS(file.path(pull_dir_04b, paste0(functional, '_', g, '_', location, '_', y, '_', s, '.rds')))
      } else if (g == "long_modsev") {
        dt <- fread(file.path(pull_dir_04a, functional, g, location, y, s, paste0('prevalence_', functional, '_', g, '_', location, '_', y, '_', s, '.csv')))
      } else {
        stop("Group does not match those used in step 05b")
      }
      
      seq.prop.dt <- readRDS(file.path(pull_dir_05a, paste0(functional, '_', g, '.rds')))
      seq.prop.dt <- seq.prop.dt[state != 'asymptomatic']
      sequela <- unique(seq.prop.dt$state)
      for (seq in sequela) {
        split.seq.prop.dt <- seq.prop.dt[state == seq]
        split.seq.prop.dt <- merge(split.seq.prop.dt, dt, all.x = T)
        
        split.seq.prop.dt[, paste0("draw_", 0:999):= lapply(0:999, function(x) {get(paste0("draw_", x)) * get(paste0("v_", x))})]
        cols.remove <- c("code", "state", paste0("v_", 0:999))
        split.seq.prop.dt[, (cols.remove):= NULL]
        
        saveRDS(split.seq.prop.dt, file.path(tmp_dir, "03_outputs", "01_draws", paste0(functional, '_', seq, '_', location, '_', y, '_', s, '.rds')))
      }
    }
  }
}

# CHECK FILES (NO NEED TO EDIT THIS SECTION) -----------------------------------
file.create(paste0(tmp_dir, "finished_loc", location, ".txt"), overwrite=T)
# ------------------------------------------------------------------------------

