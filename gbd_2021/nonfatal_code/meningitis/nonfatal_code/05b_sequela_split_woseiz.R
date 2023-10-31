#####################################################################################################################################################################################
## Purpose:		This sub-step template is for parallelized jobs submitted from main step code
## Author:		USERNAME
## Description:	Parallelization of 05b_sequela_split_woseiz
#####################################################################################################################################################################################
rm(list=ls())

if (Sys.info()["sysname"] == "Linux") {
  j <- "FILEPATH"
  h <- paste0("FILEPATH")
} else { 
  j <- "FILEPATH"
  h <- "FILEPATH"
}

# LOAD SETTINGS FROM STEP CODE (NO NEED TO EDIT THIS SECTION) ------------------
# Load functions and packages
pacman::p_load(R.utils, data.table)

# Get arguments from R.utils version of commandArgs
args <- commandArgs(trailingOnly = TRUE, asValues = TRUE)
print(args)
list2env(args, environment()); rm(args)

# ------------------------------------------------------------------------------
# pull demographics from RDS created in model_custom
demographics <- readRDS(file.path(in_dir,"demographics_temp.rds"))
years <- demographics$year_id
sexes <- demographics$sex_id
measure <- 5

groupings <- c("long_mild", "long_modsev")
# ------------------------------------------------------------------------------

# Inputs -----------------------------------------------------------------------
# directory for pulling files from previous step
pull_dir_04e <- file.path(tmp_dir, "04e_outcome_prev_womort", "03_outputs", "01_draws")
pull_dir_04d <- file.path(tmp_dir, "04d_ODE_run", "prev_results")
# split
pull_dir_05a <- file.path(tmp_dir, "05a_sequela_prop", "03_outputs", "01_draws")

# Set step-specific output directories
tmp_dir <- paste0(tmp_dir, step_num, "_", step_name,"/")
out_dir <- paste0(out_dir, step_num, "_", step_name,"/")
# ------------------------------------------------------------------------------

# Run job ----------------------------------------------------------------------
for (y in years) {
  for (s in sexes) {
    for (g in groupings) {
      if (g == "long_mild") {
        dt <- readRDS(file.path(pull_dir_04e, paste0(cause, '_', g, '_', location, '_', y, '_', s, '.rds')))
      } else if (g == "long_modsev") {
        dt <- fread(file.path(pull_dir_04d, cause, g, location, y, s, paste0('prevalence_', cause, '_', g, '_', location, '_', y, '_', s, '.csv')))
      } else {
        stop("Group does not match those used in step 05b")
      }
      
      seq.prop.dt <- readRDS(file.path(pull_dir_05a, paste0(cause, '_', g, '.rds')))
      seq.prop.dt <- seq.prop.dt[state != 'asymptomatic']
      sequela <- unique(seq.prop.dt$state)
      for (seq in sequela) {
        split.seq.prop.dt <- seq.prop.dt[state == seq]
        split.seq.prop.dt <- merge(split.seq.prop.dt, dt, all.x = T)
        
        split.seq.prop.dt[, paste0("draw_", 0:999):= lapply(0:999, function(x) {get(paste0("draw_", x)) * get(paste0("v_", x))})]
        cols.remove <- c("code", "state", paste0("v_", 0:999))
        split.seq.prop.dt[, (cols.remove):= NULL]
        
        out.filename <- paste0(location, "_", y, "_", s, ".csv")
        out.dir <- file.path(tmp_dir, "03_outputs", "01_draws", cause, g, seq)
        dir.create(out.dir, recursive = TRUE, showWarnings = FALSE)
        fwrite(split.seq.prop.dt, file.path(out.dir, out.filename))
      }
    }
  }
}

# CHECK FILES (NO NEED TO EDIT THIS SECTION) -----------------------------------
file.create("FILEPATH")
# ------------------------------------------------------------------------------

