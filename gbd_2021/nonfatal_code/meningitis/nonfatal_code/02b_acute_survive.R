#####################################################################################################################################################################################
## Purpose:		This sub-step template is for parallelized jobs submitted from main step code
## Author:		USERNAME
## Last updated:	2019/02/27
## Description:	Parallelization of 02b_acute_survive
#####################################################################################################################################################################################
rm(list=ls())

if (Sys.info()["sysname"] == "Linux") {
  j <- "FILEPATH" 
  h <- paste0("FILEPATH")
} else { 
  j <- "FILEPATH"
  h <- "FILEPATH"
}

# LOAD SETTINGS FROM MASTER CODE (NO NEED TO EDIT THIS SECTION) ----------------
# Load functions and packages
pacman::p_load(R.utils, data.table)

# Get arguments from R.utils version of commandArgs
args <- commandArgs(trailingOnly = TRUE, asValues = TRUE)
print(args)
list2env(args, environment()); rm(args)

# User specified options -------------------------------------------------------

# pull demographics from RDS created in step 01
demographics <- readRDS(file.path(in_dir,"demographics_temp.rds"))
years <- demographics$year_id
sexes <- demographics$sex_id
ages <- demographics$age_group_id

measures <- 6

# Inputs -----------------------------------------------------------------------
pull_dir_02a <- file.path(tmp_dir, "02a_cfr_draws","03_outputs", "01_draws")

# Set step-specific output directories
tmp_dir <- paste0(tmp_dir, step_num, "_", step_name,"/")
out_dir <- paste0(out_dir, step_num, "_", step_name,"/")

# Run job ----------------------------------------------------------------------
# Pull draws from file created in 01, subset to relevant measures & location
draw_file <- list.files(file.path(in_dir, "02a_cfr_draws"))[list.files(file.path(in_dir, "02a_cfr_draws")) %like% paste0(cause, "_dismod_")]
draws <- fread(file.path(in_dir, "02a_cfr_draws", draw_file))
draws <- draws[measure_id %in% measures & location_id == location]
draws[, c("model_version_id", "measure_id"):= NULL]

for (y in years) {
  for (s in sexes) {
    survive <- readRDS(file.path(pull_dir_02a,paste0("dm-", cause, "-survive-", location, "_", y, "_", s, ".rds")))
    draws.tmp <- draws[year_id == y & sex_id ==s]
    merge.dt <- merge(draws.tmp, survive, by=c("year_id", "sex_id", "age_group_id", "location_id"))
    
    # survival rate * incidence draws = survival rate of acute phase
    merge.dt[, paste0("draw_", 0:999):= lapply(0:999, function(x){get(paste0("draw_",x)) * get(paste0("v_",x))})]
    
    cols.remove <- paste0("v_", 0:999)
    merge.dt[, (cols.remove):= NULL]
    
    filename <- paste0("survive_", location, "_", y, "_", s, ".rds")
    saveRDS(merge.dt, file.path(tmp_dir, "03_outputs", "01_draws", filename))
  }
}

# CHECK FILES (NO NEED TO EDIT THIS SECTION) -----------------------------------
file.create(paste0(tmp_dir, "/02_temp/01_checks/", "finished_loc", location, ".txt"), overwrite=T)