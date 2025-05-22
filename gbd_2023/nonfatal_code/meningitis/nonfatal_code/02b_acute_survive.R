#####################################################################################################################################################################################
## Purpose:		This sub-step template is for parallelized jobs submitted from main step code
## Description:	Parallelization of 02b_acute_survive
#####################################################################################################################################################################################
rm(list=ls())



# LOAD SETTINGS FROM MASTER CODE (NO NEED TO EDIT THIS SECTION) ----------------
# Load functions and packages
pacman::p_load(R.utils, data.table)

# Get arguments from R.utils version of commandArgs
args <- commandArgs(trailingOnly = TRUE, asValues = TRUE)
print(args)
list2env(args, environment()); rm(args)

# User specified options -------------------------------------------------------

# pull demographics from RDS created in step 01
demographics <- readRDS(file.path("FILEPATH"))
years <- demographics$year_id
sexes <- demographics$sex_id
ages <- demographics$age_group_id

measures <- 6

# Inputs -----------------------------------------------------------------------
pull_dir_01b <- file.path("FILEPATH")
pull_dir_02a <- file.path("FILEPATH")

# Set step-specific output directories
tmp_dir <- paste0(tmp_dir, step_num, "_", step_name,"/")
out_dir <- paste0(out_dir, step_num, "_", step_name,"/")

# Run job ----------------------------------------------------------------------

for (y in years) {
  for (s in sexes) {
    # Pull incidence draws from file created in 01b
    draws <- fread(file.path("FILEPATH"))
    draws <- draws[measure_id %in% measures]
    draws[, c("measure_id"):= NULL]
    
    # Pull survival ratio from file created in 02a
    survive <- readRDS(file.path("FILEPATH"))
    draws.tmp <- draws[year_id == y & sex_id ==s]
    merge.dt <- merge(draws.tmp, survive, by=c("year_id", "sex_id", "age_group_id", "location_id"))
    
    # survival ratio * incidence draws = population-denominator survival rate of acute phase
    merge.dt[, paste0("draw_", 0:999):= lapply(0:999, function(x){get(paste0("draw_",x)) * get(paste0("v_",x))})]
    
    cols.remove <- paste0("v_", 0:999)
    merge.dt[, (cols.remove):= NULL]
    
    filename <- paste0("survive_", etiology, "_", location, "_", y, "_", s, ".rds")
    saveRDS(merge.dt, file.path("FILEPATH"))
  }
}

# CHECK FILES (NO NEED TO EDIT THIS SECTION) -----------------------------------
file.create(paste0(tmp_dir, "FILEPATH"), overwrite=T)