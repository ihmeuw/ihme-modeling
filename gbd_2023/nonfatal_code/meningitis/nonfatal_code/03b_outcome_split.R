#####################################################################################################################################################################################
## Purpose:		This sub-step template is for parallelized jobs submitted from main step code
## Description:	Parallelization of 03b_outcome_split
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

# Inputs -----------------------------------------------------------------------
pull_dir_02b <- file.path("FILEPATH")
pull_dir_03a <- file.path("FILEPATH")

# Get the groups as the list of sequela
groups <- substr(list.files(pull_dir_03a)[list.files(pull_dir_03a) %like% etiology], 7+nchar(etiology), nchar(list.files(pull_dir_03a)[list.files(pull_dir_03a) %like% etiology])-4)

# Set step-specific output directories
tmp_dir <- paste0(tmp_dir, step_num, "_", step_name,"/")
out_dir <- paste0(out_dir, step_num, "_", step_name,"/")

# Run job ----------------------------------------------------------------------
for (g in groups) {
  # Pull major proportion draws for meningitis_all (03a)
  major.prop.dt <- readRDS(file.path(pull_dir_03a, paste0('risk_', etiology, '_', g, '.rds')))
  for (y in years) {
    for (s in sexes) {
      # Pull survival rate from parent cause (02b)
      surv.dt <- readRDS(file.path(pull_dir_02b, paste0('survive_', etiology, '_', location, '_', y, '_', s, '.rds')))
      colnames(surv.dt) <- gsub("draw_", "surv_prob_", colnames(surv.dt)) # renames draw to surv_prob
      
      merge.dt <- merge(surv.dt, major.prop.dt, by=c('location_id', 'year_id'), all.x = T)
      setDT(merge.dt)
      
      # multiplying survival rate by major proportion draws
      merge.dt[, paste0('draw_', 0:999):= lapply(0:999, function(x) { get(paste0('draw_', x)) * get(paste0('surv_prob_', x)) } )]
      cols.remove <- c(paste0('surv_prob_', 0:999))
      merge.dt[, (cols.remove):= NULL]
      
      dir.create(file.path("FILEPATH"), showWarnings = F, recursive = T)
      saveRDS(merge.dt, file.path("FILEPATH"))
    }
  }
}

# CHECK FILES (NO NEED TO EDIT THIS SECTION) -----------------------------------
file.create(paste0("FILEPATH"), overwrite=T)
