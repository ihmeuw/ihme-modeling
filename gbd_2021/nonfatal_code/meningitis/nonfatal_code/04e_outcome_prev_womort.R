#####################################################################################################################################################################################
## Purpose:		This sub-step template is for parallelized jobs submitted from main step code
## Author:		USERNAME
## Description:	Parallelization of 04b_outcome_prev_womort
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
pacman::p_load(R.utils, data.table, matrixStats)

# Get arguments from R.utils version of commandArgs
args <- commandArgs(trailingOnly = TRUE, asValues = TRUE)
print(args)
list2env(args, environment()); rm(args)

# User specified options -------------------------------------------------------
# pull demographics from RDS created in model_custom
demographics <- readRDS(file.path(in_dir,"demographics_temp.rds"))
years <- demographics$year_id
sexes <- demographics$sex_id

groups <- c('long_mild', '_vision', '_hearing')
# ------------------------------------------------------------------------------

# Inputs -----------------------------------------------------------------------
pull_dir_03b <- file.path(tmp_dir, "03b_outcome_split", "03_outputs", "01_draws")
age_meta <- fread(file.path(in_dir,"age_meta.csv"))
age_meta <- age_meta[,c("age_group_years_start", "age_group_years_end", "age_group_id")]

# Set step-specific output directories
tmp_dir <- paste0(tmp_dir, step_num, "_", step_name,"/")
out_dir <- paste0(out_dir, step_num, "_", step_name,"/")
# ------------------------------------------------------------------------------

# Run job ----------------------------------------------------------------------
for(y in years){
  for(s in sexes){
    for(g in groups){
      print(paste("iteration for", y, s, g))
      incid.dt <- readRDS(file.path(pull_dir_03b, cause, g, paste0(location, "_", y, "_", s, ".rds")))
      setDT(incid.dt)
      
      # This code relies on the age groups being sorted in order of age_start
      incid.dt <- merge(incid.dt, age_meta, by = "age_group_id")
      setorder(incid.dt, "age_group_years_start")
      
      # Fix columns because I didn't fix it an earlier step.... CHANGE ME IN THE FUTURE PLS
      incid.dt[, c("modelable_entity_id.x", "modelable_entity_id.y"):= NULL]
      
      # Scale infant ages (age_group_id 2, 3, 388, 389) into years
      incid.dt <- incid.dt[age_group_id == 2, paste0("draw_", 0:999):= lapply(.SD, function(x) x *  1/52), .SDcols=paste0("draw_", 0:999)] # one week old neonate
      incid.dt <- incid.dt[age_group_id == 3, paste0("draw_", 0:999):= lapply(.SD, function(x) x *  3/52), .SDcols=paste0("draw_", 0:999)] # 3 week old neonate
      incid.dt <- incid.dt[age_group_id == 388, paste0("draw_", 0:999):= lapply(.SD, function(x) x * 22/52), .SDcols=paste0("draw_", 0:999)] # 1-5 month infant
      incid.dt <- incid.dt[age_group_id == 389, paste0("draw_", 0:999):= lapply(.SD, function(x) x * 26/52), .SDcols=paste0("draw_", 0:999)] # 6-11 month infant
      # Group together age_group_id 2, 3, 388, 389 as 0
      incid.dt[age_group_id %in% c(2, 3, 388, 389), age_group_id:= 0L]
      # Sum together the scaled neonatal age groups
      incid.dt <- incid.dt[, lapply(.SD, sum), by=.(measure_id, metric_id, grouping, location_id, year_id, age_group_id, sex_id), .SDcols=paste0("draw_", 0:999)]
      
      # Recalculate custom age groups so that each row represents 1 year (e.g. age_group_id = 34 (2 - 4 years) gets 3 rows)
      # leave the 12-23 month age group alone
      incid.dt[, rep:=1L][age_group_id %in% c(6:20, 30:32), rep:=5L][age_group_id == 34, rep:=3L]
      incid.dt[, num:=1:.N] # to group each row by itself
      incid.dt <- incid.dt[rep(num,rep)]
      incid.dt[, c("num", "rep"):= NULL]
      incid.dt[, time:=1:.N]
      
      # The following calculations were suggested by Theo Voss for GBD 2017
      # Initialize prevalence and exact prevalence values for year 1
      # Set exact prevalence to be 0
      incid.dt[time == 1, paste0("exact_", 0:999):= 0]
      # Set prevalence to be half-year incidence 
      incid.dt[time == 1, paste0("prev_", 0:999):= lapply(.SD, function(x) x * 1/2), .SDcols=paste0("draw_", 0:999)]
      
      # Recursively define the remaining years
      calculate_exact <- function(exact, draw) {
        for (i in 2:length(exact)) {
          exact[[i]] <- exact[[i-1]] + draw[[i-1]] * (1 - exact[[i-1]])
        }
        return(exact)
      }
      incid.dt[, paste0("exact_", 0:999):= lapply(0:999, function(x){calculate_exact(get(paste0("exact_",x)), get(paste0("draw_",x)))})]
      incid.dt[, paste0("prev_", 0:999) := lapply(0:999, function(x){get(paste0("exact_", x)) + get(paste0("draw_", x)) * 0.5 * (1 - get(paste0("exact_", x)))})]
      
      col.remove <- c(paste0("exact_", 0:999), paste0("draw_", 0:999))
      incid.dt[, (col.remove):= NULL]
      
      # Set measure_id for prevalence
      incid.dt$measure_id <- 5
      setnames(incid.dt, paste0("prev_", 0:999), paste0("draw_", 0:999))
      
      # Return to the original age groups
      incid.dt <- incid.dt[, lapply(.SD, mean), by=.(measure_id, metric_id, grouping, location_id, year_id, age_group_id, sex_id), .SDcols=paste0("draw_", 0:999)]
      # Duplicate for age groups 2, 3, 388, 389
      incid.dt[, rep:=1L][age_group_id == 0, rep:=4L]
      incid.dt[, num:=1:.N] # to group each row by itself
      incid.dt <- incid.dt[rep(num,rep)]
      incid.dt[, c("num", "rep"):= NULL]
      incid.dt[1, age_group_id:= 2]
      incid.dt[2, age_group_id:= 3]
      incid.dt[3, age_group_id:= 388]
      incid.dt[4, age_group_id:= 389]
      
      # Return to original age group ids
      incid.dt <- incid.dt[age_group_id == 2, paste0("draw_", 0:999):= lapply(.SD, function(x) x * 2/52 *(0 +  1/2)), .SDcols=paste0("draw_", 0:999)] # return to 1 week neonate (prevalence)
      incid.dt <- incid.dt[age_group_id == 3, paste0("draw_", 0:999):= lapply(.SD, function(x) x * 2/52 *(1 +  3/2)), .SDcols=paste0("draw_", 0:999)] # return to 1+3 week neonate (prevalence)
      incid.dt <- incid.dt[age_group_id == 388, paste0("draw_", 0:999):= lapply(.SD, function(x) x * 2/52 *(4 + 22/2)), .SDcols=paste0("draw_", 0:999)] # return to 4+22 week infant (prevalence)
      incid.dt <- incid.dt[age_group_id == 389, paste0("draw_", 0:999):= lapply(.SD, function(x) x * 2/52 *(26 + 26/2)), .SDcols=paste0("draw_", 0:999)] # return to 26+26 week infant (prevalence)
      
      if (g == '_vision' | g == '_hearing') {
        dir.create(file.path(tmp_dir, "03_outputs", "01_draws", g, "_unsqueezed"), showWarnings = F, recursive = T)
        col.remove <-  c("grouping")
        incid.dt[, (col.remove):= NULL]
        fwrite(incid.dt, file.path(tmp_dir, "03_outputs", "01_draws", g, "_unsqueezed", paste0(location, "_", y, "_", s, ".csv")))
      } else {
        # do not want to save long_mild with save_results because needs further splitting
        saveRDS(incid.dt, file.path(tmp_dir, '03_outputs', '01_draws', paste0(cause, '_', g, '_', location, '_', y, '_', s, '.rds')))
      }
    }
  }
}
# ------------------------------------------------------------------------------

# CHECK FILES (NO NEED TO EDIT THIS SECTION) -----------------------------------
file.create(paste0("FILEPATH"), overwrite=T)
# ------------------------------------------------------------------------------