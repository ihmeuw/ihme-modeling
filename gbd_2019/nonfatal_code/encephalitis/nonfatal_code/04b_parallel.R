#####################################################################################################################################################################################
## Purpose:		This sub-step template is for parallelized jobs submitted from main step code
## Author:		
## Last updated:	2018
## Description:	Parallelization of 04b_outcome_prev_womort
#####################################################################################################################################################################################
rm(list=ls())

# LOAD SETTINGS FROM MASTER CODE (NO NEED TO EDIT THIS SECTION) ----------------
# Load functions and packages
library(argparse)
library(data.table)
library(matrixStats)

# Get arguments from parser
parser <- ArgumentParser()
parser$add_argument("--date", help = "timestamp of current run (i.e. 2014_01_17)", default = NULL, type = "character")
parser$add_argument("--step_num", help = "step number of this step (i.e. 01a)", default = NULL, type = "character")
parser$add_argument("--step_name", help = "name of current step (i.e. first_step_name)", default = NULL, type = "character")
parser$add_argument("--location", help = "location", default = NULL, type = "integer")
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
parameters <- fread(file.path(code_dir, paste0(step_num, "_parameters.csv")))
location <- parameters[task_id, location_id]
# ------------------------------------------------------------------------------

# User specified options -------------------------------------------------------
# Source GBD 2019 Shared Functions
source(paste0(code_dir, "helper_functions/source_functions.R"))
k <- # filepath
sourceDir(paste0(k, "current/r/"))

# pull demographics from RDS created in model_custom
demographics <- readRDS(file.path(in_dir,"demographics_temp.rds"))
years <- demographics$year_id
sexes <- demographics$sex_id

functional <- "encephalitis"
groups <- c('long_mild', '_vision')
# ------------------------------------------------------------------------------

# Inputs -----------------------------------------------------------------------
pull_dir_03b <- file.path(root_tmp_dir, "03b_outcome_split", "03_outputs", "01_draws")
# ------------------------------------------------------------------------------

# Run job ----------------------------------------------------------------------
for(y in years){
  for(s in sexes){
    for(g in groups){
      print(paste("iteration for", y, s, g))
      incid.dt <- readRDS(file.path(pull_dir_03b, functional, g, paste0(location, "_", y, "_", s, ".rds")))
      setDT(incid.dt)
      
      # Fix columns because I didn't fix it an earlier step.... CHANGE ME IN THE FUTURE PLS
      incid.dt[, c("modelable_entity_id.x", "modelable_entity_id.y"):= NULL]
      
      # Scale neonatal (age_group_id 2, 3, 4) into years
      incid.dt <- incid.dt[age_group_id == 2, paste0("draw_", 0:999):= lapply(.SD, function(x) x *  1/52), .SDcols=paste0("draw_", 0:999)] # one week old neonate
      incid.dt <- incid.dt[age_group_id == 3, paste0("draw_", 0:999):= lapply(.SD, function(x) x *  3/52), .SDcols=paste0("draw_", 0:999)] # 3 week old neonate
      incid.dt <- incid.dt[age_group_id == 4, paste0("draw_", 0:999):= lapply(.SD, function(x) x * 48/52), .SDcols=paste0("draw_", 0:999)] # under one year neonate
      # Group together age_group_id 2, 3, and 4 as 0
      incid.dt[age_group_id %in% c(2, 3, 4), age_group_id:= 0L]
      # Sum together the scaled neonatal age groups
      incid.dt <- incid.dt[, lapply(.SD, sum), by=.(measure_id, metric_id, grouping, location_id, year_id, age_group_id, sex_id), .SDcols=paste0("draw_", 0:999)]
      
      # Recalculate custom age groups so that each row represents 1 year (e.g. age_group_id = 5 (1 - 4 years) gets 4 rows)
      incid.dt[, rep:=1L][age_group_id %in% c(6:20, 30:32), rep:=5L][age_group_id == 5, rep:=4L]
      incid.dt[, num:=1:.N] # to group each row by itself
      incid.dt <- incid.dt[rep(num,rep)]
      incid.dt[, c("num", "rep"):= NULL]
      incid.dt[, time:=1:.N]
      
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
      # Duplicate for age groups 2, 3, and 4
      incid.dt[, rep:=1L][age_group_id == 0, rep:=3L]
      incid.dt[, num:=1:.N] # to group each row by itself
      incid.dt <- incid.dt[rep(num,rep)]
      incid.dt[, c("num", "rep"):= NULL]
      incid.dt[1, age_group_id:= 2]
      incid.dt[2, age_group_id:= 3]
      incid.dt[3, age_group_id:= 4]
      
      # Return to original age group ids
      incid.dt <- incid.dt[age_group_id == 2, paste0("draw_", 0:999):= lapply(.SD, function(x) x * 2/52 *(0 +  1/2)), .SDcols=paste0("draw_", 0:999)] # return to 1 week neonate (prevalence)
      incid.dt <- incid.dt[age_group_id == 3, paste0("draw_", 0:999):= lapply(.SD, function(x) x * 2/52 *(1 +  3/2)), .SDcols=paste0("draw_", 0:999)] # return to 3 week neonate (prevalence)
      incid.dt <- incid.dt[age_group_id == 4, paste0("draw_", 0:999):= lapply(.SD, function(x) x * 2/52 *(4 + 48/2)), .SDcols=paste0("draw_", 0:999)] # return to 48 week neonate (prevalence)
      
      if (g == '_vision') {
        dir.create(file.path(tmp_dir, "03_outputs", "01_draws", g, "_unsqueezed"), showWarnings = F, recursive = T)
        col.remove <-  c("measure_id", "grouping")
        incid.dt[, (col.remove):= NULL]
        fwrite(incid.dt, file.path(tmp_dir, "03_outputs", "01_draws", g, "_unsqueezed", paste0("5_", location, "_", y, "_", s, ".csv")))
      } else {
        # do not want to save long_mild with save_results because needs further splitting
        saveRDS(incid.dt, file.path(tmp_dir, '03_outputs', '01_draws', paste0(functional, '_', g, '_', location, '_', y, '_', s, '.rds')))
      }
    }
  }
}
# ------------------------------------------------------------------------------

# CHECK FILES (NO NEED TO EDIT THIS SECTION) -----------------------------------
file.create(paste0(tmp_dir,"finished_loc",location,".txt"), overwrite=T)
# ------------------------------------------------------------------------------