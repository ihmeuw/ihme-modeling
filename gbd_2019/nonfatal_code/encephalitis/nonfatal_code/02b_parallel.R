#####################################################################################################################################################################################
## Purpose:		This sub-step template is for parallelized jobs submitted from main step code
## Author:		
## Last updated:	2019/02/27
## Description:	Parallelization of 02b_acute_survive
#####################################################################################################################################################################################
rm(list=ls())

# LOAD SETTINGS FROM MASTER CODE (NO NEED TO EDIT THIS SECTION) ----------------
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
parameters <- fread(file.path(code_dir, paste0(step_num, "_parameters.csv")))
location <- parameters[task_id, location_id]

# User specified options -------------------------------------------------------
# Source GBD 2019 shared functions
k <- # filepath
source(paste0(k, "get_draws.R"))
source(paste0(k, "get_demographics.R"))

# pull demographics from RDS created in model_custom
demographics <- readRDS(file.path(in_dir,"demographics_temp.rds"))
years <- demographics$year_id
sexes <- demographics$sex_id
ages <- demographics$age_group_id

functional <- "encephalitis"
parent_meid <- 1419

# Inputs -----------------------------------------------------------------------
pull_dir_02a <- file.path(root_tmp_dir, "02a_cfr_draws","03_outputs", "01_draws")

# Run job ----------------------------------------------------------------------
draws <- get_draws(gbd_id_type = "modelable_entity_id", 
                   source = "epi", 
                   gbd_id = parent_meid, 
                   age_group_id = ages, 
                   measure_id = 6, 
                   location_id = location, 
                   gbd_round_id = 6, 
                   decomp_step = ds)
setDT(draws)
draws[, c("model_version_id", "measure_id"):= NULL]

for (y in years) {
  for (s in sexes) {
    survive <- readRDS(file.path(pull_dir_02a,paste0("dm-", functional, "-survive-", location, "_", y, "_", s, ".rds")))
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
file.create(paste0(tmp_dir, "finished_loc", location, ".txt"), overwrite=T)