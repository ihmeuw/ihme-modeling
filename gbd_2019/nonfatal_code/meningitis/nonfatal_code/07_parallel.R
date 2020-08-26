#####################################################################################################################################################################################
## Purpose:		This sub-step template is for parallelized jobs submitted from main step code
## Author:		
## Last updated:	04/17/2016
## Description:	Parallelization of 07_etiology_prev
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

# User specified options --------------------------------------------------
# Source GBD 2019 Shared Functions
source(paste0(code_dir, "helper_functions/source_functions.R"))
sourceDir(paste0(k, "current/r/"))

# pull demographics from RDS created in model_custom
demographics <- readRDS(file.path(in_dir,"demographics_temp.rds"))
years <- demographics$year_id
sexes <- demographics$sex_id
age_group_ids <- demographics$age_group_id

healthstate <- "inf_sev"
measure_ids <- c(5, 6)
# Parent dismod meningitis model
meningitis_meid <- 1296

# Run job ----------------------------------------------------------------------
## pull draws from best meningitis (1296) dismod model
draws.dt <- get_draws(gbd_id_type = "modelable_entity_id",
                      gbd_id = meningitis_meid,
                      measure_id = measure_ids,
                      location_id = location,
                      status = "best",
                      source = "epi",
                      age_group_id = age_group_ids, 
                      gbd_round_id = 6,
                      decomp_step = ds)
setDT(draws.dt)
for (y in years) {
  for (s in sexes) {
    draws.tmp.dt <- draws.dt[year_id == y & sex_id == s]
    saveRDS(draws.tmp.dt, 
            file.path(tmp_dir, "03_outputs", "01_draws", 
                      paste0(healthstate, "_", location, "_", y, "_", s, ".rds")))
  }
}

# CHECK FILES (NO NEED TO EDIT THIS SECTION) -----------------------------------
file.create(paste0(tmp_dir, "/02_temp/01_code/checks/", "finished_loc", location, ".txt"), overwrite=T)