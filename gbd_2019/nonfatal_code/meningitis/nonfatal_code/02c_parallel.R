#####################################################################################################################################################################################
## Purpose:		This sub-step template is for parallelized jobs submitted from main step code
## Author:		
## Last updated:	11/20/2018
## To do: Check up on directories/file paths
## Description:	Parallelization of 02c_etiology_split
#####################################################################################################################################################################################
rm(list=ls())

# LOAD SETTINGS FROM STEP CODE (NO NEED TO EDIT THIS SECTION) ------------------
# Load functions and packages
library(argparse, lib.loc = "filepath")
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
source(paste0(code_dir, "helper_functions/source_functions.R"))
sourceDir("filepath")

functional <- "meningitis"
measure <- 6 # incidence

# pull demographics from RDS created in model_custom
demographics <- readRDS(file.path(in_dir,"demographics_temp.rds"))
years <- demographics$year_id
sexes <- demographics$sex_id
ages <- demographics$age_group_id

meid <- 1296 # meningitis meid

pull_dir_02a <- paste0(root_tmp_dir,"/02a_cfr_draws/03_outputs/01_draws/")

# Run job ----------------------------------------------------------------------
draws <- get_draws(
  gbd_id_type = "modelable_entity_id",
  gbd_id = meid,
  source = "epi",
  age_group_id = ages,
  measure_id = measure,
  location_id = location,
  gbd_round_id = 6,
  decomp_step = ds
)
setDT(draws)
draws[, c("model_version_id") := NULL]

for (y in years) {
  for (s in sexes) {
    draws.tmp <- draws[year_id == y & sex_id == s] # incidence 
    # Pull 02a outputs
    survive <- readRDS(paste0(pull_dir_02a, "dm-", functional, "-survive-", location, "_", y, "_", s, ".rds"))
    draws.tmp <- merge(draws.tmp, survive, by=c("age_group_id", "location_id", "sex_id", "year_id"))
    # Incidence of survival proportion (likelihood of surviving)
    draws.tmp[, paste0("surv_prob_", 0:999) := lapply(0:999, function(x) {
      get(paste0("draw_", x)) * get(paste0("v_", x))
    })] # multiplying parent incidence by overall survival 
    cols.remove <- c(paste0("v_", 0:999), paste0("draw_", 0:999))
    draws.tmp[, c(cols.remove):= NULL]
    # Outputs draws of a rate of survival: survivors of a meningitis / total population
    saveRDS(draws.tmp, file.path(tmp_dir, "03_outputs", "01_draws", paste0(location, "_", y, "_", s, ".rds")))
  }
}

# CHECK FILES (NO NEED TO EDIT THIS SECTION) -----------------------------------
file.create(paste0(tmp_dir, "/02_temp/01_code/checks/", "finished_loc", location, ".txt"), overwrite=T)