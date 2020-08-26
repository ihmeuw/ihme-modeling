#####################################################################################################################################################################################
## Purpose:		This sub-step template is for parallelized jobs submitted from main step code
## Author:		
## Last updated:	12/31/2015
## To do: Check file paths and directories
## Description:	Parallelization of 03b_outcome_split
#####################################################################################################################################################################################
rm(list=ls())

# LOAD SETTINGS FROM MASTER CODE (NO NEED TO EDIT THIS SECTION) ----------------
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
# Source GBD 2017 Shared Functions
source(paste0(code_dir, "helper_functions/source_functions.R"))
k <- # filepath
sourceDir(paste0(k, "current/r/"))

# pull demographics from RDS created in model_custom
demographics <- readRDS(file.path(in_dir,"demographics_temp.rds"))
years <- demographics$year_id
sexes <- demographics$sex_id

functional <- "meningitis"
groups <- c("_hearing", "long_mild", "_vision",  "long_modsev", "epilepsy")

# Inputs -----------------------------------------------------------------------
pull_dir_02c <- file.path(root_tmp_dir, "02c_etiology_split", "03_outputs", "01_draws")
pull_dir_03a <- file.path(root_tmp_dir, "03a_outcome_prop", "03_outputs", "01_draws")

# Run job ----------------------------------------------------------------------
for (g in groups) {
  # Pull group proportion draws (03a)
  group.prop.dt <- readRDS(file.path(pull_dir_03a, paste0('risk_', g, '.rds')))
  for (y in years) {
    for (s in sexes) {
      # Pull survival rate from etiology split (02c)
      surv.dt <- readRDS(file.path(pull_dir_02c, paste0(location, '_', y, '_', s, '.rds')))
      
      merge.dt <- merge(surv.dt, group.prop.dt, by=c('measure_id', 'location_id', 'year_id'), all.x = T)
      setDT(merge.dt)
      
      merge.dt[, paste0('draw_', 0:999):= lapply(0:999, function(x) { get(paste0('draw_', x)) * get(paste0('surv_prob_', x)) } )]
      cols.remove <- paste0('surv_prob_', 0:999)
      merge.dt[, (cols.remove):= NULL]
      
      dir.create(file.path(tmp_dir, '03_outputs', '01_draws', g), showWarnings = F, recursive = T)
      saveRDS(merge.dt, file.path(tmp_dir, '03_outputs', '01_draws', g, paste0(location, '_', y, '_', s, '.rds')))
    }
  }
}

# CHECK FILES (NO NEED TO EDIT THIS SECTION) -----------------------------------
file.create(paste0(tmp_dir,"/02_temp/01_code/checks/","finished_loc",location,".txt"), overwrite=T)