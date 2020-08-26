#####################################################################################################################################################################################
## Purpose:		This sub-step template is for parallelized jobs submitted from main step code
## Author:	
## Last updated:	2019
## Description:	Parallelization of 09_collate_outputs
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
parser$add_argument("--cause", help = "potential values are: meningitis_pneumo, meningitis_hib, meningitis_meningo, meningitis_other", default = NULL, type = "character")
parser$add_argument("--group", help = "potential values are: hearing, vision, and epilepsy", default = NULL, type = "character")
parser$add_argument("--state", help = "healthstate in in_dir/04b_outcome_prev_womort/meningitis_dimension.csv", default = NULL, type = "character")

args <- parser$parse_args()
print(args)
list2env(args, environment()); rm(args)
# ------------------------------------------------------------------------------


# User specified options -------------------------------------------------------
functional <- "meningitis"
measure <- 5

# Source GBD 2019 Shared Functions
source(paste0(code_dir, "helper_functions/source_functions.R"))
k <- # filepath
sourceDir(paste0(k, "current/r/"))

# pull locations from CSV created in model_custom
locations <- readRDS(file.path(in_dir,"locations_temp.rds"))

# pull demographics from RDS created in model_custom
demographics <- readRDS(file.path(in_dir,"demographics_temp.rds"))
years <- demographics$year_id
sexes <- demographics$sex_id
# ------------------------------------------------------------------------------

# Inputs -----------------------------------------------------------------------
pull_dir_05b <- file.path(root_tmp_dir, "05b_sequela_split_woseiz", "03_outputs", "01_draws")
# ------------------------------------------------------------------------------

# Run job ----------------------------------------------------------------------
dim.dt <- fread(file.path(in_dir, paste0(step_num, "_", step_name), "encephalitis_dimension.csv"))
dim.dt <- dim.dt[healthstate != "_parent" & !(grouping %in% c("_epilepsy", "cases", "_vision"))]

dim.dt[grouping %in% c("long_mild", "long_modsev"), num:= "05b"]
dim.dt[grouping %in% c("long_mild", "long_modsev"), name:= "outcome_prev_womort"]

# the step is always 05b
step <- unique(dim.dt[acause == cause & grouping == group & healthstate == state, num])
pull_dir_n <- get(paste0("pull_dir_", step))

for (l in locations) {
  for (y in years) {
    for (s in sexes) {
      filename <- paste0(cause, "_", state, "_", l, "_", y, "_", s, ".rds")
      dt <- readRDS(file.path(pull_dir_n, filename))
      if (group == "long_mild") {
        cols.remove <- c("measure_id", "grouping")
        dt[, (cols.remove):= NULL]
      } else if (group == "long_modsev") {
        cols.remove <- c("measure_id")
        dt[, (cols.remove):= NULL]
      }
      out.filename <- paste0(measure, "_", l, "_", y, "_", s, ".csv")
      out.dir <- file.path(tmp_dir, "03_outputs", "01_draws",cause, group, state)
      fwrite(dt, file.path(out.dir, out.filename))
    }
  } 
}
# ------------------------------------------------------------------------------


# CHECK FILES (NO NEED TO EDIT THIS SECTION) -----------------------------------
file.create(paste0(tmp_dir, "finished_", cause, "_", group, "_", state,".txt"), overwrite=T)

