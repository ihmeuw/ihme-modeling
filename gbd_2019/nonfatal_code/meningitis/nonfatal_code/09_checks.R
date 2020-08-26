#####################################################################################################################################################################################
## Purpose:		This sub-step template is for parallelized jobs submitted from main step code
## Author:		
## Last updated:	2019/07/10
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
parser$add_argument("--group", help = "potential values are: hearing, vision, and epilepsy", default = NULL, type = "character")
parser$add_argument("--state", help = "healthstate in in_dir/04b_outcome_prev_womort/meningitis_dimension.csv", default = NULL, type = "character")

args <- parser$parse_args()
print(args)
list2env(args, environment()); rm(args)
# ------------------------------------------------------------------------------


# User specified options -------------------------------------------------------
functional <- "meningitis"
metric <- 5

# Source GBD 2019 Shared Functions
source(paste0(code_dir, "helper_functions/source_functions.R"))
sourceDir("filepath")

# pull locations from CSV created in model_custom
locations <- readRDS(file.path(in_dir,"locations_temp.rds"))

# pull demographics from RDS created in model_custom
demographics <- readRDS(file.path(in_dir,"demographics_temp.rds"))
years <- demographics$year_id
sexes <- demographics$sex_id

# Inputs -----------------------------------------------------------------------
pull_dir_04b <- file.path(root_tmp_dir, "04b_outcome_prev_womort", "03_outputs", "01_draws")	
pull_dir_05b <- file.path(root_tmp_dir, "05b_sequela_split_woseiz", "03_outputs", "01_draws")
pull_dir_07 <- file.path(root_tmp_dir, "07_etiology_prev", "03_outputs", "01_draws")
pull_dir_08  <- file.path(root_tmp_dir, "08_viral_prev", "03_outputs", "01_draws")


# Run job ----------------------------------------------------------------------
dim.dt <- fread(file.path(in_dir, paste0(step_num, "_", step_name), "meningitis_dimension.csv"))
dim.dt <- dim.dt[healthstate != "parent" & !(grouping %in% c("_epilepsy", "_hearing", "_vision"))]
dim.dt$num <- ''

dim.dt[grouping %in% c("long_mild", "long_modsev"), num := "05b"]
dim.dt[grouping == "cases", num := "07"]
dim.dt[grouping == "viral", num:= "08"]


step <- unique(dim.dt[grouping == group & healthstate == state, num])
pull_dir_n <- get(paste0("pull_dir_", step))

total_checks <- length(locations) * length(sexes) * length(years)
counter <- 0
if (group == "viral") {
  for (l in locations) {
    for (y in years) {
      for (s in sexes) {
        pull_filename <- paste0("meningitis_other_", state, "_", l, "_", y, "_", s, ".rds")
        dt <- readRDS(file.path(pull_dir_n, "meningitis_other", pull_filename))
        keep.cols <- c("sex_id", "year_id", "age_group_id", "location_id", "measure_id", paste0("draw_", 0:999))
        dt <- dt[, c(keep.cols), with=F]
        write_filename <- paste0(metric, "_", l, "_", y, "_", s, ".csv")
        write_dir <- file.path(tmp_dir, "03_outputs", "01_draws", group, state)
        fwrite(dt, file=file.path(write_dir, write_filename))
        counter <- counter + 1
        print(paste(counter, "out of", total_checks, "completed"))
      }
    } 
  }
} else {
  for (l in locations) {
    for (y in years) {
      for (s in sexes) {
        pull_filename <- paste0(state, "_", l, "_", y, "_", s, ".rds")
        dt <- readRDS(file.path(pull_dir_n, pull_filename))
        keep.cols <- c("sex_id", "year_id", "age_group_id", "location_id", "measure_id", paste0("draw_", 0:999))
        dt <- dt[, c(keep.cols), with=F]
        write_filename <- paste0(metric, "_", l, "_", y, "_", s, ".csv")
        write_dir <- file.path(tmp_dir, "03_outputs", "01_draws", group, state)
        fwrite(dt, file=file.path(write_dir, write_filename))
        counter <- counter + 1
        print(paste(counter, "out of", total_checks, "completed"))
      }
    } 
  }
}



# CHECK FILES (NO NEED TO EDIT THIS SECTION) -----------------------------------
file.create(paste0(tmp_dir, "/02_temp/01_code/checks/", "finished_", group, "_", state,".txt"), overwrite=T)

