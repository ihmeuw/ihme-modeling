#####################################################################################################################################################################################
## Purpose:		This step template should be submitted from the 00_master.do file either by submitting all steps or selecting one or more steps to run in "steps" global
## Author:		
## Last updated:	04/17/2016
## Description:	calculate prevalence for severe viral infection
## Number of output files: 3144
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

gbd_round_year <- "2019" # CHANGE THIS TO A PARSED ARGUMENT

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
sourceDir(paste0(k, "current/r/"))

# pull demographics from RDS created in model_custom
demographics <- readRDS(file.path(in_dir,"demographics_temp.rds"))
years <- demographics$year_id
sexes <- demographics$sex_id
age_group_ids <- demographics$age_group_id

functional <- "meningitis"
me_id <- 1296
etiology <- "meningitis_other"
grouping <- "viral"
healthstate <- "inf_sev"
# ------------------------------------------------------------------------------

# Inputs -----------------------------------------------------------------------
draws.dt <- get_draws(gbd_round_id = 6,
                      gbd_id_type = "modelable_entity_id",
                      gbd_id = 1296,
                      measure_id = c(5, 6),
                      location_id = location,
                      status = "best",
                      source = "epi",
                      age_group_id = age_group_ids,
                      decomp_step = ds)                  
setDT(draws.dt)

bac.vir.ratio <- fread(file.path(in_dir, paste0(step_num, "_", step_name), paste0("bac_vir_ratio_", gbd_round_year, "_", ds, ".csv")))
# ------------------------------------------------------------------------------

# Run job ----------------------------------------------------------------------
dir.create(file.path(tmp_dir, "03_outputs", "01_draws", etiology), showWarnings = F)

for (y in years) {
  for (s in sexes) {
    draws.tmp.dt <- draws.dt[year_id == y & sex_id == s]
    draws.tmp.dt <- merge(draws.tmp.dt, bac.vir.ratio, by=c("sex_id", "age_group_id"), all.x=T)
    draws.tmp.dt[, paste0("draw_", 0:999):= lapply(0:999, function(x){get(paste0("draw_", x)) * ratio})]
    draws.tmp.dt[, ratio:=NULL]
    saveRDS(draws.tmp.dt, file.path(tmp_dir, "03_outputs", "01_draws", etiology, paste0(etiology, "_", healthstate, "_", location, "_", y, "_", s, ".rds")))
  }
}

#####################################################################################################################################################################################
## CHECK FILES (NO NEED TO EDIT THIS SECTION)
##
file.create(paste0(tmp_dir,"/02_temp/01_code/checks/","finished_loc",location,".txt"), overwrite=T)

