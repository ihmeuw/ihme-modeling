#####################################################################################################################################################################################
## Purpose:		This sub-step template is for parallelized jobs submitted from main step code
## Author:		
## Last updated:	11/20/2018
## Description:	Parallelization of 02a_cfr_draws
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
# Source GBD 2019 shared functions
source(paste0(code_dir, "helper_functions/source_functions.R"))
sourceDir(paste0(k, "current/r/"))

functional <- "meningitis"
me_id <- 1296 # meningitis 

# pull demographics from RDS created in model_custom
demographics <- readRDS(file.path(in_dir,"demographics_temp.rds"))
years <- demographics$year_id
sexes <- demographics$sex_id
ages <- demographics$age_group_id
measures <- c(7, 9)

# Run job ----------------------------------------------------------------------
# Calculating case fatality rate from remission and excess mortality rates
draws <-
  get_draws(
    gbd_id_type = "modelable_entity_id",
    source = "epi",
    gbd_id = me_id,
    age_group_id = ages,
    measure_id = measures,
    location_id = location,
    gbd_round_id = 6,
    decomp_step = ds
  )
setDT(draws)
cols.remove <- c("model_version_id")
draws[, c(cols.remove) := NULL]

for (y in years) {
  for (s in sexes) {
    draws.rem.tmp <- draws[year_id == y & sex_id == s & measure_id == 7]
    colnames(draws.rem.tmp) <- gsub("draw", "rem", colnames(draws.rem.tmp))  ## Renaming draws to remission
    
    draws.emr.tmp <- draws[year_id == y & sex_id == s & measure_id == 9]
    colnames(draws.emr.tmp) <- gsub("draw", "emr", colnames(draws.emr.tmp))  ## Renaming draws to emr
    
    draws.merge.tmp <- merge(draws.rem.tmp[ ,measure_id:= NULL], draws.emr.tmp[ ,measure_id:= NULL], by=c("location_id","year_id", "sex_id", "age_group_id")) # merging remission and EMR 
    
    # Calculate duration - see documentation for notes on equation
    draws.merge.tmp[ , paste0("dur_",0:999) := lapply(0:999, function(x){1 / (get(paste0("emr_",x)) + get(paste0("rem_",x)))})]
    
    # Calculate survival rate - see documentation for notes on equation again
    draws.merge.tmp[ , paste0("v_",0:999) := lapply(0:999, function(x){exp( -1 *  get(paste0("emr_",x)) * get(paste0("dur_",x)))})]
    
    cols.remove <- c(paste0("rem_", 0:999), paste0("emr_", 0:999), paste0("dur_", 0:999))
    draws.merge.tmp[, c(cols.remove) :=NULL]
    
    saveRDS(draws.merge.tmp, file=file.path(tmp_dir, "03_outputs", "01_draws", paste0("dm-",functional,"-survive-",location,"_",y, "_",s,".rds")))
  }
}
# CHECK FILES (NO NEED TO EDIT THIS SECTION) -----------------------------------
file.create(paste0(tmp_dir, "/02_temp/01_code/checks/", "finished_loc", location, ".txt"), overwrite=T)