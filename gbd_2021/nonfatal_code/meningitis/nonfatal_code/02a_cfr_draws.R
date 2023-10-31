#####################################################################################################################################################################################
## Purpose:		This sub-step template is for parallelized jobs submitted from main step code
## Author:		USERNAME
## Last updated:	11/20/2018
## Description:	Parallelization of 02a_cfr_draws
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
pacman::p_load(data.table, R.utils)

# Get arguments from R.utils version of commandArgs
args <- commandArgs(trailingOnly = TRUE, asValues = TRUE)
print(args)
list2env(args, environment()); rm(args)

# User specified options -------------------------------------------------------
# pull demographics from RDS created in step 01
demographics <- readRDS(file.path(in_dir,"demographics_temp.rds"))
years <- demographics$year_id
sexes <- demographics$sex_id
ages <- demographics$age_group_id

measures <- c(7, 9)

# Set step-specific output directories
tmp_dir <- paste0(tmp_dir, step_num, "_", step_name,"/")
out_dir <- paste0(out_dir, step_num, "_", step_name,"/")

# Run job ----------------------------------------------------------------------
# Calculating case fatality rate from remission and excess mortality rates
# Pull draws from file created in 01, subset to relevant measures & location
draw_file <- list.files(file.path(in_dir, "02a_cfr_draws"))[list.files(file.path(in_dir, "02a_cfr_draws")) %like% "_dismod_"]
draws <- fread(file.path(in_dir, "02a_cfr_draws", draw_file))
draws <- draws[measure_id %in% measures & location_id == location]
cols.remove <- c("model_version_id", "metric_id", "modelable_entity_id")
draws[, c(cols.remove) := NULL]

for (y in years) {
  for (s in sexes) {
    draws.rem.tmp <- draws[year_id == y & sex_id == s & measure_id == 7]
    colnames(draws.rem.tmp) <- gsub("draw", "rem", colnames(draws.rem.tmp))  ## Renaming draws to remission
    
    draws.emr.tmp <- draws[year_id == y & sex_id == s & measure_id == 9]
    colnames(draws.emr.tmp) <- gsub("draw", "emr", colnames(draws.emr.tmp))  ## Renaming draws to emr
    
    draws.merge.tmp <- merge(draws.rem.tmp[ ,measure_id:= NULL], draws.emr.tmp[ ,measure_id:= NULL], by=c("location_id","year_id", "sex_id", "age_group_id"))
    
    # Calculate duration - see documentation for notes on equation
    draws.merge.tmp[ , paste0("dur_",0:999) := lapply(0:999, function(x){1 / (get(paste0("emr_",x)) + get(paste0("rem_",x)))})]
    
    # Calculate survival rate - see documentation for notes on equation again
    draws.merge.tmp[ , paste0("v_",0:999) := lapply(0:999, function(x){exp( -1 *  get(paste0("emr_",x)) * get(paste0("dur_",x)))})]
    
    cols.remove <- c(paste0("rem_", 0:999), paste0("emr_", 0:999), paste0("dur_", 0:999))
    draws.merge.tmp[, c(cols.remove) :=NULL]
    
    saveRDS(draws.merge.tmp, file=file.path(tmp_dir, "03_outputs", "01_draws", paste0("dm-",cause,"-survive-",location,"_",y, "_",s,".rds")))
  }
}

# CHECK FILES (NO NEED TO EDIT THIS SECTION) -----------------------------------
file.create(paste0("FILEPATH"), overwrite=T)

