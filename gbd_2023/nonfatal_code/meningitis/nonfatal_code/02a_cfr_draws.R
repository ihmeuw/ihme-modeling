#####################################################################################################################################################################################
## Purpose:		This sub-step template is for parallelized jobs submitted from main step code
## Description:	Parallelization of 02a_cfr_draws
#####################################################################################################################################################################################
rm(list=ls())



# LOAD SETTINGS FROM MASTER CODE (NO NEED TO EDIT THIS SECTION) ----------------
# Load functions and packages
pacman::p_load(data.table, R.utils)

# Get arguments from R.utils version of commandArgs
args <- commandArgs(trailingOnly = TRUE, asValues = TRUE)
print(args)
list2env(args, environment()); rm(args)

# User specified options -------------------------------------------------------
# pull demographics from RDS created in step 01
demographics <- readRDS(file.path("FILEPATH"))
years <- demographics$year_id
sexes <- demographics$sex_id
ages <- demographics$age_group_id

measures <- c(7, 9)

# Set step-specific output directories
tmp_dir <- paste0(tmp_dir, step_num, "_", step_name,"/")
out_dir <- paste0(out_dir, step_num, "_", step_name,"/")

# Run job ----------------------------------------------------------------------
# Calculating case fatality ratio from remission and excess mortality rates
# Pull draws from file created in 01, subset to relevant measures & location
draw_file <- list.files(file.path("FILEPATH"))
draws <- fread(file.path("FILEPATH"))
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
    
    # Calculate duration
    draws.merge.tmp[ , paste0("dur_",0:999) := lapply(0:999, function(x){1 / (get(paste0("emr_",x)) + get(paste0("rem_",x)))})]
    
    # Calculate survival ratio 
    draws.merge.tmp[ , paste0("v_",0:999) := lapply(0:999, function(x){exp( -1 *  get(paste0("emr_",x)) * get(paste0("dur_",x)))})]
    
    cols.remove <- c(paste0("rem_", 0:999), paste0("emr_", 0:999), paste0("dur_", 0:999))
    draws.merge.tmp[, c(cols.remove) :=NULL]
    
    saveRDS(draws.merge.tmp, file=file.path("FILEPATH"))
  }
}

# CHECK FILES (NO NEED TO EDIT THIS SECTION) -----------------------------------
file.create(paste0(tmp_dir, "/FILEPATH"), overwrite=T)

