#####################################################################################################################################################################################
## Purpose:		This sub-step template is for parallelized jobs submitted from main step code
## Description:	Parallelization of 05b_sequela_split_woseiz
#####################################################################################################################################################################################
rm(list=ls())


# LOAD SETTINGS FROM STEP CODE (NO NEED TO EDIT THIS SECTION) ------------------
# Load functions and packages
pacman::p_load(R.utils, data.table)

# Get arguments from R.utils version of commandArgs
args <- commandArgs(trailingOnly = TRUE, asValues = TRUE)
print(args)
list2env(args, environment()); rm(args)

# ------------------------------------------------------------------------------
# pull demographics from RDS created in model_custom
demographics <- readRDS(file.path("FILEPATH"))
years <- demographics$year_id
sexes <- demographics$sex_id
measure <- 5

groupings <- c("long_mild", "long_modsev")
# ------------------------------------------------------------------------------

# Inputs -----------------------------------------------------------------------
# directory for pulling files from previous step
pull_dir_04e <- file.path("FILEPATH")
pull_dir_04d <- file.path("FILEPATH")
# split
pull_dir_05a <- file.path("FILEPATH")
# for viral only
pull_dir_03b <- file.path("FILEPATH")

# Set step-specific output directories
tmp_dir <- paste0(tmp_dir, step_num, "_", step_name,"/")
out_dir <- paste0(out_dir, step_num, "_", step_name,"/")
# ------------------------------------------------------------------------------

# Run job ----------------------------------------------------------------------
# For bacterial: split long_mild & long_modsev into specific sequela based on frequency_distributions.csv

# For viral: Split motor-cog 

for (y in years) {
  for (s in sexes) {
    for (g in groupings) {
      if (g == "long_mild") {
        dt <- readRDS(file.path("FILEPATH"))
      } else if (g == "long_modsev") {
        dt <- fread(file.path("FILEPATH"))
      } else {
        stop("Group does not match those used in step 05b")
      }
      
      seq.prop.dt <- readRDS(file.path("FILEPATH"))
      seq.prop.dt <- seq.prop.dt[state != 'asymptomatic']

      if (etiology == "viral"){
        # Use only motor & cognitive sequela
        seq.prop.dt <- seq.prop.dt[state %like% "motor" | state %like% "cog" | state %like% "id_"]
        # Squeeze the draws to 1: i.e., make it so that all motor + cog sequela sum to the long_mild (or long_modsev) total
        seq.prop.dt[, paste0("v_", 0:999) := lapply(0:999, function(x) {get(paste0("v_", x)) / sum(get(paste0("v_", x)))})]
        # Also - use this step to move/reformat vision_mono and behavioral files from 03b to match bacterial where they are created in 05b
        for(group_tmp in c("vision_mono", "behavior")){
          in.filename <- paste0(location, "_", y, "_", s, ".rds")
          out.filename <- paste0(location, "_", y, "_", s, ".csv")
          dt_tmp <- readRDS(file.path("FILEPATH"))
          if(group_tmp == "behavior") group_tmp <- "adhd" # Switch the name for consistency with healthstates
          out.dir <- file.path("FILEPATH")
          dir.create(out.dir, recursive = TRUE, showWarnings = FALSE)
          fwrite(dt_tmp, file.path("FILEPATH"))
        }
      }

      sequela <- unique(seq.prop.dt$state)
      for (seq in sequela) {
        split.seq.prop.dt <- seq.prop.dt[state == seq]
        split.seq.prop.dt <- merge(split.seq.prop.dt, dt, all.x = T)
        
        split.seq.prop.dt[, paste0("draw_", 0:999):= lapply(0:999, function(x) {get(paste0("draw_", x)) * get(paste0("v_", x))})]
        cols.remove <- c("code", "state", paste0("v_", 0:999))
        split.seq.prop.dt[, (cols.remove):= NULL]
        
        out.filename <- paste0(location, "_", y, "_", s, ".csv")
        out.dir <- file.path("FILEPATH")
        dir.create(out.dir, recursive = TRUE, showWarnings = FALSE)
        fwrite(split.seq.prop.dt, file.path("FILEPATH"))
      }
    }
  }
}


# CHECK FILES (NO NEED TO EDIT THIS SECTION) -----------------------------------
file.create(paste0("FILEPATH"), overwrite=T)
# ------------------------------------------------------------------------------

