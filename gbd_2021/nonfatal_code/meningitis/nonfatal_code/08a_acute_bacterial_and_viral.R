#####################################################################################################################################################################################
## Purpose:		This step calculates acute viral and bacterial meningitis
## Author:		USERNAME
## Description:	Parallelization of 05b_sequela_split_woseiz
#####################################################################################################################################################################################
rm(list=ls())

if (Sys.info()["sysname"] == "Linux") {
  j <- "FILEPATH"
  h <- paste0("FILEPATH")
} else { 
  j <- "FILEPATH"
  h <- "FILEPATH"
}

# LOAD SETTINGS FROM STEP CODE (NO NEED TO EDIT THIS SECTION) ------------------
# Load functions and packages
pacman::p_load(R.utils, data.table)
# Source get best model versions function
source("FILEPATH")

# Get arguments from R.utils version of commandArgs
args <- commandArgs(trailingOnly = TRUE, asValues = TRUE)
print(args)
list2env(args, environment()); rm(args)

# Set step-specific output directories
tmp_dir <- paste0(tmp_dir, step_num, "_", step_name,"/")
out_dir <- paste0(out_dir, step_num, "_", step_name,"/")

# pull demographics from RDS created in model_custom
demographics <- readRDS(file.path(in_dir,"demographics_temp.rds"))
years <- demographics$year_id
sexes <- demographics$sex_id
locations <- demographics$location_id
age_group_ids <- demographics$age_group_id

# Set parameters
measure_ids <- c(5, 6)

# ------------------------------------------------------------------------------
# PULL AND WRITE RESULTS FOR ACUTE BACTERIAL MENINGITIS
# Pull parent dismod draws from file created in 01, subset to relevant measures & location
draw_file <- list.files(file.path(in_dir, "02a_cfr_draws"))[list.files(file.path(in_dir, "02a_cfr_draws")) %like% "_dismod_"]
draws <- fread(file.path(in_dir, "02a_cfr_draws", draw_file))
draws <- draws[measure_id %in% measure_ids]
cols.remove <- c("model_version_id", "metric_id", "modelable_entity_id")
draws[, c(cols.remove) := NULL]

write_dir <- file.path(tmp_dir, "03_outputs", "01_draws", "acute_bacterial")
dir.create(write_dir, showWarnings = FALSE)

lapply(locations, function(l){
  for (y in years) {
    for (s in sexes) {
      dt <- draws[year_id == y & sex_id == s & location_id == l]
      keep.cols <- c("sex_id", "year_id", "age_group_id", "location_id", "measure_id", paste0("draw_", 0:999))
      dt <- dt[, c(keep.cols), with=F]
      write_filename <- paste0(l, "_", y, "_", s, ".csv")
      fwrite(dt, file=file.path(write_dir, write_filename))
    }
  }
})

# ------------------------------------------------------------------------------
# GENERATE VIRAL:BACTERIAL RATIO FILE FROM AMR RESULT
# Read in the AMR viral proportion modeled result
mv <- get_best_model_versions(entity="modelable_entity",
                              ids = 27198,
                              gbd_round_id = gbd_round,
                              decomp_step = ds,
                              status = "best")
viral_prop <- fread(paste0("FILEPATH"))

# Arithmetic so that proportion of viral becomes ratio of bacterial:viral
# Ratio is 1/(1/proportion - 1): 
# i.e., a proportion of 20% would be a 1:4 ratio or a proportion of 50% would be a 1:1 ratio
viral_prop[, ratio := 1/(1/mean - 1)]

# keep only NONFATAL
viral_prop <- viral_prop[measure_id == 3]
# delete unneeded columns
keep.cols <- c("sex_id", "year_id", "age_group_id", "location_id", "ratio", "modelable_entity_id")
viral_prop <- viral_prop[, c(keep.cols), with=F]

# write to input directory
write.csv(viral_prop, file.path(in_dir, paste0(step_num, "_", step_name), paste0("bac_vir_ratio_gbd_round_", gbd_round, "_", ds, ".csv")), row.names = FALSE)

# PULL AND WRITE RESULTS FOR ACUTE VIRAL MENINGITIS
write_dir <- file.path(tmp_dir, "03_outputs", "01_draws", "acute_viral")
dir.create(write_dir, showWarnings = FALSE)

lapply(locations, function(l){
  for (y in years) {
    for (s in sexes) {
      dt <- draws[year_id == y & sex_id == s & location_id == l]
      dt <- merge(dt, viral_prop, by=c("sex_id", "age_group_id", "location_id", "year_id"), all.x=T)
      dt[, paste0("draw_", 0:999):= lapply(0:999, function(x){get(paste0("draw_", x)) * ratio})]
      keep.cols <- c("sex_id", "year_id", "age_group_id", "location_id", "measure_id", paste0("draw_", 0:999))
      dt <- dt[, c(keep.cols), with=F]
      write_filename <- paste0(l, "_", y, "_", s, ".csv")
      fwrite(dt, file=file.path(write_dir, write_filename))
    }
  }
})

# CHECK FILES (NO NEED TO EDIT THIS SECTION) -----------------------------------
# Write check file to indicate step has finished
file.create("FILEPATH")
# ------------------------------------------------------------------------------