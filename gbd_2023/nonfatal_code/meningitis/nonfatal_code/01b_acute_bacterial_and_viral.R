#####################################################################################################################################################################################
## Purpose:		This step calculates acute viral and bacterial meningitis
## Description:	Parallelization of 05b_sequela_split_woseiz
#####################################################################################################################################################################################
rm(list=ls())



# LOAD SETTINGS FROM STEP CODE (NO NEED TO EDIT THIS SECTION) ------------------
# Load functions and packages
pacman::p_load(R.utils, data.table, parallel, pbapply)
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
demographics <- readRDS(file.path("FILEPATH"))
years <- demographics$year_id
sexes <- demographics$sex_id
locations <- demographics$location_id
age_group_ids <- demographics$age_group_id

# Set parameters
measure_ids <- c(5, 6)

# ------------------------------------------------------------------------------
# PULL AND WRITE RESULTS FOR ACUTE BACTERIAL MENINGITIS
# Pull parent dismod draws from file created in 01, subset to relevant measures & location
draw_file <- list.files(file.path("FILEPATH"))[list.files(file.path("FILEPATH")) %like% "_dismod_"]
draws <- fread(file.path("FILEPATH"))
draws <- draws[measure_id %in% measure_ids]
cols.remove <- c("model_version_id", "metric_id", "modelable_entity_id")
draws[, c(cols.remove) := NULL]

write_dir <- file.path("FILEPATH")
dir.create(write_dir, showWarnings = FALSE)

pblapply(locations, function(l){
  for (y in years) {
    for (s in sexes) {
      dt <- draws[year_id == y & sex_id == s & location_id == l]
      keep.cols <- c("sex_id", "year_id", "age_group_id", "location_id", "measure_id", paste0("draw_", 0:999))
      dt <- dt[, c(keep.cols), with=F]
      write_filename <- paste0(l, "_", y, "_", s, ".csv")
      fwrite(dt, file=file.path("FILEPATH"))
    }
  }
}, cl = 4)

# ------------------------------------------------------------------------------
# GENERATE VIRAL:BACTERIAL RATIO FILE FROM AMR RESULT

ov_mv <- get_best_model_versions(entity="modelable_entity",
                                 ids = 32037,
                                 release_id = paf_release,
                                 status = "best")
other_viral <- fread(paste0("FILEPATH"))
other_viral <- other_viral[measure_id == 3]

npe_mv <- get_best_model_versions(entity="modelable_entity",
                                  ids = 28999,
                                  release_id = paf_release,
                                  status = "best")
nonpolio_enteroviruses <- fread(paste0("FILEPATH"))
nonpolio_enteroviruses <- nonpolio_enteroviruses[measure_id == 3]

# Merge together and add up to viral proportion
setnames(other_viral, old = "mean", new = "ov_mean")
setnames(nonpolio_enteroviruses, old = "mean", new = "npe_mean")
other_viral[,c("upper", "lower", "modelable_entity_id"):=NULL]
nonpolio_enteroviruses[,c("upper", "lower", "modelable_entity_id"):=NULL]

both_meids <- merge(nonpolio_enteroviruses, other_viral, by = c("age_group_id", "cause_id", "measure_id", "year_id", "location_id", "sex_id"))
both_meids <- both_meids[, mean := ov_mean + npe_mean]

viral_prop <- both_meids[,c("age_group_id", "cause_id", "measure_id", "year_id", "location_id", "sex_id", "mean")]
viral_prop <- viral_prop[, modelable_entity_id := "27198"] # Set me_id to dummy for later merging

# Arithmetic so that proportion of viral becomes ratio of bacterial:viral
# Ratio is 1/(1/proportion - 1): 
# i.e., a proportion of 20% would be a 1:4 ratio or a proportion of 50% would be a 1:1 ratio
viral_prop[, ratio := 1/(1/mean - 1)]

# keep only NONFATAL
viral_prop <- viral_prop[measure_id == 3]

# delete unneeded columns
keep.cols <- c("sex_id", "year_id", "age_group_id", "location_id", "ratio", "modelable_entity_id")
viral_prop <- viral_prop[, c(keep.cols), with=F]

# write ratios to summary directory
write.csv(viral_prop, file.path("FILEPATH"), row.names = FALSE)

# PULL AND WRITE RESULTS FOR ACUTE VIRAL MENINGITIS
write_dir <- file.path("FILEPATH")
dir.create(write_dir, showWarnings = FALSE)

################################## FOR GBD23 ####################################
# PULL RATIOS FROM GBD21 TO USE FOR GBD23
viral_prop <- fread("FILEPATH")

# EXTEND TO 2023-2024 AND ADD MISSING LOCATIONS
if(max(viral_prop$year_id) == 2022){
  viral_p2022 <- viral_prop[year_id == 2022]
  viral_p23 <- copy(viral_p2022)
  viral_p23 <- viral_p23[, year_id := 2023]
  viral_p24 <- copy(viral_p2022)
  viral_p24 <- viral_p24[, year_id := 2024]
  viral_prop <- rbind(viral_prop, viral_p23)
  viral_prop <- rbind(viral_prop, viral_p24)
}

# Replace new GBD22 locs with location in same region
euro_loc_replace <- c(93, 4618:4626)

euro_locs_props <- data.table()
for (l in euro_loc_replace){
  replacement <- viral_prop[location_id == 81]
  replacement <- replacement[, location_id := l]
  euro_locs_props <- rbind(euro_locs_props, replacement)
}

ethio_loc_replace <- c(60908, 94364, 95069)

ethiopia_locs_props <- data.table()
for (l in ethio_loc_replace){
  replacement <- viral_prop[location_id == 44856]
  replacement <- replacement[, location_id := l]
  ethiopia_locs_props <- rbind(ethiopia_locs_props, replacement)
}

# Bind back to main viral prop dataframe
viral_prop <- rbind(viral_prop, euro_locs_props)
viral_prop <- rbind(viral_prop, ethiopia_locs_props)


pblapply(locations, function(l){
  for (y in years) {
    for (s in sexes) {
      dt <- draws[year_id == y & sex_id == s & location_id == l]
      dt <- merge(dt, viral_prop, by=c("sex_id", "age_group_id", "location_id", "year_id"), all.x=T)
      dt[, paste0("draw_", 0:999):= lapply(0:999, function(x){get(paste0("draw_", x)) * ratio})]
      keep.cols <- c("sex_id", "year_id", "age_group_id", "location_id", "measure_id", paste0("draw_", 0:999))
      dt <- dt[, c(keep.cols), with=F]
      write_filename <- paste0(l, "_", y, "_", s, ".csv")
      fwrite(dt, file=file.path("FILEPATH"))
    }
  }
}, cl = 4)

# CHECK FILES (NO NEED TO EDIT THIS SECTION) -----------------------------------
# Write check file to indicate step has finished
file.create(paste0(tmp_dir, "FILEPATH"), overwrite=T)
# ------------------------------------------------------------------------------