#####################################################################################################################################################################################
## Purpose:		This sub-step is for writing central functions (eg. get draws) outputs to input files to minimize database calls
## Author:		username
## Last updated:	02/28/2022
## Description:	Pull inputs from get_draws, get_envelope, etc.
## TODO: Write an output file w/ all versions used in this script (ex. population run id)
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
pacman::p_load(R.utils, data.table, readxl, openxlsx)

# Get arguments from R.utils version of commandArgs
args <- commandArgs(trailingOnly = TRUE, asValues = TRUE)
print(args)
list2env(args, environment()); rm(args)

# User specified options -------------------------------------------------------
# SOURCE SHARED FUNCTIONS ------------------------------
invisible(sapply(list.files("FILEPATH"), source))

# make input directories
# Load steps template
step_sheet <- read.xlsx(file.path(code_dir, paste0("FILEPATH")), sheet="steps")
step_sheet <- step_sheet[!is.na(step_sheet$step), ]
step_sheet[c("step", "hold", "name")] <- sapply(step_sheet[c("step", "hold", "name")],as.character)

# Get list of parent steps (i.e. numbered steps only)
step_sheet$step_num <- gsub("[a-z]", "", step_sheet$step)
step_num <- unique(step_sheet$step_num)

# Create step directories
for (s in step_sheet$step) {
  name <- step_sheet$name[step_sheet$step == s]
  fullname <- paste0(s, "_", name)
  for (dir in c(out_dir, tmp_dir)) {
    dir.create(file.path(dir, fullname),                           showWarnings = F)
    dir.create(file.path(dir, fullname, "01_inputs"),              showWarnings = F)
    dir.create(file.path(dir, fullname, "02_temp/01_checks"),      showWarnings = F, recursive = T)
    dir.create(file.path(dir, fullname, "02_temp/02_logs"),        showWarnings = F, recursive = T)
    dir.create(file.path(dir, fullname, "02_temp/03_data"),        showWarnings = F, recursive = T)
    dir.create(file.path(dir, fullname, "02_temp/04_diagnostics"), showWarnings = F, recursive = T)
    dir.create(file.path(dir, fullname, "03_outputs/01_draws"),    showWarnings = F, recursive = T)
    dir.create(file.path(dir, fullname, "03_outputs/02_summary"),  showWarnings = F, recursive = T)
    dir.create(file.path(dir, fullname, "03_outputs/03_other"),    showWarnings = F, recursive = T)
  }
}

# get list of MEIDs for parent MEID to pull
dim.dt <- fread(file.path(code_dir, paste0(cause, "_dimension.csv")))
parent_meid <- dim.dt[grouping == "cases" & acause == cause & healthstate == "_parent"]$modelable_entity_id

# generate demographics data
demographics <- get_demographics(gbd_team="epi", gbd_round_id=gbd_round)
years <- demographics$year_id
sexes <- demographics$sex_id
ages <- demographics$age_group_id
# save it as an R object
saveRDS(demographics, file.path(in_dir,"demographics_temp.rds"))

# generate location data 
loc.meta <- get_location_metadata(location_set_id = 9, gbd_round_id = gbd_round)
locations <- loc.meta[most_detailed == 1 & is_estimate == 1, unique(location_id)]
fwrite(loc.meta[most_detailed == 1 & is_estimate == 1], file.path(in_dir,"loc_meta.csv"))

# generate age metadata 
age_meta <- get_age_metadata(age_group_set_id = 19, gbd_round_id = gbd_round)
fwrite(age_meta, file.path(in_dir,"age_meta.csv"))

# generate population data
pop <- get_population(year_id = years, location_id = "all", sex_id = sexes, age_group_id = ages, gbd_round_id = gbd_round, decomp_step = ds)
fwrite(pop, file.path(in_dir,"pop.csv"))

# Make input directories
dir.create(file.path(in_dir, "02a_cfr_draws"))
dir.create(file.path(in_dir, "04a_dismod_prep_wmort"))

# Generate inputs needed for...

## Step 02a & 02b

# Pull arguments
measures <- c(5, 6, 7, 9)
# Check for outputs
files <- list.files(file.path(in_dir, "02a_cfr_draws"))
best_model_version <- get_best_model_versions(entity = "modelable_entity", ids = parent_meid, gbd_round_id = gbd_round, decomp_step = ds, status = "best")
mv_id <- best_model_version$model_version_id
if(! any(files %like% mv_id)){
  # If no outputs - get draws
  draws <- get_draws(gbd_id_type = "modelable_entity_id", 
                     source = "epi", 
                     gbd_id = parent_meid, 
                     age_group_id = ages, 
                     measure_id = measures, 
                     location_id = locations, 
                     gbd_round_id = gbd_round, 
                     decomp_step = ds,
                     num_workers = 24)
  # Delete old files -ONLY the files with _dismod_ in name
  files <- list.files(file.path(in_dir, "02a_cfr_draws"), full.names = T)[list.files(file.path(in_dir, "02a_cfr_draws"), full.names = T) %like% "_dismod_"]
  lapply(files, file.remove)
  # Write the draws
  fwrite(draws, file.path(in_dir, "02a_cfr_draws", paste0(cause, "_dismod_", mv_id, "_draws.csv")))
}

## Step 04
# Pull epilepsy estimates
epilepsy_meid <- 2403
measures <- c(7,9)

# Check for outputs
files <- list.files(file.path(in_dir, "04a_dismod_prep_wmort"))
best_model_version <- get_best_model_versions(entity = "modelable_entity", ids = epilepsy_meid, gbd_round_id = gbd_round, decomp_step = ds, status = "best")
mv_id <- best_model_version$model_version_id
if(! any(files %like% mv_id)){
  epilepsy_draws <- get_draws(gbd_id_type = "modelable_entity_id", 
                              source = "epi", 
                              gbd_id = epilepsy_meid,
                              age_group_id = ages, 
                              measure_id = measures, 
                              location_id = locations, 
                              gbd_round_id = gbd_round, 
                              decomp_step = ds,
                              num_workers = 24)
  # Calculate mean and standard deviation for each row
  epilepsy_draws <- cbind(epilepsy_draws, meas_value = rowMeans(epilepsy_draws[, paste0("draw_", 0:999)]), meas_stdev = apply(epilepsy_draws[, paste0("draw_", 0:999)], MARGIN = 1, FUN = sd))
  epilepsy_draws[measure_id == 7, measure := "remission"]
  epilepsy_draws[measure_id == 9, measure := "mtexcess"]
  cols.remove <- c("model_version_id", "modelable_entity_id", "measure_id", paste0("draw_", 0:999))
  epilepsy_draws[, c(cols.remove) := NULL]
  # Delete old files -ONLY the files with epilepsy in name
  files <- list.files(file.path(in_dir, "04a_dismod_prep_wmort"), full.names = T)[list.files(file.path(in_dir, "04a_dismod_prep_wmort"), full.names = T) %like% "epilepsy"]
  lapply(files, file.remove)
  # Write csv with all the draws
  fwrite(epilepsy_draws, file.path(in_dir, "04a_dismod_prep_wmort", paste0("epilepsy_dismod_", mv_id, "_draws.csv")))
}

# Pull life table

# Check for outputs
files <- list.files(file.path(in_dir, "04a_dismod_prep_wmort"))
if(! any(files %like% "mortality_envelope")){
  # Pull the mortality envelope, with shock, with HIV
  mort_envelope <- get_envelope(with_shock = 1,
                                with_hiv = 1,
                                rates = 1,
                                age_group_id = unique(age_meta$age_group_id),
                                location_id = locations,
                                year_id = years,
                                sex_id = sexes,
                                gbd_round_id = gbd_round,
                                decomp_step = ds)
  # Check for issues
  if (any(mort_envelope$mean > mort_envelope$upper | mort_envelope$mean < mort_envelope$lower)){
    print(paste0("Mortality envelope mean falls outside of UI for locations ", paste(unique(mort_envelope[mean > upper | mean < lower]$location_id), collapse = ", "), ". Coercing bounds of UI to the mean."))
    mort_envelope[mean > upper, upper := mean]
    mort_envelope[mean < lower, lower := mean]
  }
  # And write it out
  fwrite(mort_envelope, file.path(in_dir,"04a_dismod_prep_wmort","mortality_envelope.csv"))
}