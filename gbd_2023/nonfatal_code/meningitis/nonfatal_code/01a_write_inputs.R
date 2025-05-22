#####################################################################################################################################################################################
## Purpose:		This sub-step is for writing central functions outputs to input files to minimize database calls
## Description:	Pull inputs from shared functions
#####################################################################################################################################################################################
rm(list=ls())

# LOAD SETTINGS FROM MASTER CODE (NO NEED TO EDIT THIS SECTION) ----------------
# Load functions and packages
pacman::p_load(R.utils, data.table, readxl, openxlsx)

# Get arguments from R.utils version of commandArgs
args <- commandArgs(trailingOnly = TRUE, asValues = TRUE)
print(args)
list2env(args, environment()); rm(args)

# User specified options -------------------------------------------------------
# SOURCE SHARED FUNCTIONS ------------------------------
invisible(sapply(list.files("FILEPATH", full.names = T), source))

# make input directories
# Load steps template
step_sheet <- read.xlsx(file.path("FILEPATH"), sheet="steps")
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
    dir.create(file.path(dir, fullname, "FILEPATH"),              showWarnings = F)
    dir.create(file.path(dir, fullname, "FILEPATH"),      showWarnings = F, recursive = T)
    dir.create(file.path(dir, fullname, "FILEPATH"),        showWarnings = F, recursive = T)
    dir.create(file.path(dir, fullname, "FILEPATH"),        showWarnings = F, recursive = T)
    dir.create(file.path(dir, fullname, "FILEPATH"), showWarnings = F, recursive = T)
    dir.create(file.path(dir, fullname, "FILEPATH"),    showWarnings = F, recursive = T)
    dir.create(file.path(dir, fullname, "FILEPATH"),  showWarnings = F, recursive = T)
    dir.create(file.path(dir, fullname, "FILEPATH"),    showWarnings = F, recursive = T)
  }
}

dir.create(paste0(in_dir), recursive = TRUE)

# Copy files from the "inputs" folder into their respective directories
files <- list.files(paste0(code_dir, "FILEPATH"), full.names = TRUE)
dir.create(paste0(in_dir,"/FILEPATH/"), recursive = TRUE)
file.copy(from = files, to = paste0(in_dir,"/FILEPATH/"), overwrite = TRUE)

files <- list.files(paste0(code_dir, "/FILEPATH/"), full.names = TRUE)
dir.create(paste0(in_dir,"/FILEPATH/"), recursive = TRUE)
file.copy(from = files, to = paste0(in_dir,"/FILEPATH/"), overwrite = TRUE)

files <- list.files(paste0(code_dir, "/FILEPATH/"), full.names = TRUE)
dir.create(paste0(in_dir,"/FILEPATH/"), recursive = TRUE)
file.copy(from = files, to = paste0(in_dir,"/FILEPATH/"), overwrite = TRUE)

files <- list.files(paste0(code_dir, "/FILEPATH/"), full.names = TRUE)
dir.create(paste0(in_dir,"/FILEPATH/"), recursive = TRUE)
file.copy(from = files, to = paste0(in_dir,"/FILEPATH/"), overwrite = TRUE)



# get list of MEIDs for parent MEID to pull
dim.dt <- fread(file.path("FILEPATH"))
parent_meid <- dim.dt[grouping == "cases" & acause == cause & healthstate == "_parent"]$modelable_entity_id

# generate demographics data
demographics <- get_demographics(gbd_team="epi", release_id=release)
years <- demographics$year_id
sexes <- demographics$sex_id
ages <- demographics$age_group_id
# save it as an R object
saveRDS(demographics, file.path("FILEPATH"))

# generate location data 
loc.meta <- get_location_metadata(location_set_id = 9, release_id = release)
locations <- loc.meta[most_detailed == 1 & is_estimate == 1, unique(location_id)]
fwrite(loc.meta[most_detailed == 1 & is_estimate == 1], file.path(in_dir,"loc_meta.csv"))

# generate age metadata 
age_meta <- get_age_metadata(age_group_set_id = 24, release_id = release)
fwrite(age_meta, file.path("FILEPATH"))

# generate population data
pop <- get_population(year_id = years, location_id = "all", sex_id = sexes, age_group_id = ages, release_id = release)
fwrite(pop, file.path("FILEPATH"))

# Make input directories
dir.create(file.path("FILEPATH"))
dir.create(file.path("FILEPATH"))

# Generate inputs needed for...

## Step 02a & 02b

# Pull arguments
measures <- c(5, 6, 7, 9)
# Check for outputs
files <- list.files(file.path("FILEPATH"))
best_model_version <- get_best_model_versions(entity = "modelable_entity", ids = parent_meid, release_id = release,  status = "best")
mv_id <- best_model_version$model_version_id
if(! any(files %like% mv_id)){
  # If no outputs - get draws
  draws <- get_draws(gbd_id_type = "modelable_entity_id", 
                     source = "epi", 
                     gbd_id = parent_meid, 
                     age_group_id = ages, 
                     measure_id = measures, 
                     location_id = locations, 
                     release_id = release, 
                     version_id = mv_id,
                     num_workers = 24)
  # Delete old files -ONLY the files with _dismod_ in name
  files <- list.files(file.path("FILEPATH"), full.names = T)[list.files(file.path("FILEPATH"), full.names = T) %like% "_dismod_"]
  lapply(files, file.remove)
  # Write the draws
  fwrite(draws, file.path("FILEPATH"))
}

## Step 04
# Pull epilepsy estimates
epilepsy_meid <- 2403
measures <- c(7,9)

# Check for outputs
files <- list.files(file.path("FILEPATH"))
mv_id <- 798080
if(! any(files %like% mv_id)){
  epilepsy_draws <- get_draws(gbd_id_type = "modelable_entity_id", 
                              source = "epi", 
                              gbd_id = epilepsy_meid,
                              age_group_id = ages, 
                              measure_id = measures, 
                              location_id = locations, 
                              release_id = release, 
                              version_id = mv_id,
                              num_workers = 24)
  # Calculate mean and standard deviation for each row
  epilepsy_draws <- cbind(epilepsy_draws, meas_value = rowMeans(epilepsy_draws[, paste0("draw_", 0:999)]), meas_stdev = apply(epilepsy_draws[, paste0("draw_", 0:999)], MARGIN = 1, FUN = sd))
  epilepsy_draws[measure_id == 7, measure := "remission"]
  epilepsy_draws[measure_id == 9, measure := "mtexcess"]
  cols.remove <- c("model_version_id", "modelable_entity_id", "measure_id", paste0("draw_", 0:999))
  epilepsy_draws[, c(cols.remove) := NULL]
  # Delete old files -ONLY the files with epilepsy in name
  files <- list.files(file.path("FILEPATH"), full.names = T)[list.files(file.path(in_dir, "04a_dismod_prep_wmort"), full.names = T) %like% "epilepsy"]
  lapply(files, file.remove)
  # Write csv with all the draws
  fwrite(epilepsy_draws, file.path("FILEPATH"))
}

# Pull life table

# Check for outputs
files <- list.files(file.path("FILEPATH"))
if(! any(files %like% "mortality_envelope")){
  # Pull the mortality envelope, with shock, with HIV
  mort_envelope <- get_envelope(with_shock = 1,
                                with_hiv = 1,
                                rates = 1,
                                age_group_id = unique(age_meta$age_group_id),
                                location_id = locations,
                                year_id = years,
                                sex_id = sexes,
                                release_id = release)
  # Check for issues
  if (any(mort_envelope$mean > mort_envelope$upper | mort_envelope$mean < mort_envelope$lower)){
    print(paste0("Mortality envelope mean falls outside of UI for locations ", paste(unique(mort_envelope[mean > upper | mean < lower]$location_id), collapse = ", "), ". Coercing bounds of UI to the mean."))
    mort_envelope[mean > upper, upper := mean]
    mort_envelope[mean < lower, lower := mean]
  }
  # And write it out
  fwrite(mort_envelope, file.path("FILEPATH"))
}