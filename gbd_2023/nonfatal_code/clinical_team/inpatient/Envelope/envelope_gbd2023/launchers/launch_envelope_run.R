# Title: Envelope run setup
# Purpose: Launch run to produce bundle data version
# Input: setup script, raw data sources
# Output: bundle data version

# Environment setup
source("~/utils/env_setup.R")
source("~/utils/inputs_utils.R")

# Run directory structure setup
config <- openxlsx::read.xlsx(paste0(home_dir, "config_run_id.xlsx")) %>%
  as.data.frame()

# Use run_id marked as "Current"
config <- config %>%
  dplyr::filter(status == tolower("current"))

run_id <- config %>%
  dplyr::pull(run_id)

setup_run_dir <- function(run_id) {
  # Create run directory
  run_dir <- paste0(home_dir, "runs/", "run_", run_id, "/")
  if (!dir.exists(run_dir)) {
    dir.create(run_dir)
  }
  cat("Created run directory: ", run_dir, "\n")

  # Create subdirectories under run_{run_id}
  subdirs <- c("data", "logs", "viz", "utils")
  for (subdir in subdirs) {
    subdir_path <- paste0(run_dir, subdir, "/")
    if (!dir.exists(subdir_path)) {
      dir.create(subdir_path)
    }
  }
  cat("Created run data subdirectories: ", paste0(subdirs, collapse = ","), "\n")

  # Create subdirectories under ./data
  subdirs <- c(
    "clinical_inpatient",
    "formatted",
    "bundle",
    "metadata",
    "splines",
    "stpgr"
  )
  for (subdir in subdirs) {
    subdir_path <- paste0(run_dir, "data/", subdir, "/")
    if (!dir.exists(subdir_path)) {
      dir.create(subdir_path)
    }
  }
  cat("Created run data subdirectories: ", paste0(subdirs, collapse = ","), "\n")

  # assign run_dir to an object
  assign("run_dir", run_dir, envir = .GlobalEnv)
}

setup_run_dir(run_id)


# Pull all column values and assign to objects in the global env
# Say that objects are being created and print new objects
cat("Creating objects in the global environment for the following variables:\n")
config_objects <- config %>%
  dplyr::filter(run_id == run_id) %>%
  dplyr::select(-run_id, -status) %>%
  as.list()
list2env(as.list(config_objects), envir = .GlobalEnv)

# Get metadata
age_metadata <- pull_age_metadata(release_id, age_group_set_id)
age_group_ids <- unique(age_metadata$age_group_id)
complete_age_metadata <- pull_complete_age_metadata()
ghdx_metadata <- pull_ghdx_metadata()
location_metadata <- get_location_metadata(release_id = release_id, location_set_id = location_set_id)



# Launch scripts depending on the goal
# One run always results in one ST-GPR model
# A run may use existing crosswalk data version (i.e., only run ST-GPR, no formatting or corrections)
# A run may use existing bundle data version (i.e., run corrections and then ST-GPR)
# A run may use new raw data and/or metadata and run the entire pipeline: make new bundle version, make new crosswalk version, make new ST-GPR model
# This is indicated in the config
# If both bundle version id and crosswalk version id are non-missing, only launch ST-GPR model from specified crosswalk version ID
# If only bundle version id is non-missing, launch corrections and then ST-GPR model
# If both bundle and crosswalk version IDs are missing, run the entire pipeline

version_id_cols <- c("bundle_version_id", "crosswalk_version_id")
# convert version id cols to integer in config with data.table
config <- as.data.table(config)
config[, (version_id_cols) := lapply(.SD, as.integer), .SDcols = version_id_cols]
# Now if there are any characters in version_id_cols (e.g., "none", "unspecified"), they will be NA

# Get run_inputs and run_corrections boolean objects in the environment
if (is.na(config$bundle_version_id) == TRUE) {
  run_inputs <- TRUE
  run_corrections <- TRUE
  # If no bundle version ID is indicated, but crosswalk_version_id is, then only run corrections
} else if (is.na(config$crosswalk_version_id) == FALSE) {
  run_inputs <- FALSE
  run_corrections <- TRUE
  # If both bundle and crosswalk version IDs are indicated, then only run ST-GPR model
} else {
  run_inputs <- FALSE
  run_corrections <- FALSE
}





# Inputs ---------------------------------------------------------------------------------

## Ensure there's space for another bundle version
version_quota <- get_version_quota(bundle_id = bundle_id)
n_active_versions <- n_distinct(version_quota[bundle_version_status == "active", .(bundle_version_id)])
if (n_active_versions >= 15) {
  cat("Quota of 15 active versions reached. Prune before uploading new data...\n")
  print(unique(version_quota[crosswalk_version_status != "delete", .(bundle_version_id, crosswalk_version_id, crosswalk_version_status, bundle_version_status, model_version_id)]))
  stop("Please consider deleting active versions that are not associated with an active model.\n")
  prune_versions <- TRUE
  # ids_to_remove <- c(48681,48657,48599,48475)
  # call_prune_bundle_version <- prune_version(bundle_id, bundle_version_id = ids_to_remove)
} else {
  cat("Number of active versions: ", n_active_versions, "\n")
  cat("Proceeding with bundle formatting and upload...\n")
  prune_versions <- FALSE
}

# prune if quota exceeded
if (prune_versions == TRUE) {
  cat("Pruning active versions is required before proceeding with new data upload.")
  active_versions <- unique(version_quota[bundle_version_status == "active", .(bundle_version_id)])
  cat("Active versions not associated with an active or best model - these can be removed: ", paste0(active_versions, collapse = ","))
  versions_to_prune <- as.integer(readline(prompt = "Please enter bundle_version_id(s) to prune: "))
  # assert that the input is integer, otherwise throw an error
  assert_that(is.integer(versions_to_prune) & is.na(versions_to_prune) == FALSE, msg = "Please enter an integer.")
  prune_version(bundle_id, bundle_version_id = versions_to_prune)
  cat("Pruned versions: ", versions_to_prune, "\n")
  n_active_versions <- n_distinct(version_quota[bundle_version_status == "active", .(bundle_version_id)])
  cat("Number of active versions: ", n_active_versions, "\n")
}

## Format and vet input sources
if (run_inputs == TRUE) {
  cat("Launching input data formatting...\n")

  # Source paths
  source(paste0(code_dir, "src/utils/input_data_paths.R"))

  # Indicate whether you want to remove overlaps. Defaults to FALSE
  apply_ad_hoc_exclusions_bool <- FALSE # whether you want to exclude any sources based on ad hoc criteria
  exclude_zero_cases_bool <- TRUE # whether you want to exclude sources with zero cases
  include_uses_env_bool <- TRUE # whether you want to include incomplete sources using the envelope (these are needed to compute age/sex splitting model input, but not utilization modeling)
  include_proportion_measure_bool <- TRUE # whether you want to include proportion measures in the data
  max_inlier_val_int <- NA_integer_ # maximum inlier value - if NA, nothing will be outliered
  remove_overlaps_bool <- TRUE # whether you want overlaps removed from bundle data
  plot_bundle_data_bool <- TRUE # whether you want this plotted or not (takes a few minutes)
  plot_sources_using_env_bool <- TRUE # whether you want to plot data using the envelope

  # Source input formatting launcher (will use those paths)
  source(paste0(code_dir, "src/launchers/launcher_inputs.R"))

  cat(yellow("Data for bundle upload vetting plots saved to", run_dir, "viz/", "\n"))
  cat(green("Proceed with upload if data looks good.", "\n"))
} else {
  cat("Skipping input data formatting...\n")
}

# Now the data is present in the environment `data_for_upload_to_bundle`

## Renew and save bundle version
source(paste0(code_dir, "workers/inputs/upload/upload_to_bundle.R"))
# Remove all data from the existing bundle
wipe_existing_bundle(bundle_id)
# Describe newly formatted data, upload it to the bundle, save a bundle version, and pull if from DB
# Prompt user to type in description in the console
description <- readline("Please enter a description for the new bundle version: ")
updated_bundle <- update_bundle_get_version(new_bundle_data, bundle_id, description)


# Corrections ----------------------------------------------------------------------------

## Remove redundant data points and data using the envelope ----
if (run_corrections == TRUE) {
  cat("Launching corrections...\n")

  # Source paths
  # source(paste0(code_dir, "src/utils/corrections_paths.R"))

  # Source corrections launcher
  source(paste0(code_dir, "launchers/launcher_corrections.R"))

  cat(yellow("Data for corrections vetting plots saved to", run_dir, "viz/", "\n"))
  cat(green("Proceed with ST-GPR model if data looks good.", "\n"))
} else {
  cat("Skipping corrections...\n")
}

# Utilization modeling -------------------------------------------------------------------

source(paste0(code_dir, "launchers/launcher_stgpr.R"))
