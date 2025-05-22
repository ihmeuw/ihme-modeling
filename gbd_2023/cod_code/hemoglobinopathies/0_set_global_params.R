# Purpose: Create global parameter map for hemoglobinopathies CoD pipeline.
# Usage: Manually update values below as needed before running the pipeline.

source("cod_pipeline/src_functions/paths.R") 
source(paste0(COD_REPO_ROOT, "/src_functions/get_dr_loc_ids.R"))


# Create global parameter map for pipeline --------------------------------
global_param_map <- list()

# Define desired outputs
global_param_map$save_interp <- F
global_param_map$synthesize_results <- T
global_param_map$save_synth <- F  
global_param_map$scale_results <- T
global_param_map$save_synth_scaled <- T
global_param_map$mark_best_flag <- T

# Assign GBD-related values
global_param_map$cause_id <- c(614, 615, 616, 618)
global_param_map$sex_id <- c(1, 2)
global_param_map$year_ids <- c(1980:2024)
global_param_map$release_id21 <- 9  # GBD 2021
global_param_map$release_id <- 16  # GBD 2023
global_param_map$age_group_set_id <- 24

# Assign file paths
global_param_map$out_dir <- COD_OUT_DIR
global_param_map$version_map_path <- DR_VERSION_MAP
global_param_map$interp_check_path <- fs::dir_create(global_param_map$out_dir, "loc_check", "interp", Sys.Date())
global_param_map$synth_check_path <- fs::dir_create(global_param_map$out_dir, "loc_check", "synth", Sys.Date())

# Define the location IDs for the current GBD round -----------------------
global_param_map$location_id <- ihme::get_demographics("epi", 
                                                       release_id = global_param_map$release_id)$location_id
global_param_map$dr_loc_id <- get_dr_loc_ids(release_id = global_param_map$release_id, 
                                             version_map_path = global_param_map$version_map_path)

# Write out global and cod locs param maps --------------------------------
param_map_filepath <- file.path(global_param_map$out_dir, "pipeline_param_map.qs")
qs::qsave(global_param_map, param_map_filepath)
