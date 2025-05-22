# Prep --------------------------------------------------------------------

library(data.table)
invisible(sapply(list.files("cod_pipeline/src_functions/", full.names = T), source))

# Set parameters ----------------------------------------------------------

if(!interactive()) {
  args <- commandArgs(trailingOnly = TRUE)
  param_map_filepath <- args[1]
} else{
  param_map_filepath <- PARAM_MAP_FP
}

# set values based on param map
param_map <- qs::qread(param_map_filepath)

cli::cli_progress_step("Consolidating interpolated and data rich files...", 
                       msg_done = "Files successfully consolidated into one directory.")

# Consolidate synth files for DR locs and interp files for non-DR locs
consolidate_synth_interp(
  cause_id = param_map$cause_id, 
  sex_id = param_map$sex_id, 
  source_dir = param_map$out_dir
)

cli::cli_progress_step("Checking for Ethiopia subnats...", 
                       msg_done = "Subnat files exist or have been created in this step.")
old_location_id <- 44858
new_location_ids <- c(60908, 95069, 94364)

# Create df of all cause-sex ID combinations to iterate through them
cause_sex_combos <- expand.grid(cause_id = param_map$cause_id, sex_id = param_map$sex_id)

# If location ID 44858 file is present, replace it with new files for subnats
## First, check synth files by iterating through cause-sex combinations
data_dir <- "synth_files"
for (i in seq_len(nrow(cause_sex_combos))) {
  map_row <- cause_sex_combos[i,]
  
  # iterate through new location IDs and define old file name to delete
  for (new_location_id in new_location_ids) {
    old_file_name <- check_and_update_subnats(
      old_location_id = old_location_id, 
      new_location_id = new_location_id, 
      data_dir = data_dir, 
      out_dir = param_map$out_dir,
      cause_id = map_row$cause_id, 
      sex_id = map_row$sex_id)
  }
  
  # delete old file once files for any new locations are saved
  if (fs::file_exists(old_file_name)) {
    message(paste0("Deleting ", old_file_name))
    fs::file_delete(old_file_name)
  }
}

## Second, check parent model files 
data_dir <- "model_draws"
# iterate through new location IDs and define old file name to delete
for (new_location_id in new_location_ids) {
  old_file_name <- check_and_update_subnats(
    old_location_id = old_location_id, 
    new_location_id = new_location_id, 
    data_dir = data_dir,
    out_dir = param_map$out_dir
  )
}

# delete old file from the directory once files for any new locations are saved
if (fs::file_exists(old_file_name)) {
  message(paste0("Deleting ", old_file_name))
  fs::file_delete(old_file_name)
}

cli::cli_progress_done()

# Check for any extra columns and remove them as needed -----------------

# check files based on all combinations of the given cause and sex IDs
keep_needed_cols_only(data_dir = file.path(param_map$out_dir, "synth_files"), 
                      cause_id = param_map$cause_id, 
                      sex_id = param_map$sex_id,
                      write_as_csv = TRUE)
