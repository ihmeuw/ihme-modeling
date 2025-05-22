# This function gets model draws for the parent hemog model (needed for the scaling step)

invisible(sapply(list.files("cod_pipeline/src_functions/", full.names = T), source))

# Set parameters ----------------------------------------------------------

if (!interactive()) {
  args <- commandArgs(trailingOnly = TRUE)
  param_map_filepath <- args[1]
  task_id <- as.numeric(Sys.getenv("SLURM_ARRAY_TASK_ID"))
} else {
  # option to run interactively 
  task_id <- 1L
  param_map_filepath <- PARAM_MAP_FP
}

# set values based on param map
param_map <- qs::qread(param_map_filepath)

if (param_map$release_id == 16) {
  param_map$location_id <- c(param_map$location_id[!param_map$location_id %in% c(60908L, 94364L, 95069L)], 44858)
}
loc_id <- param_map$location_id[task_id]
year_list_parent <- param_map$year_ids

# Get model draws of parent hemog cod model -------------------------------

cli::cli_progress_step("Getting model draws...", msg_done = "Got model draws.")

model_df <- get_model_draws(
  loc_id = loc_id,
  all_years = year_list_parent,
  release_id = param_map$release_id
)

# Save model draws --------------------------------------------------------

cli::cli_progress_step("Saving model draws...", msg_done = "Model draws saved.")

dir_path <- file.path(param_map$out_dir, "model_draws")
fs::dir_create(dir_path, mode = "775", recurse = TRUE)
file_path <- file.path(dir_path, glue::glue("model_hemog_{loc_id}.qs"))
qs::qsave(model_df, file = file_path)
Sys.chmod(file_path, "775", use_umask = FALSE)


