
# Prep --------------------------------------------------------------------

invisible(sapply(list.files("cod_pipeline/src_functions/", full.names = T), source))

# Set parameters ----------------------------------------------------------

if(!interactive()) {
  args <- commandArgs(trailingOnly = TRUE)
  param_map_filepath <- args[1]
  task_id <- as.numeric(Sys.getenv("SLURM_ARRAY_TASK_ID"))
} else{
  param_map_filepath <- PARAM_MAP_FP
  task_id <- 1L
}

param_map <- qs::qread(param_map_filepath)
loc_id <- param_map$location_id[task_id]

input_map <- data.table::fread(file.path(param_map$out_dir, "input_map.csv"))  

# scale results ------------------------------------
message(paste0("Starting scaling for loc ", loc_id))
scaled_df <- get_subcause_draws(
  cause_id = param_map$cause_id, 
  sex_id = param_map$sex_id,
  input_dir = file.path(param_map$out_dir, "synth_files"),
  location_id = loc_id
) |>
  scale_cod_data(
    loc = loc_id, 
    out_dir = param_map$out_dir)
message("Successfully scaled")


# Output scaled draws as one file per cause-sex combination ---------------

for (cause in param_map$cause_id) {
  for (sex in param_map$sex_id) {
    dir_synth_scaled <- file.path(param_map$out_dir, "synth_scaled")
    output_dir = file.path(dir_synth_scaled, cause, sex)
    fs::dir_create(path = output_dir, mode = "u=rwx,go=rwx", recurse = TRUE)
    data.table::fwrite(
      x = scaled_df[scaled_df$cause_id == cause & scaled_df$sex_id == sex, ],
      file = file.path(output_dir, glue::glue("synth_scaled_hemog_{loc_id}.csv"))
    )
    message(paste0("Scaled results were output for: cause ", cause, ", sex ", sex, ", location ", loc_id))
  }
}

message("Scaling step finished")