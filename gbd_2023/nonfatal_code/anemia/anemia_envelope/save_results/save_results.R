
# load in command args ----------------------------------------------------

if(interactive()) {
  task_id <- 1
  me_id_file_path <- file.path(getwd(), 'save_results/me_id_map.csv')
  output_dir <- 'FILEPATH'
  me_metadata_file_path <- file.path(getwd(), 'save_results/me_metadata.rds')
} else {
  task_id <- as.integer(Sys.getenv("SLURM_ARRAY_TASK_ID"))
  
  command_args <- commandArgs(trailingOnly = TRUE)
  me_id_file_path <- command_args[1]
  output_dir <- command_args[2]
  me_metadata_file_path <- command_args[3]
}

me_id_map <- read.csv(me_id_file_path)

save_me_metadata_list <- readRDS(me_metadata_file_path)

# save results ------------------------------------------------------------

me_dir <- file.path(
  output_dir, me_id_map$subfolder[task_id]
)

ihme::save_results_epi(
  input_dir = me_dir,
  input_file_pattern = 'for_upload_{location_id}.csv',
  modelable_entity_id = me_id_map$output_me_id[task_id],
  description = save_me_metadata_list$description,
  measure_id = me_id_map$measure_id[task_id],
  metric_id = save_me_metadata_list$metric_id,
  n_draws = save_me_metadata_list$num_draws,
  release_id = save_me_metadata_list$gbd_rel_id,
  bundle_id = me_id_map$bundle_id[task_id],
  crosswalk_version_id = me_id_map$xwalk_version_id[task_id],
  mark_best = save_me_metadata_list$mark_best
)