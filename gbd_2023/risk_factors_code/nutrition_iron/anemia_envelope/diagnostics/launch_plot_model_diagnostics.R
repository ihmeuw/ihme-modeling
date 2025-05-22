
# launch stgpr model diagnostics ------------------------------------------

map_file_path <- file.path(getwd(), "diagnostics/id_param_map.yaml")
adj_type <- 'final'
release_id <- 16
output_dir <- 'FILEPATH'

nch::submit_job(
  script = file.path(getwd(), "diagnostics/plot_model_results.R"),
  script_args = c(map_file_path, adj_type, release_id, output_dir),
  memory = 100,
  ncpus = 20,
  time = 60 * 10,
  job_name = 'plot_stgpr_results'
)

# launch post ensemble diagnostics ----------------------------------------

final_map_file_path <- file.path(getwd(), '/diagnostics/param_maps/final_param_map.yaml')
adj_type <- 'brinda'
output_dir <- 'FILEPATH'

nch::submit_job(
  script = file.path(getwd(), 'diagnostics/plot_final_results.R'),
  script_args = c(final_map_file_path, adj_type, output_dir),
  memory = 50,
  ncpus = 8,
  time = 60 * 7,
  job_name = 'plot_ensemble_results'
)
