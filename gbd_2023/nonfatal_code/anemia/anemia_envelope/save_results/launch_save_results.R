
# define adj type for saving ----------------------------------------------

adj_type <- 'who'

# load in list of files saved in ensemble step ----------------------------

sd_files <- list.files(
  path = 'FILEPATH',
  pattern = '*.\\.fst$',
  full.names = TRUE
)

sd_file_param_path <- file.path(getwd(), 'save_results/sd_files.rds')
saveRDS(
  object = sd_files,
  file = sd_file_param_path
)

# define map for ME IDs, update if needed ---------------------------------

me_id_file_path <- file.path(getwd(), 'save_results/me_id_map.csv')

# define where inputs for ME upload should be saved -----------------------

output_dir <- 'FILEPATH'

# define ME parameters that are ID agnostic -------------------------------

save_me_metadata_list <- list(
  mark_best = FALSE,
  description = '...',
  metric_id = 3,
  num_draws = 1000,
  gbd_rel_id = 16
)

me_metadata_file_path <- file.path(getwd(), 'save_results/me_metadata.rds')

saveRDS(
  object = save_me_metadata_list,
  file = me_metadata_file_path
)

# launch ME draw compile step ---------------------------------------------

compile_results_id <- nch::submit_job(
  script = file.path(getwd(), 'save_results/save_out_me_data.R'),
  job_name = 'compile_anemia_ensemble',
  script_args = c(adj_type, sd_file_param_path, me_id_file_path, output_dir),
  memory = 50,
  ncpus = 10,
  time = 60 * 2
)

# launch save results -----------------------------------------------------

nch::submit_job(
  script = file.path(getwd(), 'save_results/save_results.R'),
  job_name = 'save_gbd_results',
  script_args = c(me_id_file_path, output_dir, me_metadata_file_path),
  memory = 250,
  ncpus = 40,
  time = 60 * 10,
  array = "1-7%2",
  dependency = if(exists('compile_results_id')) compile_results_id else NULL
)
