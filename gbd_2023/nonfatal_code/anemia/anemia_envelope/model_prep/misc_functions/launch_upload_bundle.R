
# launch parallel jobs for upload bundle ----------------------------------

bundle_map <- read.csv(file.path(getwd(), 'model_prep/param_maps/bundle_map.csv'))

array_string <- paste0("1-", nrow(bundle_map), "%", nrow(bundle_map))

nch::submit_job(
  script = file.path(getwd(), 'model_prep/misc_functions/parallel_upload_bundle.R'),
  job_name = "upload_anemia_bundles",
  memory = 20,
  ncpus = 5,
  time = 60,
  array = array_string
)
