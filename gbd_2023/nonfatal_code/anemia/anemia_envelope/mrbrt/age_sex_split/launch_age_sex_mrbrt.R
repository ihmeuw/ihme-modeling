
# create param map --------------------------------------------------------

param_map_file_name <- file.path(getwd(), "mrbrt/age_sex_split/config.csv")

bundle_map <- read.csv(file.path(getwd(), "model_prep/param_maps/bundle_map.csv"))

param_map <- expand.grid(
  var = unique(bundle_map$var),
  sex_id = 1:2,
  cv_pregnant = 0:1
)

i_vec <- which(
  param_map$sex_id == 2 |
    param_map$sex_id == 1 & param_map$cv_pregnant == 0
)

param_map <- param_map |>
  dplyr::slice(i_vec)

write.csv(
  x = param_map,
  file = param_map_file_name,
  row.names = FALSE
)

# create array string -----------------------------------------------------

num_parallel_jobs <- min(nrow(param_map), 50)
array_string <- paste0("1-", nrow(param_map), "%", num_parallel_jobs)

# launch job --------------------------------------------------------------

nch::submit_job(
  script = file.path(getwd(), "mrbrt/age_sex_split/get_age_weights_linear_cascade_by_sex_preg.R"),
  script_args = param_map_file_name,
  job_name = "anemia_age_sex_weights",
  memory = 20,
  ncpus = 5,
  time = 120,
  array = array_string,
  partition = 'long.q'
)
