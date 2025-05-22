args <- commandArgs(trailingOnly = TRUE)
params_global <- readRDS(args[1])
params <- readRDS(args[2])

ihme::get_draws(
  gbd_id_type = "modelable_entity_id",
  gbd_id = purrr::chuck(params, "me_id"),
  source = "epi",
  measure_id = nch::id_for("measure", "prevalence"),
  age_group_id = purrr::chuck(params, "age_group_id"),
  sex_id = purrr::chuck(params_global, "sex_id"),
  location_id = purrr::chuck(params_global, "location_id"),
  year_id = purrr::chuck(params_global, "year_id"),
  status = "best",
  release_id = purrr::chuck(params_global, "release_id"),
  num_workers = parallelly::availableCores()
) |>
  data.table::fwrite(x = _, file = purrr::chuck(params, "path"))
