demographics <- ihme::get_demographics(
  gbd_team = "epi",
  release_id = nch::id_for("release", "GBD 2023"),
)
params_global <- list(
  release_id = nch::id_for("release", "GBD 2023"),
  previous_release_id = nch::id_for("release", "GBD 2021"),
  dir_mnt = "FILEPATH",
  sex_id = purrr::chuck(demographics, "sex_id"),
  location_id = purrr::chuck(demographics, "location_id"),
  year_id = purrr::chuck(demographics, "year_id")
)
path_params_global <- "params_global.rds"
readr::write_rds(params_global, file = path_params_global)
