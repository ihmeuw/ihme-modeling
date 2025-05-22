#' Prepare a vector of data rich location ids for pulling CODEm results
get_dr_loc_ids <- function(release_id, version_map_path) {
  # Get all locations for current GBD round 
  regular_locs <- ihme::get_location_metadata(
    location_set_id = 35, 
    release_id = release_id
  ) |>
    dplyr::select(
      location_id, 
      super_region_name, 
      ihme_loc_id, 
      location_name, 
      level, 
      is_estimate)
  
  # Get one set of DR model estimates to identify data rich locations
  warning("Have you updated the version map in the /cod_results folder to be the ", 
          "most recent data rich models you want to pull in?")
  
  version_map <- readxl::read_excel(fs::path(version_map_path))
  version_id <- version_map$dr_version_id[
    version_map$sex_id == 1 & version_map$cause_id == 614
  ]
  dr_mod <- ihme::get_model_results(
    gbd_team = "cod",
    release_id = release_id,
    gbd_id = 614,
    model_version_id = version_id
  ) |> 
    dplyr::distinct(location_id)
  
  # Subset dr_locs to most detailed level estimate
  dr_locs <- dplyr::left_join(dr_mod, regular_locs, by = "location_id") |>
    dplyr::filter(is_estimate == 1) |>
    dplyr::pull(location_id)
}
