# adjusts onemod draws in old age to match spxmod values

library(data.table)


# Setup -------------------------------------------------------------------

cfg <- config::get(file = fs::path_wd("population_postprocessing/config.yml"))

map_age_gbd <- demInternal::get_age_map(type = "gbd", gbd_year = cfg$gbd_year)
map_age_gbd_short <- map_age_gbd[, .(age_group_id, age_start)]

id_vars <- c("location_id", "year_id", "age_group_id", "sex_id")

dir_pop_input <- fs::path(cfg$dir$pop, "versioned_inputs", "gbd2023")
filename_pop_input <- glue::glue("onemod_pop-data={cfg$version$rs_data}-est={cfg$version$onemod}-loc_ages-filled")


# Calculate spxmod scalars for set location-ages --------------------------

dt_old_age_adj <-
  fs::path(dir_pop_input, filename_pop_input, ext = "csv") |>
  fread() |>
  _[, .(age_start = min(age_start)), by = .(location_id)] |>
  setnames("age_start", "age_adj_start")


filename_onemod_adj <- glue::glue("predictions-updated-{cfg$version$onemod}")

dt_onemod_means_adj <-
  fs::path(
    cfg$dir$onemod_updated, filename_onemod_adj, ext = "parquet"
  ) |>
  arrow::open_dataset(format = "parquet") |>
  dplyr::select(dplyr::all_of(id_vars), spxmod, kreg) |>
  dplyr::left_join(map_age_gbd_short, by = "age_group_id") |>
  dplyr::left_join(dt_old_age_adj, by = "location_id") |>
  dplyr::filter(age_start >= age_adj_start) |>
  dplyr::mutate(scalar = spxmod / kreg) |>
  dplyr::select(dplyr::all_of(id_vars), scalar)


# Adjust scale mx draws to spxmod -----------------------------------------

dt <-
  fs::path(cfg$dir$onemod, cfg$version$onemod, "draws", ext = "parquet") |>
  arrow::open_dataset(format = "parquet") |>
  dplyr::left_join(dt_onemod_means_adj, by = id_vars) |>
  arrow::to_duckdb() |>
  dplyr::mutate(dplyr::across(draw_0:draw_999, ~ if_else(is.na(scalar), .x, .x * scalar))) |>
  dplyr::select(dplyr::all_of(id_vars), dplyr::starts_with("draw"))

# check for NA's
na_check <-
  dt |>
  dplyr::filter(dplyr::across(dplyr::everything(), is.na)) |>
  dplyr::collect() |>
  nrow()

stopifnot(na_check == 0)


# Save file ---------------------------------------------------------------

file_onemod_draws_update <- paste("draws", "updated", cfg$version$onemod, sep = "-")

dt |>
  arrow::to_arrow() |>
  arrow::write_parquet(fs::path(cfg$dir$onemod_updated, file_onemod_draws_update, ext = "parquet"))
