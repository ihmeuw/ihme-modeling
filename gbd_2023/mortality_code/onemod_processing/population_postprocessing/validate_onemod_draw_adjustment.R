# we have to go year by year I think to melt and summarize the adjusted onemod draws to ensure they
# match with the final mx in the mean file

library(data.table)
library(furrr)

plan("multisession")

# Setup -------------------------------------------------------------------

cfg <- config::get(file = fs::path_wd("population_postprocessing/config.yml"))

map_age_gbd <- demInternal::get_age_map(type = "gbd", gbd_year = cfg$gbd_year)
map_age_gbd_short <- map_age_gbd[, .(age_group_id, age_start)]

map_loc <- demInternal::get_locations(gbd_year = cfg$gbd_year)

id_vars <- c("location_id", "year_id", "age_group_id", "sex_id")

filename_onemod_draws_adj <- paste("draws", "updated", cfg$version$onemod, sep = "-")
filename_onemod_means_adj <- paste("predictions", "updated", cfg$version$onemod, sep = "-")

filename_draws_summary <- paste("draws", "updated", "summary", cfg$version$onemod, sep = "-")


# Load Data ---------------------------------------------------------------

dt_draws <-
  fs::path(cfg$dir$onemod_updated, filename_onemod_draws_adj, ext = "parquet") |>
  arrow::open_dataset(format = "parquet")

dt_means <-
  fs::path(cfg$dir$onemod_updated, filename_onemod_means_adj, ext = "parquet") |>
  arrow::open_dataset(format = "parquet")

dt_msca_draws <-
  fs::path(cfg$dir$onemod, cfg$version$onemod, "predictions", ext = "parquet") |>
  arrow::open_dataset(format = "parquet")

dt_pop <-
  demInternal::get_dem_outputs(
    process_name = "population estimate",
    run_id = cfg$version$pop_adj
  ) |>
  _[, .SD, .SDcols = c(id_vars, "mean")] |>
  setnames("mean", "population")


# Melt and summarize draws ------------------------------------------------

get_logit_mean <- function(yr) {

  dt_draws |>
    dplyr::filter(year_id == yr) |>
    dplyr::filter(dplyr::all_of(id_vars), dplyr::starts_with("draw")) |>
    dplyr::collect() |>
    setDT() |>
    melt(id.vars = id_vars) |>
    _[, .(
      mx_logit_mean = 4 * boot::inv.logit(mean(boot::logit(value / 4))),
      arith_mean = mean(value)
    ), by = id_vars]

}

fp_draws_summary <- fs::path(cfg$dir$onemod_updated, filename_draws_summary, ext = "parquet")

if (!fs::file_exists(fp_draws_summary)) {

  dt_draws_summary <-
    purrr::map(
      cfg$years,
      \(yr) get_logit_mean(yr = yr)
    ) |>
    rbindlist()

  dt_draws_summary |>
    arrow::write_parquet(fp_draws_summary)

} else {

  dt_draws_summary <-
    fp_draws_summary |>
    arrow::open_dataset(format = "parquet")
}

dt_compare <-
  dt_means |>
  dplyr::left_join(dt_draws_summary, by = id_vars) |>
  dplyr::select(dplyr::all_of(id_vars), spxmod, kreg, mx_pop_adj = mx_final, mx_logit_mean, mx_arith_mean) |>
  dplyr::left_join(dt_pop, by = id_vars) |>
  dplyr::mutate(
    deaths_pop_adj = population * mx_pop_adj,
    deaths_logit_mean = population * mx_logit_mean,
    deaths_arith_mean = population * mx_arith_mean
  ) |>
  dplyr::collect() |>
  setDT()


filename_compare <- glue::glue("compare_mx_deaths_{cfg$version$onemod}")

dt_compare |>
  arrow::write_parquet(fs::path(cfg$dir$onemod_updated, filename_compare, ext = "parquet"))
