source("4b_kern_impair/funs.R")

params_global <- readr::read_rds("params_global.rds")
dir_mnt <- purrr::chuck(params_global, "dir_mnt")
draws <- read_prev_draws(
  dir_mnt = dir_mnt,
  nms = c("rh_disease", "g6pd", "preterm", "other"),
  type = "ehb",
  by =  c(
    "age_group_id",
    "sex_id",
    "location_id",
    "year_id",
    "measure_id",
    "metric_id"
  )
) |>
  sum_draws() |>
  add_age_groups()

ehb_draws_dir <- fs::path(dir_mnt, "4b_Kernicterus_Impairment", "ehb_draws")
fs::dir_create(ehb_draws_dir)

purrr::walk(
  purrr::chuck(params_global, "location_id"),
  function(loc_id) {
    dplyr::filter(draws, .data[["location_id"]] == loc_id) |>
      data.table::fwrite(fs::path(
        ehb_draws_dir,
        paste0(loc_id, ".csv")
      ))
  }
)
